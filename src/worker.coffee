_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-rate-limit-logger:worker')

MINUTE=60 * 1000
DELAY=10 * 1000

class Worker
  constructor: ({ @client, @elasticSearch, env })->
    { @RATE_LIMIT_KEY_PREFIX } = env
    unless @client?
      throw new Error('Worker: requires client')
    unless @elasticSearch?
      throw new Error('Worker: requires elasticSearch')
    unless @RATE_LIMIT_KEY_PREFIX?
      throw new Error('Worker: requires RATE_LIMIT_KEY_PREFIX')
    if _.endsWith(@RATE_LIMIT_KEY_PREFIX, '-')
      throw new Error('Worker: requires RATE_LIMIT_KEY_PREFIX to not end with "-"')

  _doAndDelay: (callback) =>
    @do (error) =>
      return callback error if error?
      debug "chillin' for #{DELAY / 1000} seconds"
      _.delay callback, DELAY

  do: (callback) =>
    minute = @getLastMinute()
    debug 'minute same?', @lastMinute == minute, { @lastMinute, minute }
    return callback null if @lastMinute == minute
    @lastMinute = minute
    minuteKey = @getLastMinuteKey minute
    debug { minute, minuteKey }
    @client.hgetall minuteKey, (error, result) =>
      return callback error if error?
      debug 'got result', _.size result
      chunks = _.chunk _.toPairs(result), 100
      bulkUpdate = async.apply @bulkUpdate, { minute }
      async.eachSeries chunks, bulkUpdate, (error) =>
        return callback error if error?
        debug 'done with minute', minute
        callback null
    return # avoid returning promise

  run: (callback) =>
    async.doUntil @_doAndDelay, @_shouldStop, (error) =>
      @stopped = true
      callback error

  bulkUpdate: ({ minute, minuteKey }, result, callback) =>
    body = @_getBodyFromResult { result, minute, minuteKey }
    @elasticSearch.bulk { body }, (error, result) =>
      debug 'bulk update', { error }
      return callback error if error?
      # debug 'bulk update result', JSON.stringify result, null, 2
      callback null

  _getBodyFromResult: ({ result, minute }) =>
    items = []
    index = 'stats:meshblu-rate-limits'
    type  = 'rate-limit:by-uuid'
    _.each result, ([ uuid, count ]) =>
      count = _.toNumber count
      return debug 'count is not a number' if _.isNaN count
      date = minute * MINUTE
      id = "#{minute}-#{uuid}"
      items.push {
        create: { _index: index, _type: type, _id: id }
      }
      items.push { index, type, date, minute, count, uuid }
      return
    return items

  getLastMinute: =>
    currentMinute = Math.floor(Date.now() / MINUTE)
    return currentMinute - 2

  getLastMinuteKey: (minute) =>
    return "#{@RATE_LIMIT_KEY_PREFIX}-#{minute}"

  _shouldStop: =>
    return @stopping == true

  stop: (callback) =>
    @stopping = true
    callback null

module.exports = Worker

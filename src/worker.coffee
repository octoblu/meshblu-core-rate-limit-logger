_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-rate-limit-logger:worker')

ONE_MIN_IN_MS=60 * 1000

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

  _doWithNextTick: (callback) =>
    # give some time for garbage collection
    process.nextTick =>
      @do (error) =>
        process.nextTick =>
          callback error

  do: (callback) =>
    minute = @getLastMinute()
    minuteKey = @getLastMinuteKey(minute)
    debug { minute, minuteKey }
    @client.hgetall minuteKey, (error, result) =>
      return callback error if error?
      debug 'got result', _.size result
      chunks = _.chunk _.toPairs(result), 100
      bulkUpdate = async.apply @bulkUpdate, { minute }
      async.eachSeries chunks, bulkUpdate, callback
    return # avoid returning promise

  run: (callback) =>
    async.doUntil @_doWithNextTick, @_shouldStop, (error) =>
      @stopped = true
      callback error

  bulkUpdate: ({ minute, minuteKey }, result, callback) =>
    body = @_getBodyFromResult { result, minute, minuteKey }
    @elasticSearch.bulk { body }, (error) => callback error

  _getBodyFromResult: ({ result, minute }) =>
    items = []
    index = 'meshblu:stats'
    type  = 'rate-limit:by-uuid'
    _.each result, ([ uuid, count ]) =>
      count = _.toNumber count
      return debug 'count is not a number' if _.isNaN count
      date = minute * ONE_MIN_IN_MS
      id = "#{minute}-#{uuid}"
      items.push create: { _index: index, _type: type, _id: id }
      items.push { index, type, date, minute, count, uuid }
      return
    return items

  getLastMinute: =>
    currentMinute = Math.floor(Date.now() / ONE_MIN_IN_MS)
    return currentMinute - 1

  getLastMinuteKey: (minute) =>
    return "#{@RATE_LIMIT_KEY_PREFIX}-#{minute}"

  _shouldStop: =>
    return @stopping == true

  stop: (callback) =>
    @stopping = true
    callback null

module.exports = Worker

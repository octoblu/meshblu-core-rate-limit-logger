_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-rate-limit-logger:worker')

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
      throw new Error('Worker: RATE_LIMIT_KEY_PREFIX should not end with a "-"')

  _doWithNextTick: (callback) =>
    # give some time for garbage collection
    process.nextTick =>
      @do (error) =>
        process.nextTick =>
          callback error

  do: (callback) =>
    lastMinuteKey = @getLastMinute()
    debug { lastMinuteKey }
    @client.hgetall lastMinuteKey, (error, result) =>
      return callback error if error?
      debug 'got result', result
      @elasticSearch.bulk result, (error) =>
        return callback error if error?
        callback null, result
    return # avoid returning promise

  run: (callback) =>
    async.doUntil @_doWithNextTick, @_shouldStop, (error) =>
      @stopped = true
      callback error

  getLastMinute: =>
    currentMinute = Math.floor(Date.now() / (1000*60))
    lastMinute    = currentMinute - 1
    return "#{@RATE_LIMIT_KEY_PREFIX}-#{lastMinute}"

  _shouldStop: =>
    return @stopping == true

  stop: (callback) =>
    @stopping = true
    callback null

module.exports = Worker

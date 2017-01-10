async = require 'async'

class Worker
  constructor: ({ @client, env })->
    { @QUEUE_NAME, @QUEUE_TIMEOUT } = env
    throw new Error('Worker: requires client') unless @client?
    throw new Error('Worker: requires QUEUE_NAME') unless @QUEUE_NAME?
    throw new Error('Worker: requires QUEUE_TIMEOUT') unless @QUEUE_TIMEOUT?

  _doWithNextTick: (callback) =>
    # give some time for garbage collection
    process.nextTick =>
      @do (error) =>
        process.nextTick =>
          callback error

  do: (callback) =>
    @client.brpop @QUEUE_NAME, @QUEUE_TIMEOUT, (error, result) =>
      return callback error if error?
      return callback() unless result?

      [ queue, data ] = result
      try
        data = JSON.parse data
      catch error
        return callback error

      callback null, data
    return # avoid returning promise

  run: (callback) =>
    async.doUntil @_doWithNextTick, @_shouldStop, (error) =>
      @stopped = true
      callback error

  _shouldStop: =>
    return @stopping == true

  stop: (callback) =>
    @stopping = true
    callback null

module.exports = Worker

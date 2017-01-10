_              = require 'lodash'
envalid        = require 'envalid'
Redis          = require 'ioredis'
RedisNS        = require '@octoblu/redis-ns'
SigtermHandler = require 'sigterm-handler'

Worker         = require './src/worker'
packageJSON    = require './package.json'

OPTIONS = {
  REDIS_URI: envalid.str({ devDefault: 'redis://localhost:6379' })
  REDIS_NAMESPACE: envalid.str()
  QUEUE_NAME: envalid.str()
  QUEUE_TIMEOUT: envalid.num({ default: 30 })
}

class Command
  constructor: ->
    process.on 'uncaughtException', @die
    @env = envalid.cleanEnv process.env, OPTIONS

  run: =>
    @getWorkerClient (error, client) =>
      return @die error if error?

      worker = new Worker { client, @env }
      worker.run @die

      sigtermHandler = new SigtermHandler { events: ['SIGINT', 'SIGTERM'] }
      sigtermHandler.register worker.stop

  getWorkerClient: (callback) =>
    @getRedisClient @env.REDIS_URI, (error, client) =>
      return callback error if error?
      clientNS  = new RedisNS @env.REDIS_NAMESPACE, client
      callback null, clientNS

  getRedisClient: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, dropBufferSupport: true
    client = _.bindAll client, _.functionsIn(client)
    client.ping (error) =>
      return callback error if error?
      client.once 'error', @die
      callback null, client

  die: (error) =>
    return process.exit(0) unless error?
    console.error 'ERROR'
    console.error error.stack
    process.exit 1

module.exports = Command

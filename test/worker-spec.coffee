_       = require 'lodash'
Redis   = require 'ioredis'
RedisNS = require '@octoblu/redis-ns'
Worker  = require '../src/worker'

describe 'Worker', ->
  beforeEach (done) ->
    client = new Redis 'localhost', dropBufferSupport: true
    client = _.bindAll client, _.functionsIn(client)
    client.ping (error) =>
      return done error if error?
      client.once 'error', done
      @client = new RedisNS 'test-worker', client
      done null
    return # redis fix

  beforeEach ->
    env = {
      QUEUE_NAME: 'work'
      QUEUE_TIMEOUT: 1
    }
    @sut = new Worker { @client, env }

  afterEach (done) ->
    @sut.stop done

  describe '->do', ->
    beforeEach (done) ->
      data = JSON.stringify foo: 'bar'
      @client.lpush 'work', data, done
      return # stupid promises

    beforeEach (done) ->
      @sut.do (error, @data) =>
        done error

    it 'should call the callback with data', ->
      expect(@data).to.deep.equal foo: 'bar'

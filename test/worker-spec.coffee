_             = require 'lodash'
Redis         = require 'ioredis'
debug         = require('debug')('spec:meshblu-core-rate-limit-logger:worker')
RedisNS       = require '@octoblu/redis-ns'
Worker        = require '../src/worker'

describe 'Worker', ->
  beforeEach (done) ->
    client = new Redis 'localhost', dropBufferSupport: true
    client = _.bindAll client, _.functionsIn(client)
    client.ping (error) =>
      return done error if error?
      client.once 'error', done
      client.flushdb (error) =>
        return done error if error?
        debug 'redis is ready'
        @client = new RedisNS 'test-worker', client
        done null
    return # redis fix

  beforeEach ->
    env = {
      ELASTICSEARCH_URI: "http://localhost:#{0xd00d}"
      RATE_LIMIT_KEY_PREFIX: 'some-rate-limit-key'
    }
    @elasticSearch = {
      bulk: sinon.stub().yields null
    }
    @sut = new Worker { @client, env, @elasticSearch }

  afterEach (done) ->
    @sut.stop done

  describe '->do', ->
    beforeEach (done) ->
      lastMinuteKey = @sut.getLastMinute()
      debug { lastMinuteKey }
      @client.hset lastMinuteKey, 'some-test-uuid', 64, done
      return # stupid promises

    beforeEach (done) ->
      lastMinuteKey = @sut.getLastMinute()
      debug { lastMinuteKey }
      @client.hset lastMinuteKey, 'some-other-uuid', 52, done
      return # stupid promises

    beforeEach (done) ->
      @sut.do (error) =>
        debug 'done'
        done error

    it 'should bulk update in elastic search', ->
      expect(@elasticSearch.bulk).to.have.been.called

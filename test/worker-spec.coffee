_       = require 'lodash'
UUID    = require 'uuid'
async   = require 'async'
Redis   = require 'ioredis'
debug   = require('debug')('spec:meshblu-core-rate-limit-logger:worker')
RedisNS = require '@octoblu/redis-ns'
Worker  = require '../src/worker'

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
    env =
      ELASTICSEARCH_URI: "http://localhost:#{0xd00d}"
      RATE_LIMIT_KEY_PREFIX: 'some-rate-limit-key'
    @elasticSearch =
      bulk: sinon.stub().yields()
    @sut = new Worker { @client, env, @elasticSearch }

  afterEach (done) ->
    @sut.stop done

  describe '->do', ->
    describe 'when no records exist', ->
      beforeEach (done) ->
        @sut.do (error) =>
          done error

      it 'should bulk update in elastic search', ->
        expect(@elasticSearch.bulk).to.not.have.been.called

    describe 'when two records exist', ->
      beforeEach ->
        @minute = @sut.getLastMinute()
        @minuteKey = @sut.getLastMinuteKey @minute
        debug { @minuteKey, @minute }

      beforeEach (done) ->
        @client.hset @minuteKey, 'some-test-uuid', 64, done
        return # stupid promises

      beforeEach (done) ->
        @client.hset @minuteKey, 'some-other-uuid', 52, done
        return # stupid promises

      beforeEach (done) ->
        @sut.do (error) =>
          done error

      it 'should bulk update in elastic search', ->
        expect(@elasticSearch.bulk).to.have.been.calledWith body: [
          {
            index:
              _index: 'stats:meshblu-rate-limits',
              _type: 'rate-limit:by-uuid',
              _id: "#{@minute}-some-test-uuid"
          }
          {
            index: 'stats:meshblu-rate-limits'
            type: 'rate-limit:by-uuid'
            date: @minute * 60 * 1000
            uuid: 'some-test-uuid'
            minute: @minute
            count: 64
          }
          {
            index:
              _index: 'stats:meshblu-rate-limits',
              _type: 'rate-limit:by-uuid',
              _id: "#{@minute}-some-other-uuid"
          }
          {
            index: 'stats:meshblu-rate-limits'
            type: 'rate-limit:by-uuid'
            date: @minute * 60 * 1000
            uuid: 'some-other-uuid'
            minute: @minute
            count: 52
          }
        ]

    describe 'when more than a 200 records exist', ->
      beforeEach ->
        @minute = @sut.getLastMinute()
        @minuteKey = @sut.getLastMinuteKey @minute
        debug { @minuteKey, @minute }

      beforeEach (done) ->
        keys = []
        _.times 200, =>
          keys.push UUID.v1()
          keys.push _.random(1, 1000)
        @client.hmset @minuteKey, keys..., done
        return # redis fix

      beforeEach (done) ->
        @sut.do (error) =>
          done error

      it 'should bulk update in elastic search twice', ->
        expect(@elasticSearch.bulk).to.have.been.called.twice

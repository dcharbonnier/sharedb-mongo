const expect = require('expect.js');
const mongodb = require('mongodb');
const ShareDbMongo = require('../index');
const getQuery = require('@teamwork/sharedb-mingo-memory/get-query');

const mongoUrl = process.env.TEST_MONGO_URL || 'mongodb://localhost:27017/test';

function create(callback) {
  const db = new ShareDbMongo({mongo: function(shareDbCallback) {
    mongodb.connect(mongoUrl, { useNewUrlParser: true }, (err, mongo) => {
      if (err) return callback(err);
      mongo.db().dropDatabase(err => {
        if (err) return callback(err);
        shareDbCallback(null, mongo);
        callback(null, db, mongo);
      });
    });
  }});
};

require('@teamwork/sharedb/test/db')({create: create, getQuery: getQuery});

describe('mongo db', () => {
  beforeEach(function(done) {
    const self = this;
    create((err, db, mongo) => {
      if (err) return done(err);
      self.db = db;
      self.mongo = mongo;
      done();
    });
  });

  afterEach(function(done) {
    this.db.close(done);
  });

  describe('indexes', () => {
    it('adds ops index', function(done) {
      const mongo = this.mongo;
      this.db.commit('testcollection', 'foo', {v: 0, create: {}}, {}, null, err => {
        if (err) return done(err);
        mongo.db().collection('o_testcollection').indexInformation((err, indexes) => {
          if (err) return done(err);
          // Index for getting document(s) ops
          expect(indexes['d_1_v_1']).ok();
          // Index for checking committed op(s) by src and seq
          expect(indexes['src_1_seq_1_v_1']).ok();
          done()
        });
      });
    });

    it('respects unique indexes', function(done) {
      const db = this.db;
      this.mongo.db().collection('testcollection').createIndex({x: 1}, {unique: true}, err => {
        if (err) return done(err);
        db.commit('testcollection', 'foo', {v: 0, create: {}}, {v: 1, data: {x: 7}}, null, (err, succeeded) => {
          if (err) return done(err);
          db.commit('testcollection', 'bar', {v: 0, create: {}}, {v: 1, data: {x: 7}}, null, (err, succeeded) => {
            expect(err && err.code).equal(11000);
            done();
          });
        });
      });
    });
  });

  describe('security options', () => {
    it('does not allow editing the system collection', function(done) {
      const db = this.db;
      db.commit('system', 'test', {v: 0, create: {}}, {}, null, err => {
        expect(err).ok();
        db.getSnapshot('system', 'test', null, null, err => {
          expect(err).ok();
          done();
        });
      });
    });
  });

  describe('query', () => {
    // Run query tests for the types of queries supported by ShareDBMingo
    require('@teamwork/sharedb-mingo-memory/test/query')();

    it('does not allow $where queries', function(done) {
      this.db.query('testcollection', {$where: 'true'}, null, null, (err, results) => {
        expect(err).ok();
        done();
      });
    });

    it('queryPollDoc does not allow $where queries', function(done) {
      this.db.queryPollDoc('testcollection', 'somedoc', {$where: 'true'}, null, err => {
        expect(err).ok();
        done();
      });
    });

    it('$query is deprecated', function(done) {
      this.db.query('testcollection', {$query: {}}, null, null, err => {
        expect(err).ok();
        expect(err.code).eql(4106);
        done();
      });
    });

    it('only one collection operation allowed', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $aggregate: {}}, null, null, err => {
        expect(err).ok();
        expect(err.code).eql(4108);
        done();
      });
    });

    it('only one cursor operation allowed', function(done) {
      this.db.query('testcollection', {$count: true, $explain: true}, null, null, err => {
        expect(err).ok();
        expect(err.code).eql(4109);
        done();
      });
    });

    it('cursor transform can\'t run after collection operation', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $sort: {y: 1}}, null, null, err => {
        expect(err).ok();
        expect(err.code).eql(4110);
        done();
      });
    });

    it('cursor operation can\'t run after collection operation', function(done) {
      this.db.query('testcollection', {$distinct: {y: 1}, $count: true}, null, null, err => {
        expect(err).ok();
        expect(err.code).eql(4110);
        done();
      });
    });

    it('non-object $readPref should return error', function(done) {
      this.db.query('testcollection', {$readPref: true}, null, null, err => {
        expect(err).ok();
        expect(err.code).eql(4107);
        done();
      });
    });

    it('malformed $mapReduce should return error', function(done) {
      this.db.allowJSQueries = true; // required for $mapReduce
      this.db.query('testcollection', {$mapReduce: true}, null, null, err => {
        expect(err).ok();
        expect(err.code).eql(4107);
        done();
      });
    });

    describe('queryPollDoc correctly filters on _id', done => {
      const snapshot = {type: 'json0', v: 1, data: {}, id: "test"};

      beforeEach(function(done) {
        this.db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, done);
      });

      it('filter on id string that matches doc', function(done) {
        test.bind(this)({_id: 'test'}, true, done);
      });
      it('filter on id string that doesn\'t match doc', function(done) {
        test.bind(this)({_id: 'nottest'}, false, done);
      });
      it('filter on id regexp that matches doc', function(done) {
        test.bind(this)({_id: /test/}, true, done);
      });
      it('filter on id regexp that doesn\'t match doc', function(done) {
        test.bind(this)({_id: /nottest/}, false, done);
      });
      it('filter on id $in that matches doc', function(done) {
        test.bind(this)({_id: {$in: ['test']}}, true, done);
      });
      it('filter on id $in that doesn\'t match doc', function(done) {
        test.bind(this)({_id: {$in: ['nottest']}}, false, done);
      });

      // Intentionally inline calls to 'it' rather than place them
      // inside 'test' so that you can use Mocha's 'skip' or 'only'
      function test(query, expectedHasDoc, done) {
        this.db.queryPollDoc(
          'testcollection',
          snapshot.id,
          query,
          null,
          (err, hasDoc) => {
            if (err) done(err);
            expect(hasDoc).eql(expectedHasDoc);
            done();
          }
        );
      };
    });

    it('$distinct should perform distinct operation', function(done) {
      const snapshots = [
        {type: 'json0', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', v: 1, data: {x: 3, y: 2}}
      ];
      const db = this.db;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, err => {
        if (err) return done(err);
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, err => {
          if (err) return done(err);
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, err => {
            if (err) return done(err);
            const query = {$distinct: {field: 'y'}};
            db.query('testcollection', query, null, null, (err, results, extra) => {
              if (err) return done(err);
              expect(extra).eql([1, 2]);
              done();
            });
          });
        });
      });
    });

    it('$aggregate should perform aggregate command', function(done) {
      const snapshots = [
        {type: 'json0', v: 1, data: {x: 1, y: 1}},
        {type: 'json0', v: 1, data: {x: 2, y: 2}},
        {type: 'json0', v: 1, data: {x: 3, y: 2}}
      ];
      const db = this.db;
      db.allowAggregateQueries = true;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, err => {
        if (err) return done(err);
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, err => {
          if (err) return done(err);
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, err => {
            if (err) return done(err);
            const query = {$aggregate: [
              {$group: {_id: '$y', count: {$sum: 1}}},
              {$sort: {count: 1}}
            ]};
            db.query('testcollection', query, null, null, (err, results, extra) => {
              if (err) return done(err);
              expect(extra).eql([{_id: 1, count: 1}, {_id: 2, count: 2}]);
              done();
            });
          });
        });
      });
    });

    it('does not let you run $aggregate queries without options.allowAggregateQueries', function(done) {
      const query = {$aggregate: [
        {$group: {_id: '$y',count: {$sum: 1}}},
        {$sort: {count: 1}}
      ]};
      this.db.query('testcollection', query, null, null, (err, results) => {
        expect(err).ok();
        done();
      });
    });

    it('does not allow $mapReduce queries by default', function(done) {
      const snapshots = [
        {type: 'json0', v: 1, data: {player: 'a', round: 1, score: 5}},
        {type: 'json0', v: 1, data: {player: 'a', round: 2, score: 7}},
        {type: 'json0', v: 1, data: {player: 'b', round: 1, score: 15}}
      ];
      const db = this.db;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, err => {
        if (err) return done(err);
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, err => {
          if (err) return done(err);
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, err => {
            if (err) return done(err);
            const query = {
              $mapReduce: {
                map: function() {
                  emit(this.player, this.score);
                },
                reduce: function(key, values) {
                  return values.reduce((t, s) => t + s);
                }
              }
            };
            db.query('testcollection', query, null, null, err => {
              expect(err).ok();
              done();
            });
          });
        });
      });
    });

    it('$mapReduce queries should work when allowJavaScriptQuery == true', function(done) {
      const snapshots = [
        {type: 'json0', v: 1, data: {player: 'a', round: 1, score: 5}},
        {type: 'json0', v: 1, data: {player: 'a', round: 2, score: 7}},
        {type: 'json0', v: 1, data: {player: 'b', round: 1, score: 15}}
      ];
      const db = this.db;
      db.allowJSQueries = true;
      db.commit('testcollection', 'test1', {v: 0, create: {}}, snapshots[0], null, err => {
        if (err) return done(err);
        db.commit('testcollection', 'test2', {v: 0, create: {}}, snapshots[1], null, err => {
          if (err) return done(err);
          db.commit('testcollection', 'test3', {v: 0, create: {}}, snapshots[2], null, err => {
            if (err) return done(err);
            const query = {
              $mapReduce: {
                map: function() {
                  emit(this.player, this.score);
                },
                reduce: function(key, values) {
                  return values.reduce((t, s) => t + s);
                }
              }
            };
            db.query('testcollection', query, null, null, (err, results, extra) => {
              if (err) return done(err);
              expect(extra).eql([{_id: 'a', value: 12}, {_id: 'b', value: 15}]);
              done();
            });
          });
        });
      });
    });
  });
});

describe('mongo db connection', () => {
  describe('via url string', () => {
    beforeEach(function(done) {
      this.db = new ShareDbMongo({mongo: mongoUrl});

      // This will enqueue the callback, testing the 'pendingConnect'
      // logic.
      this.db.getDbs((err, mongo, mongoPoll) => {
        if (err) return done(err);
        mongo.db().dropDatabase(err => {
          if (err) return done(err);
          done();
        });
      });
    });

    afterEach(function(done) {
      this.db.close(done);
    });

    it('commit and query', function(done) {
      const snapshot = {type: 'json0', v: 1, data: {}, id: "test"};
      const db = this.db;

      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, err => {
        if (err) return done(err);
        db.query('testcollection', {}, null, null, (err, results) => {
          if (err) return done(err);
          expect(results).eql([snapshot]);
          done();
        });
      });
    });
  });

  describe('via url string with mongoPoll and pollDelay option', () => {
    beforeEach(function(done) {
      this.pollDelay = 1000;
      this.db = new ShareDbMongo({mongo: mongoUrl, mongoPoll: mongoUrl, pollDelay: this.pollDelay});
      done();
    });

    afterEach(function(done) {
      this.db.close(done);
    });

    it('delays queryPoll but not commit', function(done) {
      const db = this.db;
      const pollDelay = this.pollDelay;

      const snapshot = {type: 'json0', v: 1, data: {}, id: "test"};
      const timeBeforeCommit = new Date;
      db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, err => {
        if (err) return done(err);
        expect((new Date) - timeBeforeCommit).lessThan(pollDelay);

        const timeBeforeQuery = new Date;
        db.queryPoll('testcollection', {}, null, (err, results) => {
          if (err) return done(err);
          expect(results.length).eql(1);
          expect((new Date) - timeBeforeQuery).greaterThan(pollDelay);
          done();
        });
      });
    });
  });
});

describe('parse query', () => {
  const parseQuery = ShareDbMongo._parseQuery;
  const makeQuerySafe = ShareDbMongo._makeQuerySafe;

  const addsType = query => {
    const queryWithTypeNeNull = shallowClone(query);
    queryWithTypeNeNull._type = {$type: 2};
    const parsedQuery = parseQuery(query);
    makeQuerySafe(parsedQuery.query);
    expect(parsedQuery.query).eql(queryWithTypeNeNull);
  };

  const doesNotModify = query => {
    const parsedQuery = parseQuery(query);
    makeQuerySafe(parsedQuery.query);
    expect(parsedQuery.query).eql(query);
  };

  describe('adds _type: {$type: 2} when necessary', () => {
    it('basic', () => {
      addsType({});
      addsType({foo: null});
      doesNotModify({foo: 1});
      addsType({foo: {$bitsAllSet: 1}}); // We don't try to analyze $bitsAllSet
    });

    it('does not modify already set type', () => {
      doesNotModify({_type: null});
      doesNotModify({_type: 'foo'});
      doesNotModify({_type: {$ne: null}});
    });

    it('ignores fields that remain set on deleted docs', () => {
      addsType({_id: 'x'});
      addsType({_o: 'x'});
      addsType({_v: 2});
      addsType({_m: {mtime: 2}});
      addsType({'_m.mtime': 2});

      addsType({_id: 'x', foo: null});
      addsType({_o: 'x', foo: null});
      addsType({_v: 2, foo: null});
      addsType({_m: {mtime: 2}, foo: null});
      addsType({'_m.mtime': 2, foo: null});

      doesNotModify({_id: 'x', foo: 1});
      doesNotModify({_o: 'x', foo: 1});
      doesNotModify({_v: 2, foo: 1});
      doesNotModify({_m: {mtime: 2}, foo: 1});
      doesNotModify({'_m.mtime': 2, foo: 1});
    });

    it('$ne', () => {
      addsType({foo: {$ne: 1}});
      doesNotModify({foo: {$ne: 1}, bar: 1});
      doesNotModify({foo: {$ne: null}});
    });

    it('comparisons', () => {
      doesNotModify({foo: {$gt: 1}});
      doesNotModify({foo: {$gte: 1}});
      doesNotModify({foo: {$lt: 1}});
      doesNotModify({foo: {$lte: 1}});
      doesNotModify({foo: {$gte: 2, $lte: 5}});
      addsType({foo: {$gte: null, $lte: null}});
    });

    it('$exists', () => {
      doesNotModify({foo: {$exists: true}});
      addsType({foo: {$exists: false}});
      doesNotModify({foo: {$exists: true}, bar: {$exists: false}});
    });

    it('$not', () => {
      addsType({$not: {foo: 1}});
      addsType({$not: {foo: null}}); // We don't try to analyze $not
    });

    it('$in', () => {
      doesNotModify({foo: {$in: [1, 2, 3]}});
      addsType({foo: {$in: [null, 2, 3]}});
      doesNotModify({foo: {$in: [null, 2, 3]}, bar: 1});
    })

    it('top-level $and', () => {
      doesNotModify({$and: [{foo: {$ne: null}}, {bar: {$ne: null}}]});
      doesNotModify({$and: [{foo: {$ne: 1}}, {bar: {$ne: null}}]});
      addsType({$and: [{foo: {$ne: 1}}, {bar: {$ne: 1}}]});
    });

    it('top-level $or', () => {
      doesNotModify({$or: [{foo: {$ne: null}}, {bar: {$ne: null}}]});
      addsType({$or: [{foo: {$ne: 1}}, {bar: {$ne: null}}]});
      addsType({$or: [{foo: {$ne: 1}}, {bar: {$ne: 1}}]});
    });

    it('malformed queries', () => {
      // if we don't understand the query, definitely don't mark it as
      // "safe as is"
      addsType({$or: {foo: 3}});
      addsType({foo: {$or: 3}});
      addsType({$not: [1, 2]});
      addsType({foo: {$bad: 1}});
      addsType({$bad: [2, 3]});
      addsType({$and: [[{foo: 1}]]});
    });
  });
});

function shallowClone(object) {
  const out = {};
  for (const key in object) {
    out[key] = object[key];
  }
  return out;
}

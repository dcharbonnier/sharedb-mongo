const expect = require('expect.js');
const ShareDbMongo = require('../index');
const checkOp = require('@teamwork/sharedb/lib/ot').checkOp;

describe('skipPoll', () => {
  // Run a test function against a small sample set of queries
  function testSampleQueries(test) {
    function testInNewCase(query) {
      it(JSON.stringify(query), () => {
        test(query);
      })
    };

    testInNewCase({a: 1});
    testInNewCase({a: {$in: [1, 2]}});
    testInNewCase({a: {b: 1}});
    testInNewCase({'a.b': 'foo'});
    testInNewCase({$not: {a: 1}});
    testInNewCase({$or: [{a: 1}, {b: 1}]});
    testInNewCase({a: 1, $sort: {b: 1}});
    testInNewCase({a: 1, $limit: 5});
    testInNewCase({a: 1, $count: true});
    testInNewCase({$distinct: {field: 'a'}});
  }

  describe('noops always skip', () => {
    testSampleQueries(query => {
      assertSkips({v: 0}, query);
    });
  });

  describe('creates never skip', () => {
    testSampleQueries(query => {
      assertNotSkips({v: 0, create: {type: 'json0', _id: 'dummyid'}}, query);
      assertNotSkips({v: 0, create: {type: 'json0', _id: 'dummyid', a: 1}}, query);
      assertNotSkips({v: 0, create: {type: 'json0', _id: 'dummyid', a: {b: 'foo'}}}, query);
    });
  });

  describe('deletes never skip', () => {
    testSampleQueries(query => {
      assertNotSkips({v: 0, del: true}, query);
    });
  });

  describe('updates', () => {
    it('never skip for queries returning extra', () => {
      test({a: 1, $count: true});
      test({$distinct: {field: 'a'}});

      function test(query) {
        assertNotSkips({op: []}, query);
        assertNotSkips({op: [{p: ['a'], dummyOp: 1}]}, query);
        assertNotSkips({op: [{p: ['x'], dummyOp: 1}]}, query);
      }
    });

    describe('skip sometimes for queries returning results', () => {
      test({a: 1}, ['a']);
      test({a: {$in: [1, 2]}}, ['a']);
      test({a: {$nin: [1, 2]}}, ['a']);
      test({a: {b: 1}}, ['a']);
      test({a: 1, x: 2}, ['a', 'x']);
      test({x: 2}, ['x']);
      test({y: 3}, ['y']);
      test({a: 1, y: 3}, ['a', 'y']);

      test({'a.b': 42}, ['a']);
      test({'a.0.b': 42}, ['a']);
      test({'a.b': {$exists: true}}, ['a']);
      test({'a.b': 3, c: [{$gt: 4}, {$lt: 5}]}, ['a', 'c']);

      test({$or: [{a: 1}, {'b.c': 2}]}, ['a', 'b']);
      test({$or: [{a: 1}, {$or: [{b: 2}, {c: 3}]}]}, ['a', 'b', 'c']);
      test({$or: [{a: 1}, {$or: [{b: 2}, {'c.d': 3}]}]}, ['a', 'b', 'c']);
      test({$and: [{a: 1}, {'b.c': 2}]}, ['a', 'b']);
      test({$nor: [{a: 1}, {'b.c': 2}]}, ['a', 'b']);
      test({$not: {a: 1}}, ['a']);
      test({$not: {'a.b': 1}}, ['a']);
      test({$not: {a: {b: 1}}}, ['a']);

      test({$or: [{a: 1}, {$or: [{b: 2}, {'c.d': 3}]}], $skip: 5}, ['a', 'b', 'c']);
      test({$or: [{a: 1}, {$or: [{b: 2}, {'c.d': 3}]}], $limit: 5}, ['a', 'b', 'c']);

      test({a: 1, x: 2, $sort: {a: 1}}, ['a', 'x']);
      test({a: 1, x: 2, $sort: {y: 1}}, ['a', 'x', 'y']);
      test({a: 1, x: 2, $sort: {y: 1, z: -1}}, ['a', 'x', 'y', 'z']);

      // 'fields' is an array of top-level fields from which query reads
      function test(query, fields) {
        describe(JSON.stringify(query), () => {
          it('empty path changes', () => {
            assertNotSkips({op: [{p: [], dummyOp: 1}]}, query);
          });

          it('top-level field changes', () => {
            assertIfSkips({op: [{p: ['a'], dummyOp: 1}]}, query, !has(fields, 'a'));
            assertIfSkips({op: [{p: ['a', 1], dummyOp: 1}]}, query, !has(fields, 'a'));
            assertIfSkips({op: [{p: ['x'], dummyOp: 1}]}, query, !has(fields, 'x'));
            assertIfSkips({op: [{p: ['x', 'y'], dummyOp: 1}]}, query, !has(fields, 'x'));
          });

          it('multiple ops', () => {
            assertIfSkips(
              {op: [{p: ['a'], dummyOp: 1}, {p: ['x'], dummyOp: 1}]},
              query,
              !has(fields, 'a') && !has(fields, 'x')
            );
          });

          it('multiple ops including empty path', () => {
            assertNotSkips({op: [{p: ['a'], dummyOp: 1}, {p: [], dummyOp: 1}]}, query);
            assertNotSkips({op: [{p: [], dummyOp: 1}, {p: ['x'], dummyOp: 1}]}, query);
          });
        });
      }
    });
  });
});

// `rawOp` is a partial op document, containing only one 'create', 'del' or 'op'
function assertIfSkips(rawOp, query, expectedSkips) {
  const op = {src: 'dummysrc', seq: 0, v: 0};
  for (const key in rawOp) {
    op[key] = rawOp[key];
  }
  const actualSkips = ShareDbMongo.prototype.skipPoll(
    'dummycollection', 'dummyid', op, query);
  expect(actualSkips).eql(expectedSkips);
}

function assertSkips(rawOp, query) { assertIfSkips(rawOp, query, true); };
function assertNotSkips(rawOp, query) { assertIfSkips(rawOp, query, false); };

function has(haystack, needle) {
  return haystack.indexOf(needle) !== -1;
}

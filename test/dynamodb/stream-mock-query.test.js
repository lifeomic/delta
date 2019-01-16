const {dynamodb} = require('../..');
const rxjs = require('rxjs');
const test = require('ava');

const {BehaviorSubject} = require('rxjs');
const {toArray} = require('rxjs/operators');

async function testStream (test, { expectedResults, items, requests }) {
  const stream = dynamodb.mockItemQueryStream(items, requests);
  const results = await stream.pipe(toArray()).toPromise();
  test.deepEqual(results, expectedResults || items);
}

test('a list of items can be streamed', async (test) => {
  await testStream(
    test,
    {
      items: ['one', 'two', 'three'],
      requests: new BehaviorSubject(Infinity)
    }
  );
});

test('a subset of items can be streamed', async (test) => {
  const items = ['one', 'two', 'three'];

  await testStream(
    test,
    {
      expectedResults: items.slice(0, 2),
      items,
      requests: rxjs.of(2)
    }
  );
});

test('items can be streamed in chunks', async (test) => {
  const items = ['one', 'two', 'three'];

  await testStream(
    test,
    {
      expectedResults: items.slice(0, 2),
      items,
      requests: rxjs.of(1, 1)
    }
  );
});

test('the stream terminates when the item set is exhausted', async (test) => {
  await testStream(
    test,
    {
      items: ['one', 'two', 'three'],
      requests: rxjs.of(2, 3, 4)
    }
  );
});

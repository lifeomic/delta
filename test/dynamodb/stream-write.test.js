const set = require('lodash/set');
const sinon = require('sinon');
const test = require('ava');
const util = require('util');
const uuid = require('uuid/v4');

const { dynamodb } = require('../..');
const { requestToDeleteItem, requestToUpdateItem, mockItemQueryStream } = require('./helpers/streams');
const { tap, toArray } = require('rxjs/operators');

const assertItemDeleted = (stub, table, item) => sinon.assert.calledWithExactly(
  stub,
  sinon.match(requestToDeleteItem(table, item.id)),
  sinon.match.func
);

const assertItemUpdated = (stub, table, item) => sinon.assert.calledWithExactly(
  stub,
  sinon.match(requestToUpdateItem(table, item)),
  sinon.match.func
);

const mockMethod = (documentClient, name) => {
  const stub = sinon.stub().yields(null, {});
  const promisified = util.promisify(stub);

  const method = (...args) => ({
    promise: () => promisified(...args)
  });

  set(documentClient, name, method);
  return stub;
};

const testWritingItems = async (test, {operator, validator}) => {
  const { batchWrite, dynamoClient } = test.context;
  const evenTable = uuid();
  const oddTable = uuid();

  const items = new Array(42).fill(null)
    .map((item, index) => ({
      item: { id: uuid() },
      table: index % 2 ? oddTable : evenTable
    }));

  const {write, writeBufferSpace} = dynamodb.write(dynamoClient);

  const expected = [];
  const performed = await mockItemQueryStream(items, writeBufferSpace)
    .pipe(operator)
    .pipe(tap((value) => expected.push(value)))
    .pipe(write)
    .pipe(toArray())
    .toPromise();

  test.deepEqual(performed, expected);

  sinon.assert.calledTwice(batchWrite);
  items.slice(0, 25).forEach((item) => validator(batchWrite.firstCall, item.table, item.item));
  items.slice(25).forEach((item) => validator(batchWrite.secondCall, item.table, item.item));
};

test.beforeEach((test) => {
  const dynamoClient = {};
  test.context.batchWrite = mockMethod(dynamoClient, 'batchWrite');
  test.context.dynamoClient = dynamoClient;
});

test('deleting a stream of items writes the items to the database in batches', async (test) => {
  await testWritingItems(test, {operator: dynamodb.delete(), validator: assertItemDeleted});
});

test('updating a stream of items writes the items to the database in batches', async (test) => {
  await testWritingItems(test, {operator: dynamodb.put(), validator: assertItemUpdated});
});

test('when a request fails the stream emits an error', async (test) => {
  const { batchWrite, dynamoClient } = test.context;
  const table = uuid();

  const items = new Array(42).fill(null)
    .map((item, index) => ({
      item: { id: uuid() },
      table
    }));

  const failure = new Error('simulated failure');
  batchWrite.yields(failure);

  const {write, writeBufferSpace} = dynamodb.write(dynamoClient);

  const error = await test.throws(
    mockItemQueryStream(items, writeBufferSpace)
      .pipe(dynamodb.delete())
      .pipe(write)
      .toPromise()
  );

  test.is(error, failure);
  sinon.assert.calledOnce(batchWrite);
  items.slice(0, 25).forEach((item) => assertItemDeleted(batchWrite.firstCall, item.table, item.item));
});

test('unprocessed items are retried', async (test) => {
  const { batchWrite, dynamoClient } = test.context;
  const table = uuid();

  const items = new Array(25).fill(null)
    .map((item) => ({ item: { id: uuid() }, table }));

  const unprocessed = {
    UnprocessedItems: {
      [table]: items.slice(0, 10).map(({item}) => ({
        DeleteRequest: {
          Key: { id: item.id }
        }
      }))
    }
  };

  batchWrite
    .yields(new Error('unmocked write'))
    .onFirstCall().yields(null, unprocessed)
    .onSecondCall().yields(null, {});

  const {write, writeBufferSpace} = dynamodb.write(dynamoClient);

  await mockItemQueryStream(items, writeBufferSpace)
    .pipe(dynamodb.delete())
    .pipe(write)
    .toPromise();

  sinon.assert.calledTwice(batchWrite);
  items.forEach((item) => assertItemDeleted(batchWrite.firstCall, item.table, item.item));
  items.slice(0, 10).forEach((item) => assertItemDeleted(batchWrite.secondCall, item.table, item.item));
});

test('when a batch fails too many times an error is emitted', async (test) => {
  const { batchWrite, dynamoClient } = test.context;
  const table = uuid();

  const items = new Array(25).fill(null)
    .map((item) => ({ item: { id: uuid() }, table }));

  const unprocessed = {
    UnprocessedItems: {
      [table]: items.slice(0, 10).map(({item}) => ({
        DeleteRequest: {
          Key: { id: item.id }
        }
      }))
    }
  };

  batchWrite.yields(null, unprocessed);

  const {write, writeBufferSpace} = dynamodb.write(dynamoClient);

  await test.throws(
    mockItemQueryStream(items, writeBufferSpace)
      .pipe(dynamodb.delete())
      .pipe(write)
      .toPromise(),
    'Failed to complete batch write after multiple attempts'
  );

  sinon.assert.calledThrice(batchWrite);
});

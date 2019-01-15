const {dynamodb} = require('../..');
const sinon = require('sinon');
const test = require('ava');
const uuid = require('uuid/v4');

const rxjs = require('rxjs');
const { toArray, mergeMap, delay, tap } = require('rxjs/operators');
const { useDynamoDB } = require('@lifeomic/lambda-tools').dynamodb;

useDynamoDB(test, true);

// 'serial' ensures this hook runs after `useDynamoDB` hooks
test.serial.beforeEach(async (test) => {
  const { dynamoClient } = test.context.dynamodb;
  const table = uuid();

  await dynamoClient.createTable({
    AttributeDefinitions: [
      { AttributeName: 'id', AttributeType: 'S' },
      { AttributeName: 'user', AttributeType: 'S' }
    ],
    KeySchema: [
      { AttributeName: 'user', KeyType: 'HASH' },
      { AttributeName: 'id', KeyType: 'RANGE' }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    },
    TableName: table
  }).promise();

  test.context.table = table;
});

function createTestStream (documentClient, table, itemsNeeded) {
  return dynamodb.query(
    documentClient,
    {
      ExpressionAttributeNames: {
        '#user': 'user'
      },
      ExpressionAttributeValues: {
        ':user': 'alice'
      },
      KeyConditionExpression: '#user = :user',
      TableName: table
    },
    itemsNeeded
  );
}

async function testStreamBehavior (test, initialRequestCount, startRequestCount, follupRequestCount) {
  const { documentClient } = test.context.dynamodb;
  const { table } = test.context;

  const items = [
    { id: 'alpha', user: 'alice' },
    { id: 'bravo', user: 'bob' },
    { id: 'charlie', user: 'alice' }
  ];

  const querySpy = sinon.spy(documentClient, 'query');

  await documentClient.batchWrite({
    RequestItems: {
      [table]: items.map((item) => ({ PutRequest: { Item: item } }))
    }
  }).promise();

  const itemRequests = new rxjs.BehaviorSubject(initialRequestCount);
  const slowIdentity = sinon.spy((item) => {
    return rxjs.from([item]).pipe(
      delay(100),
      tap(() => {
        // Whenever an item is processsed, maybe request more items
        if (follupRequestCount === 0) {
          // If no items are needed then close the stream
          itemRequests.complete();
        } else {
          itemRequests.next(follupRequestCount);
        }
      })
    );
  });
  const stream = createTestStream(documentClient, table, itemRequests);

  const arrayBuilder = stream.pipe(
    mergeMap(slowIdentity),
    toArray()
  ).toPromise();

  // After building the pipeline and waiting on a result, request some items
  if (startRequestCount > 0) {
    itemRequests.next(startRequestCount);
  }
  const results = await arrayBuilder;
  test.deepEqual(
    results.sort((left, right) => left.item.id.localeCompare(right.item.id)),
    [
      { item: items[0], table: table },
      { item: items[2], table: table }
    ]
  );

  return {
    querySpy,
    slowIdentity
  };
}

test('results can be requested one at a time', async (test) => {
  const {querySpy, slowIdentity} = await testStreamBehavior(test, 0, 1, 1);
  sinon.assert.callCount(querySpy, 3);

  // Make sure that the query for the second page does not happen until
  // the first identity is called
  test.true(querySpy.getCall(1).calledAfter(slowIdentity.getCall(0)));

  // Make sure that the query for the third page does not happen until
  // the second identity is called
  test.true(querySpy.getCall(2).calledAfter(slowIdentity.getCall(1)));
});

test('results can be requested all at once', async (test) => {
  await testStreamBehavior(test, 0, 2, 0);
});

test('when more than the available items are requested then all are returned', async (test) => {
  await testStreamBehavior(test, 0, 5, 0);
});

test('results can be requested even before the  stream is started', async (test) => {
  await testStreamBehavior(test, 2, 0, 0);
});

test('no results are returned the stream is empty', async (test) => {
  const { documentClient } = test.context.dynamodb;
  const { table } = test.context;

  const stream = createTestStream(documentClient, table, rxjs.of(100));

  const results = await stream.pipe(toArray()).toPromise();
  test.deepEqual(results, []);
});

test('when a database request fails the stream fails', async (test) => {
  const { documentClient, dynamoClient } = test.context.dynamodb;
  const { table } = test.context;

  await dynamoClient.deleteTable({ TableName: table }).promise();

  const stream = dynamodb.query(
    documentClient,
    {
      ExpressionAttributeNames: {
        '#user': 'user'
      },
      ExpressionAttributeValues: {
        ':user': 'alice'
      },
      KeyConditionExpression: '#user = :user',
      TableName: table
    },
    rxjs.of(100)
  );

  await test.throws(stream.pipe(toArray()).toPromise());
});

test('when the item request Observer fails the stream fails', async (test) => {
  const { documentClient } = test.context.dynamodb;
  const { table } = test.context;

  const stream = dynamodb.query(
    documentClient,
    {
      ExpressionAttributeNames: {
        '#user': 'user'
      },
      ExpressionAttributeValues: {
        ':user': 'alice'
      },
      KeyConditionExpression: '#user = :user',
      TableName: table
    },
    rxjs.throwError(new Error('Bad item request Observer'))
  );

  await test.throws(stream.pipe(toArray()).toPromise());
});

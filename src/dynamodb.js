const assert = require('assert');
const get = require('lodash/get');
const pick = require('lodash/pick');
const set = require('lodash/set');

const rxjs = require('rxjs');
const { from, EMPTY, of, combineLatest, BehaviorSubject } = require('rxjs');
const { bufferCount, concatAll, scan, switchMap, concatMap, map } = require('rxjs/operators');

const DELETE = 'DeleteRequest';
const PUT = 'PutRequest';

const DYNAMODB_BATCH_SIZE = 25;
const END_OF_PAGES = Symbol('END_OF_PAGES');
const NO_KEY_KNOWN = Symbol('NO_KEY_KNOWN');

exports.delete = function deleteItems (keys = [ 'id' ]) {
  return map((item) => ({ ...item, key: pick(item.item, keys), operation: DELETE }));
};

exports.put = function putItems () {
  return map((item) => ({ ...item, operation: PUT }));
};

exports.query = function query (documentClient, query, itemRequests) {
  const table = query.TableName;
  const pageStartKeys = new BehaviorSubject(null);
  const noMoreItemsWillBeRequested = new BehaviorSubject(false);

  const desiredItemAdjustments = new BehaviorSubject(0);
  const desiredItemCount = desiredItemAdjustments
    .pipe(
      scan((total, adjustment) => total + adjustment, 0)
    );

  itemRequests.subscribe(
    (value) => desiredItemAdjustments.next(value),
    (error) => desiredItemAdjustments.error(error),
    () => {
      // When the item request Observable completes, then
      // no more items are needed
      noMoreItemsWillBeRequested.next(true);
    }
  );

  return combineLatest(pageStartKeys, desiredItemCount, noMoreItemsWillBeRequested)
    .pipe(
      // Consider the start key, dsired item count and whether more
      // desired items might be requested to decide if a new page should
      // be fetched or not.
      switchMap(([startKey, itemsDesired, noMoreRequests]) => {
        if (startKey === NO_KEY_KNOWN) {
          return EMPTY;
        }
        if (startKey !== END_OF_PAGES && itemsDesired > 0) {
          return of({startKey, itemsDesired});
        } else {
          if (noMoreRequests || startKey === END_OF_PAGES) {
            desiredItemAdjustments.complete();
            noMoreItemsWillBeRequested.complete();
            pageStartKeys.complete();
          }
          return EMPTY;
        }
      }),
      // A new page was requested, so use the start key to execute
      // the query
      concatMap(({startKey, itemsDesired}) => {
        const pagedQuery = {
          ...query,
          ExclusiveStartKey: startKey,
          Limit: itemsDesired
        };

        // Now that the key has been used, replace it with
        // a Symbol that shows it as alredy used
        pageStartKeys.next(NO_KEY_KNOWN);

        return from(documentClient.query(pagedQuery).promise());
      }),
      concatMap(({ LastEvaluatedKey, Items }) => {
        // Subtract the items that will be returned from the
        // desired item count and update the next page start token
        desiredItemAdjustments.next(-1 * Items.length);
        pageStartKeys.next(LastEvaluatedKey || END_OF_PAGES);

        return from(Items);
      }),
      map(item => { return {table, item}; })
    );
};

const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

const performBatchWrite = async (documentClient, batch, attempts = 0) => {
  if (attempts >= 3) {
    throw new Error('Failed to complete batch write after multiple attempts');
  }

  if (attempts > 0) {
    await sleep(200 * attempts * attempts);
  }

  const results = await documentClient.batchWrite(batch).promise();

  if (results.UnprocessedItems && Object.keys(results.UnprocessedItems).length) {
    const batch = { RequestItems: results.UnprocessedItems };
    return performBatchWrite(documentClient, batch, attempts + 1);
  }
};

const requestForItem = (item) => {
  const request = {};

  switch (item.operation) {
    case DELETE:
      set(request, item.operation, { Key: item.key });
      break;
    case PUT:
      set(request, item.operation, { Item: item.item });
      break;
  }

  assert(Object.keys(request).length === 1, `Invalid write operation '${item.operation}'`);
  return request;
};

exports.write = function write (documentClient) {
  const writeBufferSpace = new BehaviorSubject(DYNAMODB_BATCH_SIZE);

  const write = rxjs.pipe(
    // DynamoDB restricts batchWrites to batches of `DYNAMODB_BATCH_SIZE`
    // See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-api
    bufferCount(DYNAMODB_BATCH_SIZE),
    concatMap(async (batch) => {
      // Now that a batch of item is ready, request another batch
      writeBufferSpace.next(DYNAMODB_BATCH_SIZE);

      const request = {
        RequestItems: batch.reduce(
          (requests, item) => {
            const table = get(requests, item.table, []);
            const request = requestForItem(item);
            table.push(request);
            return { ...requests, [item.table]: table };
          },
          {}
        )
      };

      await performBatchWrite(documentClient, request);
      // Re-emit the successfully processed operations
      return from(batch);
    }),
    concatAll()
  );

  return {writeBufferSpace, write};
};

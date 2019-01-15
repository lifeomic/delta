const { from, EMPTY, of, combineLatest, BehaviorSubject } = require('rxjs');
const { scan, switchMap, concatMap, map } = require('rxjs/operators');

const NO_KEY_KNOWN = Symbol('NO_KEY_KNOWN');
const END_OF_PAGES = Symbol('END_OF_PAGES');

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
          return of(startKey);
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
      concatMap((startKey) => {
        const pagedQuery = {
          ...query,
          ExclusiveStartKey: startKey,
          // Paging is forced during testing
          Limit: process.env.DYNAMODB_DEFAULT_PAGE_SIZE
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

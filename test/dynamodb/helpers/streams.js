const get = require('lodash/get');
const isEqual = require('lodash/isEqual');
const take = require('lodash/take');
const drop = require('lodash/drop');

const rxjs = require('rxjs');
const {concatMap, takeWhile} = require('rxjs/operators');

exports.requestToDeleteItem = (table, id) => (request) => get(request, `RequestItems.${table}`)
  .filter((request) => request.hasOwnProperty('DeleteRequest'))
  .map((request) => request.DeleteRequest.Key.id)
  .includes(id);

exports.requestToUpdateItem = (table, expected) => (request) => get(request, `RequestItems.${table}`)
  .filter((request) => request.hasOwnProperty('PutRequest'))
  .map((request) => request.PutRequest.Item)
  .some((actual) => isEqual(actual, expected));

const END_MARKER = Symbol('End of mock query');

exports.mockItemQueryStream = (items, itemRequests) => {
  return itemRequests
    .pipe(
      concatMap(count => {
        const itemsToReturn = take(items, count);
        items = drop(items, count);

        if (items.length === 0) {
          itemsToReturn.push(END_MARKER);
        }

        return rxjs.from(itemsToReturn);
      }),
      takeWhile(item => item !== END_MARKER)
    );
};

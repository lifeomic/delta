const get = require('lodash/get');
const isEqual = require('lodash/isEqual');

exports.requestToDeleteItem = (table, id) => (request) => get(request, `RequestItems.${table}`)
  .filter((request) => request.hasOwnProperty('DeleteRequest'))
  .map((request) => request.DeleteRequest.Key.id)
  .includes(id);

exports.requestToUpdateItem = (table, expected) => (request) => get(request, `RequestItems.${table}`)
  .filter((request) => request.hasOwnProperty('PutRequest'))
  .map((request) => request.PutRequest.Item)
  .some((actual) => isEqual(actual, expected));

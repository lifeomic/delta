const { UnhandledEventError } = require('./errors');

exports.pipeline = (...steps) => async (event, context) => {
  let value = event;

  while (steps.length && value !== undefined) {
    const step = steps.shift();
    value = await step(value, context);
  }

  return value;
};

exports.some = (...handlers) => async (event, context) => {
  let result;

  while (result === undefined && handlers.length) {
    const handler = handlers.shift();
    result = await handler(event, context);
  }

  if (result === undefined) {
    throw new UnhandledEventError(event);
  }

  return result;
};

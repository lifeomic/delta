const rxjs = require('rxjs');
const sinon = require('sinon');
const test = require('ava');

const { count, delay, flatMap, ignoreElements, mapTo, toArray } = require('rxjs/operators');
const { generators, handlers } = require('..');

test('some returns the last operator emission', async (test) => {
  const context = {};
  const event = {};

  const one = () => ignoreElements();
  const two = () => mapTo('hello');
  const three = () => rxjs.pipe(mapTo('goodbye'), delay(100));

  const handler = handlers.some(one, two, three);
  const result = await handler(event, context);
  test.is(result, 'goodbye');
});

test('some passes the context to each operator factory', async (test) => {
  const context = {};
  const event = {};

  const one = sinon.stub().returns(mapTo('one'));
  const two = sinon.stub().returns(mapTo('two'));
  const three = sinon.stub().returns(mapTo('three'));

  const handler = handlers.some(one, two, three);
  await handler(event, context);
  sinon.assert.calledWithExactly(one, context);
  sinon.assert.calledWithExactly(two, context);
  sinon.assert.calledWithExactly(three, context);
});

test('some throws an error if no operators emit', async (test) => {
  const context = {};
  const event = {};

  const one = () => ignoreElements();
  const two = () => ignoreElements();
  const three = () => ignoreElements();

  const handler = handlers.some(one, two, three);
  await test.throws(handler(event, context), /no elements/);
});

test('some throws if one of the handlers throws', async (test) => {
  const context = {};
  const event = {};

  const failure = new Error('simulated failure');
  const one = () => ignoreElements();
  const two = () => flatMap(() => Promise.reject(failure));
  const three = () => mapTo('hello');

  const handler = handlers.some(one, two, three);
  const error = await test.throws(handler(event, context));

  test.is(error, failure);
});

test('withContext wraps a handler with a context transformation', async (test) => {
  const baseHandler = sinon.stub().resolves('hello');
  const context = { value: 1 };
  const event = {};

  const wrappedHandler = handlers.withContext(
    (context) => ({ value: context.value + 1 }),
    baseHandler
  );

  const result = await wrappedHandler(event, context);
  test.is(result, 'hello');
  sinon.assert.calledOnce(baseHandler);
  sinon.assert.calledWithExactly(baseHandler, event, { value: 2 });
});

test('the record extractor emits no records for non-stream payloads', async (test) => {
  const event = {};

  const result = await rxjs.of(event)
    .pipe(handlers.records(), count())
    .toPromise();

  test.is(result, 0);
});

test('the record extractor emits no records for empty stream payloads', async (test) => {
  const event = { Records: [] };

  const result = await rxjs.of(event)
    .pipe(handlers.records(), count())
    .toPromise();

  test.is(result, 0);
});

test('the record extractor emits each record in stream payloads', async (test) => {
  const event = {
    Records: [
      { name: 'one' },
      { name: 'two' },
      { name: 'three' }
    ]
  };

  const result = await rxjs.of(event)
    .pipe(handlers.records(), toArray())
    .toPromise();

  test.deepEqual(result, event.Records);
});

test('the scheduled event filter re-emits cloudwatch schedule events', async (test) => {
  const event = generators.scheduledEvent();

  const result = await rxjs.of(event)
    .pipe(handlers.followSchedule(), toArray())
    .toPromise();

  test.deepEqual(result, [ event ]);
});

test('the scheduled event filter does not re-emit other events', async (test) => {
  const event = {};

  const result = await rxjs.of(event)
    .pipe(handlers.followSchedule(), count())
    .toPromise();

  test.is(result, 0);
});

test('the scheduled event filter can be scoped to a specific schedule', async (test) => {
  const genericEvent = generators.scheduledEvent();
  const myScheduleEvent = generators.scheduledEvent({ schedule: 'arn:aws:events:us-east-1:123456789012:rule/my-schedule' });
  const otherScheduleEvent = generators.scheduledEvent({ schedule: 'arn:aws:events:us-east-1:123456789012:rule/other-schedule' });

  const result = await rxjs.of(genericEvent, myScheduleEvent, otherScheduleEvent)
    .pipe(handlers.followSchedule({ schedule: 'arn:aws:events:us-east-1:123456789012:rule/my-schedule' }), toArray())
    .toPromise();

  test.deepEqual(result, [ myScheduleEvent ]);
});

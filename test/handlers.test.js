const sinon = require('sinon');
const test = require('ava');

const { handlers } = require('..');
const { UnhandledEventError } = require('..').errors;

test.beforeEach((test) => {
  test.context = {
    context: {},
    event: {}
  };
});

test('some returns the first handler response', async (test) => {
  const { context, event } = test.context;

  const one = sinon.stub().resolves();
  const two = sinon.stub().resolves('hello');
  const three = sinon.stub().resolves('goodbye');

  const handler = handlers.some(one, two, three);
  const result = await handler(context, event);
  test.is(result, 'hello');
  sinon.assert.calledWithExactly(one, event, context);
  sinon.assert.calledWithExactly(two, event, context);
  sinon.assert.notCalled(three);
});

test('some throws an error if no handlers respond', async (test) => {
  const { context, event } = test.context;

  const one = sinon.stub().resolves();
  const two = sinon.stub().resolves();
  const three = sinon.stub().resolves();

  const handler = handlers.some(one, two, three);
  await test.throws(handler(context, event), UnhandledEventError);

  sinon.assert.calledWithExactly(one, event, context);
  sinon.assert.calledWithExactly(two, event, context);
  sinon.assert.calledWithExactly(three, event, context);
});

test('some throws if one of the handlers throws', async (test) => {
  const { context, event } = test.context;

  const failure = new Error('simulated failure');
  const one = sinon.stub().resolves();
  const two = sinon.stub().rejects(failure);
  const three = sinon.stub().resolves('hello');

  const handler = handlers.some(one, two, three);
  const error = await test.throws(handler(context, event));

  test.is(error, failure);
  sinon.assert.calledWithExactly(one, event, context);
  sinon.assert.calledWithExactly(two, event, context);
  sinon.assert.notCalled(three);
});

test('pipeline invokes each step with the return value of the previous step', async (test) => {
  const { context, event } = test.context;

  const one = sinon.stub().resolves('one');
  const two = sinon.stub().resolves('two');
  const three = sinon.stub().resolves('three');

  const handler = handlers.pipeline(one, two, three);
  await handler(event, context);

  sinon.assert.calledWithExactly(one, event, context);
  sinon.assert.calledWithExactly(two, 'one', context);
  sinon.assert.calledWithExactly(three, 'two', context);
});

test('pipeline returns undefined at the first step that returns undefined', async (test) => {
  const { context, event } = test.context;

  const one = sinon.stub().resolves('one');
  const two = sinon.stub().resolves();
  const three = sinon.stub().resolves('three');

  const handler = handlers.pipeline(one, two, three);
  const result = await handler(event, context);

  test.is(result, undefined);
  sinon.assert.calledWithExactly(one, event, context);
  sinon.assert.calledWithExactly(two, 'one', context);
  sinon.assert.notCalled(three);
});

test('pipeline returns the return value of the last step', async (test) => {
  const { context, event } = test.context;

  const one = sinon.stub().resolves('one');
  const two = sinon.stub().resolves('two');
  const three = sinon.stub().resolves('three');

  const handler = handlers.pipeline(one, two, three);
  const result = await handler(event, context);
  test.is(result, 'three');
});

test('pipeline throws if a step throws', async (test) => {
  const { context, event } = test.context;

  const failure = new Error('simulated failure');
  const one = sinon.stub().resolves('one');
  const two = sinon.stub().rejects(failure);
  const three = sinon.stub().resolves('three');

  const handler = handlers.pipeline(one, two, three);
  const error = await test.throws(handler(event, context));

  test.is(error, failure);
  sinon.assert.calledWithExactly(one, event, context);
  sinon.assert.calledWithExactly(two, 'one', context);
  sinon.assert.notCalled(three);
});

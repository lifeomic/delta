Delta provides tooling for creating and testing AWS Lambda functions that
process AWS data streams.

## Usage

```bash
yarn add @lifeomic/delta
```

### `DynamoStreamHandler`

This helper provides an abstraction over a DynamoDB Stream Lambda handler.

```typescript
import { DynamoStreamHandler } from '@lifeomic/delta';

const stream = new DynamoStreamHandler({
  logger,
  parse: (item) => {
    // parse the item using your custom logic, e.g. using zod or ajv.
    return { id: item.id };
  },
  createRunContext: () => {
    /* ... create the "context", e.g. data sources ... */
    return { doSomething: () => null };
  },
  // Optionally specify a concurrency setting for processing events.
  concurrency: 5,
})
  .onInsert(async (ctx, entity) => {
    // INSERT actions receive a single strongly typed new entities
    // (entities are typed based on the `parse` function)
    entity.id;

    // `ctx` contains the nice result of `createRunContext`
    await ctx.doSomething();

    // `ctx` contains a logger by default, which already includes niceties like
    // the AWS request id
    ctx.logger.info('blah blah');
  })
  // The API is chainable to help with readability
  .onModify(async (ctx, oldEntity, newEntity) => {
    // MODIFY actions receive strongly typed old + new entities
    oldEntity.id;
    newEntity.id;
  })
  .onRemove(async (ctx, oldEntity) => {
    // REMOVE actions receive a single strongly typed old entity
    oldEntity.id;

    ctx.logger.info('first remove action');
  })
  // When multiple actions have been added for the same event (e.g. two `onRemove` calls),
  // they are executed in.order.
  .onRemove(async (ctx, oldEntity) => {
    ctx.logger.info('second remove action');
  });

// Provides a dead-simple API for creating the Lambda.
export const handler = stream.lambda();
```

`DynamoStreamHandler` also comes with a nice helper for testing: `harness(...)`

```typescript
const context = {
  doSomething: jest.fn()
}

const harness = stream.harness({
  /* optionally override the logger */
  logger,
  createRunContext: () => {
    /* optionally override the context, to mock e.g. data sources */
    return context;
  }
})

test('something', async () => {
  // Provides a simple `sendEvent` function
  await harness.sendEvent({
    records: [
      // Simplified, strongly-typed event types for readability
      { type: 'remove', entity: ... },
      { type: 'insert', entity: ... },
      { type: 'modify', oldEntity: ..., newEntity: ... },
    ]
  })

  expect(context.doSomething).toHaveBeenCalled()
})
```

### `SQSMessageHandler`

This helper provides an abstraction over a SQS message Lambda handler.

```typescript
import { SQSMessageHandler } from '@lifeomic/delta';

const queue = new SQSMessageHandler({
  logger,
  parseMessage: (message) => {
    /* ... parse from message string -> your custom type ... */
    return JSON.parse(message);
  },
  createRunContext: () => {
    /* ... create the "context", e.g. data sources ... */
    return { doSomething: () => null };
  },
  // Optionally specify a concurrency setting for processing events.
  concurrency: 5,
})
  .onMessage(async (ctx, message) => {
    // `ctx` contains the nice result of `createRunContext`:
    await ctx.doSomething();

    // `ctx` contains a logger by default, which already includes niceties like
    // the AWS request id
    ctx.logger.info('blah blah');
  })
  // Add multiple message handlers for code organization.
  .onMessage(async (ctx, message) => {
    // do something else
  });

// Provides a dead-simple API for creating the Lambda.
export const handler = stream.lambda();
```

`SQSMessageHandler` also comes with a nice helper for testing: `harness(...)`

```typescript
const context = {
  doSomething: jest.fn()
}

const harness = queue.harness({
  stringifyMessage: (message) => {
    /* stringify from your custom type -> string */
    return JSON.stringify(message)
  },
  /* optionally override the logger */
  logger,
  createRunContext: () => {
    /* optionally override the context, to mock e.g. data sources */
    return context;
  }
})

test('something', async () => {
  // Provides a simple `sendEvent` function
  await harness.sendEvent({
    message: [
      { /* message 1 */}
      { /* message 2 */}
      { /* message 3 */}
    ]
  })

  expect(context.doSomething).toHaveBeenCalledTimes(3)
})
```

### Parallel Processing + Ordering

By default, the abstractions in `@lifeomic/delta` (`DynamoStreamHandler` and `SQSMessageHandler`) will process events in parallel. To control the parallelization, specify a `concurrency` value when creating the handler.

These abstractions also ensure that within a batch of events correct _ordering_ of events is maintained according to the ordering semantics of the upstream event source, even when processing in parallel.

In `DynamoStreamHandler`, events for the same _key_ will always be processed serially -- events from different keys will be processed in parallel.

In `SQSMessageHandler`, events with the same `MessageGroupId` will always processed serially -- events with different `MessageGroupId` values will be processed in parallel.

**Note**: while the ordering semantics above will always be preserved, events that do _not_ need to be ordered will not necessarily be processed in the same order they were received in the batch (even when using a `concurrency` value of `1`).

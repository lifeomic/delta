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
  // Optionally specify a list of image keys to obfuscate the values of
  loggerObfuscateImageKeys: ['api-secret'],
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

### Message Handlers (SQS & SNS)

Both `SQSMessageHandler` and `SNSMessageHandler` provide a unified abstraction for processing message-based Lambda events. They share the same core functionality through a common base class, with only the underlying AWS event structure differing.

#### Shared Configuration & Usage

Both handlers support the same configuration options and API:

```typescript
import { SQSMessageHandler, SNSMessageHandler } from '@lifeomic/delta';

// Same configuration for both handlers
const config = {
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
};

// Use with SQS
const sqsHandler = new SQSMessageHandler(config)
  .onMessage(async (ctx, message) => {
    // `ctx` contains the nice result of `createRunContext`:
    await ctx.doSomething();
    // `ctx` contains a logger by default, which already includes niceties like
    // the AWS request id
    ctx.logger.info('processed SQS message');
  })
  // Add multiple message handlers for code organization.
  .onMessage(async (ctx, message) => {
    // do something else
  });

// Or with SNS - identical API
const snsHandler = new SNSMessageHandler(config)
  .onMessage(async (ctx, message) => {
    // `ctx` contains the nice result of `createRunContext`:
    await ctx.doSomething();
    // `ctx` contains a logger by default, which already includes niceties like
    // the AWS request id
    ctx.logger.info('processed SNS message');
  })
  // Add multiple message handlers for code organization.
  .onMessage(async (ctx, message) => {
    // do something else
  });

// Both provide the same lambda() method
export const sqsLambda = sqsHandler.lambda();
export const snsLambda = snsHandler.lambda();
```

#### Shared Testing Harness

Both handlers provide identical testing harnesses:

```typescript
const context = { doSomething: jest.fn() };

// Same harness API for both handlers
const testConfig = {
  stringifyMessage: (message) => JSON.stringify(message),
  logger,
  createRunContext: () => context,
};

const sqsHarness = sqsHandler.harness(testConfig);
const snsHarness = snsHandler.harness(testConfig);

test('SQS messages', async () => {
  await sqsHarness.sendEvent({
    messages: [{ id: 1 }, { id: 2 }],
  });
  expect(context.doSomething).toHaveBeenCalledTimes(2);
});

test('SNS messages', async () => {
  await snsHarness.sendEvent({
    messages: [{ id: 1 }, { id: 2 }],
  });
  expect(context.doSomething).toHaveBeenCalledTimes(2);
});
```

#### Key Differences

- **`SQSMessageHandler`**: Processes SQS queue messages, uses `MessageGroupId` for ordering
- **`SNSMessageHandler`**: Processes SNS topic notifications, uses `MessageId` for ordering

Both handlers support the same features: message parsing, context creation, concurrency control, redaction, partial batch responses, and comprehensive error handling.

### `KinesisEventHandler`

This helper provides an abstraction over a Kinesis stream Lambda handler.

```typescript
import { KinesisEventHandler } from '@lifeomic/delta';

const queue = new KinesisEventHandler({
  logger,
  parseEvent: (event) => {
    /* ... parse from event data -> your custom type ... */
    return JSON.parse(event);
  },
  createRunContext: () => {
    /* ... create the "context", e.g. data sources ... */
    return { doSomething: () => null };
  },
  // Optionally specify a concurrency setting for processing events.
  concurrency: 5,
})
  .onEvent(async (ctx, event) => {
    // `ctx` contains the nice result of `createRunContext`:
    await ctx.doSomething();

    // `ctx` contains a logger by default, which already includes niceties like
    // the AWS request id
    ctx.logger.info('blah blah');
  })
  // Add multiple event handlers for code organization.
  .onEvent(async (ctx, event) => {
    // do something else
  });

// Provides a dead-simple API for creating the Lambda.
export const handler = stream.lambda();
```

`KinesisEventHandler` also comes with a nice helper for testing: `harness(...)`

```typescript
const context = {
  doSomething: jest.fn()
}

const harness = queue.harness({
  stringifyEvent: (event) => {
    /* stringify from your custom type -> string */
    return JSON.stringify(event)
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
    events: [
      { /* event 1 */}
      { /* event 2 */}
      { /* event 3 */}
    ]
  })

  expect(context.doSomething).toHaveBeenCalledTimes(3)
})
```

### Parallel Processing + Ordering

By default, the abstractions in `@lifeomic/delta` will process events in parallel. To control the
parallelization, specify a `concurrency` value when creating the handler.

These abstractions also ensure that within a batch of events correct _ordering_
of events is maintained according to the ordering semantics of the upstream
event source, even when processing in parallel.

In `DynamoStreamHandler`, events for the same _key_ will always be processed
serially -- events from different keys will be processed in parallel.

In `SQSMessageHandler` and `SNSMessageHandler`, ordering is maintained by:

- **SQS**: Events with the same `MessageGroupId` are processed serially
- **SNS**: Events are ordered by `MessageId` for consistency

Events with different ordering keys are processed in parallel.

In `KinesisEventHandler`, events with the same `partitionKey` will always
processed serially -- events with different `partitionKey` values will be
processed in parallel.

**Note**: while the ordering semantics above will always be preserved, events
that do _not_ need to be ordered will not necessarily be processed in the same
order they were received in the batch (even when using a `concurrency` value of
`1`).

### Partial Batch Responses

All of the handlers in `@lifeomic/delta` support returning [partial batch responses](https://docs.aws.amazon.com/prescriptive-guidance/latest/lambda-event-filtering-partial-batch-responses-for-sqs/best-practices-partial-batch-responses.html). This behavior can be enabled
by specifying the `usePartialBatchResponses` configuration option:

```typescript
// Dynamo
const stream = new DynamoStreamHandler({
  // ...
  usePartialBatchResponses: true,
});

// Kinesis
const stream = new KinesisEventHandler({
  // ...
  usePartialBatchResponses: true,
});

// SQS & SNS (same configuration)
const messageHandler = new SQSMessageHandler({
  // ... or SNSMessageHandler
  usePartialBatchResponses: true,
});
```

When `usePartialBatchResponses` is enabled, the handler will return a set of
`batchItemFailures`. If events are ordered, ordering is preserved correctly.

**Note**: When enabling this option, be sure to _also_ configure the correct
[`FunctionResponseTypes`](https://docs.aws.amazon.com/lambda/latest/api/API_CreateEventSourceMapping.html#lambda-CreateEventSourceMapping-request-FunctionResponseTypes) in your Lambda event source mapping.

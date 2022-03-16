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
  unmarshall: (object) => {
    /* ... unmarshall from unknown stream format -> your custom type ... */
    return { id: object.id };
  },
  createRunContext: () => {
    /* ... create the "context", e.g. data sources ... */
    return { doSomething: () => null };
  },
})
  .onInsert(async (ctx, entity) => {
    // INSERT actions receive a single strongly typed new entities
    // (entities are typed based on the `unmarshall` function)
    entity.id;

    // `ctx` contains the nice result of `createRunContext`
    await ctx.dataSources.doSomething();

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

`DynamoStreamHelper` also comes with a nice helper for testing: `harness(...)`

```typescript
const context = {
  doSomething: jest.fn()
}

const harness = stream.harness({
  marshall: () => {
    /* marshall from your custom type -> stream format */
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
    records: [
      // Simplified, strongly-typed event types for readability
      { type: 'remove', entity: ... },
      { type: 'insert', entity: ... },
      { type: 'modify', oldEntity: ..., newEntity: ... },
    ]
  })

  expect(context.dataSources.doSomething).toHaveBeenCalled()
})
```

/* eslint-disable @typescript-eslint/no-unsafe-argument */
import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { DynamoStreamHandler } from './dynamo-streams';

const testSerializer = {
  unmarshall: (object: any) => object.marshalled,
  marshall: (object: any) => ({ marshalled: object }),
};

const logger: jest.Mocked<LoggerInterface> = {
  info: jest.fn(),
  error: jest.fn(),
  child: jest.fn(),
} as any;

const dataSources = {
  doSomething: jest.fn(),
};

beforeEach(() => {
  logger.info.mockReset();
  logger.error.mockReset();
  logger.child.mockReset();
  logger.child.mockImplementation(() => logger);
  dataSources.doSomething.mockReset();
});

describe('DynamoStreamHandler', () => {
  test('responds to HTTP health checks', async () => {
    const lambda = new DynamoStreamHandler({
      logger,
      unmarshall: testSerializer.unmarshall,
      createRunContext: () => ({}),
    }).lambda();

    const result = await lambda(
      { httpMethod: 'GET' } as any,
      {} as any,
      {} as any,
    );

    expect(result).toStrictEqual({
      statusCode: 200,
      body: '{"healthy":true}',
    });
  });

  test('handles insert events', async () => {
    const lambda = new DynamoStreamHandler({
      logger,
      unmarshall: testSerializer.unmarshall,
      createRunContext: () => ({ logger, dataSources }),
    })
      .onInsert((ctx, entity) => {
        ctx.dataSources.doSomething(entity);
      })
      .lambda();

    await lambda(
      {
        Records: [
          {
            eventName: 'INSERT',
            dynamodb: { NewImage: { marshalled: { id: 'new-insert' } } as any },
          },
        ],
      },
      {} as any,
      {} as any,
    );

    expect(dataSources.doSomething).toHaveBeenCalledTimes(1);
    expect(dataSources.doSomething).toHaveBeenCalledWith({
      id: 'new-insert',
    });
  });

  test('handles modify events', async () => {
    const lambda = new DynamoStreamHandler({
      logger,
      unmarshall: testSerializer.unmarshall,
      createRunContext: () => ({ logger, dataSources }),
    })
      .onModify((ctx, oldEntity, newEntity) => {
        ctx.dataSources.doSomething(oldEntity, newEntity);
      })
      .lambda();

    await lambda(
      {
        Records: [
          {
            eventName: 'MODIFY',
            dynamodb: {
              OldImage: { marshalled: { id: 'old-modify' } } as any,
              NewImage: { marshalled: { id: 'new-modify' } } as any,
            },
          },
        ],
      },
      {} as any,
      {} as any,
    );

    expect(dataSources.doSomething).toHaveBeenCalledTimes(1);
    expect(dataSources.doSomething).toHaveBeenCalledWith(
      { id: 'old-modify' },
      { id: 'new-modify' },
    );
  });

  test('handles remove events', async () => {
    const lambda = new DynamoStreamHandler({
      logger,
      unmarshall: testSerializer.unmarshall,
      createRunContext: () => ({ logger, dataSources }),
    })
      .onRemove((ctx, entity) => {
        ctx.dataSources.doSomething(entity);
      })
      .lambda();

    await lambda(
      {
        Records: [
          {
            eventName: 'REMOVE',
            dynamodb: {
              OldImage: { marshalled: { id: 'old-remove' } } as any,
            },
          },
        ],
      },
      {} as any,
      {} as any,
    );

    expect(dataSources.doSomething).toHaveBeenCalledTimes(1);
    expect(dataSources.doSomething).toHaveBeenCalledWith({
      id: 'old-remove',
    });
  });

  test('handles a variety of events', async () => {
    const lambda = new DynamoStreamHandler({
      logger,
      unmarshall: testSerializer.unmarshall,
      createRunContext: () => ({ logger, dataSources }),
    })
      // onInsert twice to test same event through multiple actions
      .onInsert((ctx, entity) => {
        ctx.dataSources.doSomething(entity);
      })
      .onInsert((ctx, entity) => {
        ctx.dataSources.doSomething(entity);
      })
      .onModify((ctx, oldEntity, newEntity) => {
        ctx.dataSources.doSomething(oldEntity, newEntity);
      })
      .onRemove((ctx, entity) => {
        ctx.dataSources.doSomething(entity);
      })
      .lambda();

    await lambda(
      {
        Records: [
          {
            eventName: 'INSERT',
            dynamodb: {
              NewImage: {
                marshalled: { id: 'new-insert-varied-lambda' },
              } as any,
            },
          },
          {
            eventName: 'MODIFY',
            dynamodb: {
              OldImage: {
                marshalled: { id: 'old-modify-varied-lambda' },
              } as any,
              NewImage: {
                marshalled: { id: 'new-modify-varied-lambda' },
              } as any,
            },
          },
          {
            eventName: 'REMOVE',
            dynamodb: {
              OldImage: {
                marshalled: { id: 'old-remove-varied-lambda' },
              } as any,
            },
          },
          // A second remove event to test multiple events through a single action
          {
            eventName: 'REMOVE',
            dynamodb: {
              OldImage: {
                marshalled: { id: 'old-remove-varied-lambda-second' },
              } as any,
            },
          },
        ],
      },
      {} as any,
      {} as any,
    );

    expect(dataSources.doSomething).toHaveBeenCalledTimes(5);

    expect(dataSources.doSomething).toHaveBeenNthCalledWith(1, {
      id: 'new-insert-varied-lambda',
    });
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(2, {
      id: 'new-insert-varied-lambda',
    });
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      3,
      { id: 'old-modify-varied-lambda' },
      { id: 'new-modify-varied-lambda' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(4, {
      id: 'old-remove-varied-lambda',
    });
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(5, {
      id: 'old-remove-varied-lambda-second',
    });
  });

  describe('harness', () => {
    test('sends insert event', async () => {
      const { sendEvent } = new DynamoStreamHandler({
        logger,
        unmarshall: testSerializer.unmarshall,
        createRunContext: () => ({ dataSources }),
      })
        .onInsert((ctx, entity) => {
          ctx.dataSources.doSomething(entity);
        })
        .harness({ marshall: testSerializer.marshall });

      await sendEvent({
        records: [{ type: 'insert', entity: { id: 'new-insert' } }],
      });

      expect(dataSources.doSomething).toHaveBeenCalledTimes(1);
      expect(dataSources.doSomething).toHaveBeenCalledWith({
        id: 'new-insert',
      });
    });

    test('sends modify event', async () => {
      const { sendEvent } = new DynamoStreamHandler({
        logger,
        unmarshall: testSerializer.unmarshall,
        createRunContext: () => ({ dataSources }),
      })
        .onModify((ctx, oldEntity, newEntity) => {
          ctx.dataSources.doSomething(oldEntity, newEntity);
        })
        .harness({ marshall: testSerializer.marshall });

      await sendEvent({
        records: [
          {
            type: 'modify',
            oldEntity: { id: 'old-modify' },
            newEntity: { id: 'new-modify' },
          },
        ],
      });

      expect(dataSources.doSomething).toHaveBeenCalledTimes(1);
      expect(dataSources.doSomething).toHaveBeenCalledWith(
        { id: 'old-modify' },
        { id: 'new-modify' },
      );
    });

    test('sends remove event', async () => {
      const { sendEvent } = new DynamoStreamHandler({
        logger,
        unmarshall: testSerializer.unmarshall,
        createRunContext: () => ({ dataSources }),
      })
        .onRemove((ctx, entity) => {
          ctx.dataSources.doSomething(entity);
        })
        .harness({ marshall: testSerializer.marshall });

      await sendEvent({
        records: [{ type: 'remove', entity: { id: 'old-remove' } }],
      });

      expect(dataSources.doSomething).toHaveBeenCalledTimes(1);
      expect(dataSources.doSomething).toHaveBeenCalledWith({
        id: 'old-remove',
      });
    });

    test('sends a variety of events', async () => {
      const { sendEvent } = new DynamoStreamHandler({
        logger,
        unmarshall: testSerializer.unmarshall,
        createRunContext: () => ({ dataSources }),
      })
        // onInsert twice to test same event through multiple actions
        .onInsert((ctx, entity) => {
          ctx.dataSources.doSomething(entity);
        })
        .onInsert((ctx, entity) => {
          ctx.dataSources.doSomething(entity);
        })
        .onModify((ctx, oldEntity, newEntity) => {
          ctx.dataSources.doSomething(oldEntity, newEntity);
        })
        .onRemove((ctx, entity) => {
          ctx.dataSources.doSomething(entity);
        })
        .harness({ marshall: testSerializer.marshall });

      await sendEvent({
        records: [
          { type: 'insert', entity: { id: 'new-insert-varied-harness' } },
          {
            type: 'modify',
            oldEntity: { id: 'old-modify-varied-harness' },
            newEntity: { id: 'new-modify-varied-harness' },
          },
          { type: 'remove', entity: { id: 'old-remove-varied-harness' } },
          // A second remove event to test multiple events through a single action
          {
            type: 'remove',
            entity: { id: 'old-remove-varied-harness-second' },
          },
        ],
      });

      expect(dataSources.doSomething).toHaveBeenCalledTimes(5);

      expect(dataSources.doSomething).toHaveBeenNthCalledWith(1, {
        id: 'new-insert-varied-harness',
      });
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(2, {
        id: 'new-insert-varied-harness',
      });
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(
        3,
        { id: 'old-modify-varied-harness' },
        { id: 'new-modify-varied-harness' },
      );
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(4, {
        id: 'old-remove-varied-harness',
      });
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(5, {
        id: 'old-remove-varied-harness-second',
      });
    });

    test('allows overriding context and logger', async () => {
      const testValue = uuid();

      const overrideLogger = {
        info: jest.fn(),
        error: jest.fn(),
        child: jest.fn(),
      };
      overrideLogger.child.mockImplementation(() => overrideLogger);
      const { sendEvent } = new DynamoStreamHandler({
        logger,
        unmarshall: testSerializer.unmarshall,
        createRunContext: () => ({ dataSources }),
      })
        .onInsert((ctx) => {
          ctx.logger.info(testValue);
          ctx.dataSources.doSomething((ctx as any).testValue);
        })
        .harness({
          marshall: testSerializer.marshall,
          logger: overrideLogger as any,
          createRunContext: () => ({ dataSources, testValue }),
        });

      await sendEvent({
        records: [{ type: 'insert', entity: { id: 'new-insert' } }],
      });

      expect(logger.info).not.toHaveBeenCalled();
      expect(overrideLogger.info).toHaveBeenCalledWith(testValue);

      expect(dataSources.doSomething).toHaveBeenCalledWith(testValue);
    });
  });

  describe('error scenarios', () => {
    const lambda = new DynamoStreamHandler({
      logger,
      unmarshall: testSerializer.unmarshall,
      createRunContext: () => ({ logger, dataSources }),
    }).lambda();

    test('no dynamodb property', async () => {
      await lambda(
        { Records: [{ eventName: 'INSERT' }] },
        {} as any,
        {} as any,
      );

      expect(logger.error).toHaveBeenCalledWith(
        expect.anything(),
        'The dynamodb property was not present on event',
      );
    });

    test('INSERT with no NewImage', async () => {
      await lambda(
        { Records: [{ eventName: 'INSERT', dynamodb: {} }] },
        {} as any,
        {} as any,
      );

      expect(logger.error).toHaveBeenCalledWith(
        expect.anything(),
        'No NewImage was defined for an INSERT event',
      );
    });

    test('MODIFY with no NewImage', async () => {
      await lambda(
        {
          Records: [
            { eventName: 'MODIFY', dynamodb: { OldImage: { marshalled: {} } } },
          ],
        },
        {} as any,
        {} as any,
      );

      expect(logger.error).toHaveBeenCalledWith(
        expect.anything(),
        'No NewImage was defined for a MODIFY event',
      );
    });

    test('MODIFY with no OldImage', async () => {
      await lambda(
        {
          Records: [
            { eventName: 'MODIFY', dynamodb: { NewImage: { marshalled: {} } } },
          ],
        },
        {} as any,
        {} as any,
      );

      expect(logger.error).toHaveBeenCalledWith(
        expect.anything(),
        'No OldImage was defined for a MODIFY event',
      );
    });

    test('REMOVE with no OldImage', async () => {
      await lambda(
        { Records: [{ eventName: 'REMOVE', dynamodb: {} }] },
        {} as any,
        {} as any,
      );

      expect(logger.error).toHaveBeenCalledWith(
        expect.anything(),
        'No OldImage was defined for a REMOVE event',
      );
    });
  });
});

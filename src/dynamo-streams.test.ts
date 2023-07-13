import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { DynamoStreamHandler } from './dynamo-streams';
import { marshall } from '@aws-sdk/util-dynamodb';
import { z } from 'zod';

const TestSchema = z.object({ id: z.string(), name: z.string().optional() });

const testSerializer = {
  parse: (object: any) => TestSchema.parse(object),
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
      parse: testSerializer.parse,
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

  test('responds to healthCheck events', async () => {
    const lambda = new DynamoStreamHandler({
      logger,
      parse: testSerializer.parse,
      createRunContext: () => ({}),
    }).lambda();

    const result = await lambda(
      { healthCheck: true } as any,
      {} as any,
      {} as any,
    );

    expect(result).toStrictEqual({
      healthy: true,
    });
  });

  test('handles insert events', async () => {
    const lambda = new DynamoStreamHandler({
      logger,
      parse: testSerializer.parse,
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
            dynamodb: { NewImage: { id: { S: 'new-insert' } } },
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
      parse: testSerializer.parse,
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
              OldImage: marshall({ id: 'old-modify' }) as any,
              NewImage: marshall({ id: 'new-modify' }) as any,
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
      parse: testSerializer.parse,
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
              OldImage: marshall({ id: 'old-remove' }) as any,
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
      parse: testSerializer.parse,
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
              NewImage: marshall({ id: 'new-insert-varied-lambda' }) as any,
            },
          },
          {
            eventName: 'MODIFY',
            dynamodb: {
              OldImage: marshall({ id: 'old-modify-varied-lambda' }) as any,
              NewImage: marshall({ id: 'new-modify-varied-lambda' }) as any,
            },
          },
          {
            eventName: 'REMOVE',
            dynamodb: {
              OldImage: marshall({ id: 'old-remove-varied-lambda' }) as any,
            },
          },
          // A second remove event to test multiple events through a single action
          {
            eventName: 'REMOVE',
            dynamodb: {
              OldImage: marshall({ id: 'old-remove-varied-lambda-second' }),
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
        parse: testSerializer.parse,
        createRunContext: () => ({ dataSources }),
      })
        .onInsert((ctx, entity) => {
          ctx.dataSources.doSomething(entity);
        })
        .harness();

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
        parse: testSerializer.parse,
        createRunContext: () => ({ dataSources }),
      })
        .onModify((ctx, oldEntity, newEntity) => {
          ctx.dataSources.doSomething(oldEntity, newEntity);
        })
        .harness();

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
        parse: testSerializer.parse,
        createRunContext: () => ({ dataSources }),
      })
        .onRemove((ctx, entity) => {
          ctx.dataSources.doSomething(entity);
        })
        .harness();

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
        parse: testSerializer.parse,
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
        .harness();

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
        parse: testSerializer.parse,
        createRunContext: () => ({ dataSources }),
      })
        .onInsert((ctx) => {
          ctx.logger.info(testValue);
          ctx.dataSources.doSomething((ctx as any).testValue);
        })
        .harness({
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

  test('generates a correlation id', async () => {
    const { sendEvent } = new DynamoStreamHandler({
      logger,
      parse: testSerializer.parse,
      createRunContext: (ctx) => {
        expect(typeof ctx.correlationId === 'string').toBe(true);
        return {};
      },
    }).harness();

    await sendEvent({ records: [] });

    expect(logger.child).toHaveBeenCalledWith(
      expect.objectContaining({ correlationId: expect.any(String) }),
    );

    expect.assertions(2);
  });

  describe('error scenarios', () => {
    const lambda = new DynamoStreamHandler({
      logger,
      parse: testSerializer.parse,
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
            {
              eventName: 'MODIFY',
              dynamodb: { OldImage: { id: { S: 'test-id' } } },
            },
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
            {
              eventName: 'MODIFY',
              dynamodb: { NewImage: { id: { S: 'test-id' } } },
            },
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

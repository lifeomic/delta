import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { DynamoStreamHandler } from './dynamo-streams';
import { marshall } from '@aws-sdk/util-dynamodb';
import { z } from 'zod';

const TestSchema = z.object({
  id: z.string(),
  name: z.string().optional(),
  otherValue: z.string().optional(),
  otherMap: z
    .object({
      name: z.string(),
      age: z.number(),
    })
    .optional(),
});

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

  test('throws error if there are unprocessed records', async () => {
    const handler = new DynamoStreamHandler({
      logger,
      parse: testSerializer.parse,
      createRunContext: () => ({ logger, dataSources }),
    })
      .onInsert((ctx, entity) => {
        if (entity.id === 'new-insert-1') {
          ctx.dataSources.doSomething(entity);
        } else {
          throw new Error(`Failed to process ${entity.id}`);
        }
      })
      .lambda();

    await expect(
      handler(
        {
          Records: [
            {
              eventName: 'INSERT',
              dynamodb: { NewImage: { id: { S: 'new-insert-1' } } },
            },
            {
              eventName: 'INSERT',
              dynamodb: { NewImage: { id: { S: 'new-insert-2' } } },
            },
            {
              eventName: 'INSERT',
              dynamodb: { NewImage: { id: { S: 'new-insert-3' } } },
            },
          ],
        },
        {} as any,
        {} as any,
      ),
    ).rejects.toThrowError(
      new AggregateError([
        new Error('Failed to process new-insert-2'),
        new Error('Failed to process new-insert-3'),
      ]),
    );

    expect(dataSources.doSomething).toHaveBeenCalledTimes(1);
    expect(dataSources.doSomething).toHaveBeenCalledWith({
      id: 'new-insert-1',
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
        ctx.dataSources.doSomething('insert 1', entity);
      })
      .onInsert((ctx, entity) => {
        ctx.dataSources.doSomething('insert 2', entity);
      })
      .onModify((ctx, oldEntity, newEntity) => {
        ctx.dataSources.doSomething('modify', oldEntity, newEntity);
      })
      .onRemove((ctx, entity) => {
        ctx.dataSources.doSomething('remove', entity);
      })
      .lambda();

    await lambda(
      {
        Records: [
          {
            eventName: 'INSERT',
            dynamodb: {
              NewImage: marshall({ id: 'test-id-1' }) as any,
            },
          },
          {
            eventName: 'MODIFY',
            dynamodb: {
              OldImage: marshall({ id: 'test-id-1' }) as any,
              NewImage: marshall({ id: 'test-id-1', name: 'new name' }) as any,
            },
          },
          {
            eventName: 'REMOVE',
            dynamodb: {
              OldImage: marshall({ id: 'test-id-2' }) as any,
            },
          },
          // A second remove event to test multiple events through a single action
          {
            eventName: 'REMOVE',
            dynamodb: {
              OldImage: marshall({ id: 'test-id-0' }),
            },
          },
        ],
      },
      {} as any,
      {} as any,
    );

    expect(dataSources.doSomething).toHaveBeenCalledTimes(5);
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(1, 'insert 1', {
      id: 'test-id-1',
    });
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      2,
      'modify',
      { id: 'test-id-1' },
      { id: 'test-id-1', name: 'new name' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(3, 'remove', {
      id: 'test-id-2',
    });
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(4, 'remove', {
      id: 'test-id-0',
    });
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(5, 'insert 2', {
      id: 'test-id-1',
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
          ctx.dataSources.doSomething('insert 1', entity);
        })
        .onInsert((ctx, entity) => {
          ctx.dataSources.doSomething('insert 2', entity);
        })
        .onModify((ctx, oldEntity, newEntity) => {
          ctx.dataSources.doSomething('modify', oldEntity, newEntity);
        })
        .onRemove((ctx, entity) => {
          ctx.dataSources.doSomething('remove', entity);
        })
        .harness();

      await sendEvent({
        records: [
          { type: 'insert', entity: { id: 'test-id-1' } },
          {
            type: 'modify',
            oldEntity: { id: 'test-id-1' },
            newEntity: { id: 'test-id-1', name: 'new name' },
          },
          { type: 'remove', entity: { id: 'test-id-2' } },
          // A second remove event to test multiple events through a single action
          {
            type: 'remove',
            entity: { id: 'test-id-0' },
          },
        ],
      });

      expect(dataSources.doSomething).toHaveBeenCalledTimes(5);

      expect(dataSources.doSomething).toHaveBeenNthCalledWith(1, 'insert 1', {
        id: 'test-id-1',
      });
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(
        2,
        'modify',
        { id: 'test-id-1' },
        { id: 'test-id-1', name: 'new name' },
      );
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(3, 'remove', {
        id: 'test-id-2',
      });
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(4, 'remove', {
        id: 'test-id-0',
      });
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(5, 'insert 2', {
        id: 'test-id-1',
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
        'No NewImage was defined for a MODIFY event',
      );
      expect(logger.child).toHaveBeenCalledWith({
        record: {
          eventName: 'MODIFY',
          dynamodb: { OldImage: { id: { S: 'test-id' } } },
        },
      });
    });

    test('MODIFY with no OldImage', async () => {
      await lambda(
        {
          Records: [
            {
              eventName: 'MODIFY',
              dynamodb: {
                NewImage: { id: { S: 'test-id' } },
              },
            },
          ],
        },
        {} as any,
        {} as any,
      );

      expect(logger.error).toHaveBeenCalledWith(
        'No OldImage was defined for a MODIFY event',
      );
      expect(logger.child).toHaveBeenCalledWith({
        record: {
          eventName: 'MODIFY',
          dynamodb: {
            NewImage: { id: { S: 'test-id' } },
          },
        },
      });
    });

    test('REMOVE with no OldImage', async () => {
      await lambda(
        { Records: [{ eventName: 'REMOVE', dynamodb: {} }] },
        {} as any,
        {} as any,
      );

      expect(logger.error).toHaveBeenCalledWith(
        'No OldImage was defined for a REMOVE event',
      );
    });
  });

  describe('parallelization', () => {
    const wait = (ms: number) =>
      new Promise((resolve) => setTimeout(resolve, ms));

    test('processes events in parallel, while maintaining ordering by Keys', async () => {
      const mocks = {
        insert: { started: jest.fn(), finished: jest.fn() },
        modify: { started: jest.fn(), finished: jest.fn() },
        remove: { started: jest.fn(), finished: jest.fn() },
      };

      const handler = new DynamoStreamHandler({
        logger,
        parse: testSerializer.parse,
        createRunContext: () => ({}),
      })
        .onInsert(async (ctx, item) => {
          mocks.insert.started(Date.now(), item);
          await wait(100);
          mocks.insert.finished(Date.now(), item);
        })
        .onModify(async (ctx, oldItem, newItem) => {
          mocks.modify.started(Date.now(), oldItem, newItem);
          await wait(100);
          mocks.modify.finished(Date.now(), oldItem, newItem);
        })
        .onRemove(async (ctx, item) => {
          mocks.remove.started(Date.now(), item);
          await wait(100);
          mocks.remove.finished(Date.now(), item);
        })
        .lambda();

      const start = Date.now();

      await handler(
        {
          Records: [
            {
              eventName: 'INSERT',
              dynamodb: {
                Keys: marshall({ id: 'test-id-1', name: 'test-name-1' }) as any,
                NewImage: marshall({
                  id: 'test-id-1',
                  name: 'test-name-1',
                  otherValue: 'test-value-1',
                }) as any,
              },
            },
            {
              eventName: 'MODIFY',
              dynamodb: {
                Keys: marshall({ id: 'test-id-1', name: 'test-name-1' }) as any,
                NewImage: marshall({
                  id: 'test-id-1',
                  name: 'test-name-1',
                  otherValue: 'test-value-1',
                }) as any,
                OldImage: marshall({
                  id: 'test-id-1',
                  name: 'test-name-1',
                  otherValue: 'test-value-2',
                }) as any,
              },
            },
            {
              eventName: 'MODIFY',
              dynamodb: {
                Keys: marshall({ id: 'test-id-2', name: 'test-name-1' }) as any,
                NewImage: marshall({
                  id: 'test-id-2',
                  name: 'test-name-1',
                  otherValue: 'test-value-1',
                }) as any,
                OldImage: marshall({
                  id: 'test-id-2',
                  name: 'test-name-1',
                  otherValue: 'test-value-2',
                }) as any,
              },
            },
            {
              eventName: 'REMOVE',
              dynamodb: {
                Keys: marshall({ id: 'test-id-1', name: 'test-name-1' }) as any,
                OldImage: marshall({
                  id: 'test-id-1',
                  name: 'test-name-1',
                  otherValue: 'test-value-2',
                }) as any,
              },
            },
          ],
        },
        {} as any,
        null as any,
      );

      const end = Date.now();

      // This assertion confirms that the group doesn't process in less than 300ms.
      // If it did, then some of the ordered events would be parallelized, which would be bad.
      expect(end - start).toBeGreaterThanOrEqual(300);

      // This assertions confirms that there is some parallelization happening.
      expect(end - start).toBeLessThan(400);

      // Now, let's also explicitly assert that the events were processed in order.
      const insertFinishedTime = mocks.insert.finished.mock.calls.find(
        ([, { id }]) => id === 'test-id-1',
      )[0];
      const modifyStartedTime = mocks.modify.started.mock.calls.find(
        ([, { id }]) => id === 'test-id-1',
      )[0];

      expect(modifyStartedTime).toBeGreaterThanOrEqual(insertFinishedTime);

      const modifyFinishedTime = mocks.modify.finished.mock.calls.find(
        ([, { id }]) => id === 'test-id-1',
      )[0];

      const removeStartedTime = mocks.remove.started.mock.calls.find(
        ([, { id }]) => id === 'test-id-1',
      )[0];

      expect(removeStartedTime).toBeGreaterThanOrEqual(modifyFinishedTime);
    });

    test('concurrency can be set to 1, which will result in serial processing', async () => {
      const processMock = jest.fn();

      const handler = new DynamoStreamHandler({
        logger,
        parse: testSerializer.parse,
        createRunContext: () => ({}),
        concurrency: 1,
      })
        .onInsert(async (ctx, item) => {
          processMock('insert', item);
          await wait(100);
        })
        .onModify(async (ctx, oldItem, newItem) => {
          processMock('modify', oldItem, newItem);
          await wait(100);
        })
        .onRemove(async (ctx, item) => {
          processMock('remove', item);
          await wait(100);
        })
        .lambda();

      const start = Date.now();

      await handler(
        {
          Records: [
            {
              eventName: 'INSERT',
              dynamodb: {
                Keys: marshall({ id: 'test-id-1', name: 'test-name-1' }) as any,
                NewImage: marshall({
                  id: 'test-id-1',
                  name: 'test-name-1',
                  otherValue: 'test-value-1',
                }) as any,
              },
            },
            {
              eventName: 'MODIFY',
              dynamodb: {
                Keys: marshall({ id: 'test-id-1', name: 'test-name-1' }) as any,
                OldImage: marshall({
                  id: 'test-id-1',
                  name: 'test-name-1',
                  otherValue: 'test-value-1',
                }) as any,
                NewImage: marshall({
                  id: 'test-id-1',
                  name: 'test-name-1',
                  otherValue: 'test-value-2',
                }) as any,
              },
            },
            {
              eventName: 'MODIFY',
              dynamodb: {
                Keys: marshall({ id: 'test-id-2', name: 'test-name-1' }) as any,
                OldImage: marshall({
                  id: 'test-id-2',
                  name: 'test-name-1',
                  otherValue: 'test-value-1',
                }) as any,
                NewImage: marshall({
                  id: 'test-id-2',
                  name: 'test-name-1',
                  otherValue: 'test-value-2',
                }) as any,
              },
            },
            {
              eventName: 'REMOVE',
              dynamodb: {
                Keys: marshall({ id: 'test-id-1', name: 'test-name-1' }) as any,
                OldImage: marshall({
                  id: 'test-id-1',
                  name: 'test-name-1',
                  otherValue: 'test-value-2',
                }) as any,
              },
            },
          ],
        },
        {} as any,
        null as any,
      );

      const end = Date.now();

      // This assertions provides some reasonable confirmation that parallelization is not happening.
      expect(end - start).toBeGreaterThanOrEqual(400);
    });
  });

  describe('logging obfuscation', () => {
    test('MODIFY with no OldImage and obfuscated secret', async () => {
      const lambda = new DynamoStreamHandler({
        logger,
        loggerObfuscateImageKeys: ['otherValue'],
        parse: testSerializer.parse,
        createRunContext: () => ({ logger, dataSources }),
      }).lambda();

      await lambda(
        {
          Records: [
            {
              eventName: 'MODIFY',
              dynamodb: {
                NewImage: {
                  id: { S: 'test-id' },
                  otherValue: { S: 'secret data' },
                },
              },
            },
          ],
        },
        {} as any,
        {} as any,
      );

      expect(logger.error).toHaveBeenCalledWith(
        'No OldImage was defined for a MODIFY event',
      );
      expect(logger.child).toHaveBeenCalledWith({
        record: {
          eventName: 'MODIFY',
          dynamodb: {
            NewImage: { id: { S: 'test-id' }, otherValue: { S: 'obfuscated' } },
          },
        },
      });
    });

    test('event not modified during obfuscation', async () => {
      const lambda = new DynamoStreamHandler({
        logger,
        loggerObfuscateImageKeys: ['otherMap', 'otherValue'],
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
                OldImage: {
                  id: { S: 'old-modify' },
                  otherValue: { S: 'secret data' },
                },
                NewImage: {
                  id: { S: 'new-modify' },
                  otherMap: {
                    M: { name: { S: 'secret data' }, age: { N: '35' } },
                  },
                },
              },
            },
          ],
        },
        {} as any,
        {} as any,
      );

      expect(dataSources.doSomething).toHaveBeenCalledTimes(1);
      expect(dataSources.doSomething).toHaveBeenCalledWith(
        { id: 'old-modify', otherValue: 'secret data' },
        { id: 'new-modify', otherMap: { name: 'secret data', age: 35 } },
      );
    });
  });
});

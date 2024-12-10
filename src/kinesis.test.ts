import { v4 as uuid } from 'uuid';

import { KinesisEventHandler } from './kinesis';
import { useMockLogger } from './jest-utils';

const logger = useMockLogger();

const testSerializer = {
  parseEvent: (msg: string) => JSON.parse(msg),
  stringifyEvent: (msg: any) => JSON.stringify(msg),
  toKinesisNativeRecord: (msg: Record<string, unknown>) =>
    Buffer.from(JSON.stringify(msg)).toString('base64'),
};

describe('KinesisEventHandler', () => {
  test('responds to HTTP health checks', async () => {
    const lambda = new KinesisEventHandler({
      logger,
      parseEvent: testSerializer.parseEvent,
      createRunContext: () => ({}),
    }).lambda();

    const result = await lambda({ httpMethod: 'GET' } as any, {} as any);

    expect(result).toStrictEqual({
      statusCode: 200,
      body: '{"healthy":true}',
    });
  });

  test('responds to healthCheck events', async () => {
    const lambda = new KinesisEventHandler({
      logger,
      parseEvent: testSerializer.parseEvent,
      createRunContext: () => ({}),
    }).lambda();

    const result = await lambda({ healthCheck: true } as any, {} as any);

    expect(result).toStrictEqual({
      healthy: true,
    });
  });

  test('generates a correlation id', async () => {
    const lambda = new KinesisEventHandler({
      logger,
      parseEvent: testSerializer.parseEvent,
      createRunContext: (ctx) => {
        expect(typeof ctx.correlationId === 'string').toBe(true);
        return {};
      },
    }).lambda();

    await lambda(
      {
        Records: [
          {
            kinesis: {
              partitionKey: uuid(),
              data: testSerializer.toKinesisNativeRecord({
                data: 'test-event-1',
              }),
            },
          },
        ],
      } as any,
      {} as any,
    );

    expect(logger.child).toHaveBeenCalledWith(
      expect.objectContaining({ correlationId: expect.any(String) }),
    );

    expect.assertions(2);
  });

  test('sending events with context', async () => {
    const dataSources = {
      doSomething: jest.fn(),
    };

    const lambda = new KinesisEventHandler({
      logger,
      parseEvent: testSerializer.parseEvent,
      createRunContext: () => dataSources,
      concurrency: 1,
    })
      .onEvent((ctx, event) => {
        ctx.doSomething('first-handler', event);
      })
      .onEvent((ctx, event) => {
        ctx.doSomething('second-handler', event);
      })
      .lambda();

    await lambda(
      {
        Records: [
          {
            kinesis: {
              partitionKey: uuid(),
              data: testSerializer.toKinesisNativeRecord({
                data: 'test-event-1',
              }),
            },
          },
          {
            kinesis: {
              partitionKey: uuid(),
              data: testSerializer.toKinesisNativeRecord({
                data: 'test-event-2',
              }),
            },
          },
          {
            kinesis: {
              partitionKey: uuid(),
              data: testSerializer.toKinesisNativeRecord({
                data: 'test-event-3',
              }),
            },
          },
          {
            kinesis: {
              partitionKey: uuid(),
              data: testSerializer.toKinesisNativeRecord({
                data: 'test-event-4',
              }),
            },
          },
        ],
      } as any,
      {} as any,
    );

    // Expect 8 calls. 2 event handlers * 4 events.
    expect(dataSources.doSomething).toHaveBeenCalledTimes(8);

    // Now, confirm the ordering.
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      1,
      'first-handler',
      { data: 'test-event-1' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      2,
      'second-handler',
      { data: 'test-event-1' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      3,
      'first-handler',
      { data: 'test-event-2' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      4,
      'second-handler',
      { data: 'test-event-2' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      5,
      'first-handler',
      { data: 'test-event-3' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      6,
      'second-handler',
      { data: 'test-event-3' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      7,
      'first-handler',
      { data: 'test-event-4' },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      8,
      'second-handler',
      { data: 'test-event-4' },
    );
  });

  describe('harness', () => {
    test('sends events correctly', async () => {
      const dataSources = {
        doSomething: jest.fn(),
      };

      const { sendEvent } = new KinesisEventHandler({
        logger,
        parseEvent: testSerializer.parseEvent,
        createRunContext: () => dataSources,
      })
        .onEvent((ctx, msg) => {
          ctx.doSomething(msg);
        })
        .harness({
          stringifyEvent: testSerializer.stringifyEvent,
        });

      await sendEvent({
        events: [{ data: 'test-event-1' }, { data: 'test-event-2' }],
      });

      expect(dataSources.doSomething).toHaveBeenCalledTimes(2);
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(1, {
        data: 'test-event-1',
      });
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(2, {
        data: 'test-event-2',
      });
    });

    test('throws aggregate error if there are unprocessed records', async () => {
      expect.assertions(2);

      const handler = new KinesisEventHandler({
        logger,
        parseEvent: testSerializer.parseEvent,
        createRunContext: () => ({}),
      })
        .onEvent((ctx, message) => {
          if (message.data !== 'test-event-1') {
            throw new Error(`Failed to process ${message.data}`);
          }
        })
        .lambda();

      try {
        await handler(
          {
            Records: [
              {
                kinesis: {
                  partitionKey: uuid(),
                  data: testSerializer.toKinesisNativeRecord({
                    data: 'test-event-1',
                  }),
                },
              },
              {
                kinesis: {
                  partitionKey: uuid(),
                  data: testSerializer.toKinesisNativeRecord({
                    data: 'test-event-2',
                  }),
                },
              },
              {
                kinesis: {
                  partitionKey: uuid(),
                  data: testSerializer.toKinesisNativeRecord({
                    data: 'test-event-3',
                  }),
                },
              },
            ] as any,
          },
          {} as any,
        );
      } catch (e: any) {
        expect(e.message).toContain('Failed to process test-event-2');
        expect(e.message).toContain('Failed to process test-event-3');
      }
    });

    test('allows overriding context and logger', async () => {
      const testValue = uuid();

      const overrideLogger = {
        info: jest.fn(),
        error: jest.fn(),
        child: jest.fn(),
      };
      overrideLogger.child.mockImplementation(() => overrideLogger);

      const dataSources = {
        doSomething: jest.fn(),
      };

      const { sendEvent } = new KinesisEventHandler({
        logger,
        parseEvent: testSerializer.parseEvent,
        createRunContext: () => ({ dataSources }),
      })
        .onEvent((ctx) => {
          ctx.logger.info((ctx as any).testValue);
          ctx.dataSources.doSomething((ctx as any).testValue);
        })
        .harness({
          stringifyEvent: testSerializer.stringifyEvent,
          logger: overrideLogger as any,
          createRunContext: () => ({ dataSources, testValue }),
        });

      await sendEvent({ events: [{}] });

      expect(logger.info).not.toHaveBeenCalled();
      expect(overrideLogger.info).toHaveBeenCalledWith(testValue);

      expect(dataSources.doSomething).toHaveBeenCalledWith(testValue);
    });
  });

  const wait = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));

  describe('parallelization', () => {
    test('processes events in parallel', async () => {
      const handler = new KinesisEventHandler({
        logger,
        parseEvent: testSerializer.parseEvent,
        createRunContext: () => ({}),
      })
        .onEvent(async () => {
          // We'll wait 100ms per event, then use that to make timing
          // assertions below.
          await wait(100);
        })
        .lambda();

      const start = Date.now();

      await handler(
        {
          Records: [
            {
              kinesis: {
                partitionKey: uuid(),
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-1',
                }),
              },
            },
            {
              kinesis: {
                partitionKey: uuid(),
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-2',
                }),
              },
            },
            {
              kinesis: {
                partitionKey: uuid(),
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-3',
                }),
              },
            },
          ] as any,
        },
        {} as any,
      );

      const end = Date.now();

      // This assertion confirms there is parallel processing.
      expect(end - start).toBeLessThan(200);
    });

    test('maintains ordering by partitionKey', async () => {
      const eventStarted = jest.fn();
      const eventFinished = jest.fn();
      const handler = new KinesisEventHandler({
        logger,
        parseEvent: testSerializer.parseEvent,
        createRunContext: () => ({}),
      })
        .onEvent(async (ctx, msg) => {
          eventStarted(msg, Date.now());
          // We'll wait 100ms per event, then use that to make timing
          // assertions below.
          await wait(100);
          eventFinished(msg, Date.now());
        })
        .lambda();

      const start = Date.now();

      await handler(
        {
          Records: [
            {
              kinesis: {
                partitionKey: 'group-id',
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-1',
                }),
              },
            },
            {
              kinesis: {
                partitionKey: uuid(),
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-2',
                }),
              },
            },
            {
              kinesis: {
                partitionKey: 'group-id-2',
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-other-1',
                }),
              },
            },
            {
              kinesis: {
                partitionKey: 'group-id',
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-3',
                }),
              },
            },
            {
              kinesis: {
                partitionKey: 'group-id-2',
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-other-2',
                }),
              },
            },
            {
              kinesis: {
                partitionKey: uuid(),
                data: testSerializer.toKinesisNativeRecord({
                  data: 'test-event-4',
                }),
              },
            },
          ] as any,
        },
        {} as any,
      );

      const end = Date.now();

      // This assertion confirms that the group doesn't process in less than 200ms.
      // If it did, then the events would be fully parallelized, which would be bad.
      expect(end - start).toBeGreaterThanOrEqual(200);

      // This assertion confirms that there is at least some parallelization happening.
      expect(end - start).toBeLessThanOrEqual(450);

      // Now, let's also assert that event 3 was processed _after_ the end of event 1.
      const event1FinishedTime = eventFinished.mock.calls.find(
        (call) => call[0].data === 'test-event-1',
      )[1];

      const event3StartedTime = eventStarted.mock.calls.find(
        (call) => call[0].data === 'test-event-3',
      )[1];

      expect(event3StartedTime).toBeGreaterThanOrEqual(event1FinishedTime);

      const eventOther1FinishedTime = eventFinished.mock.calls.find(
        (call) => call[0].data === 'test-event-other-1',
      )[1];

      const eventOther2StartedTime = eventStarted.mock.calls.find(
        (call) => call[0].data === 'test-event-other-2',
      )[1];

      expect(eventOther2StartedTime).toBeGreaterThanOrEqual(
        eventOther1FinishedTime,
      );
    });
  });

  describe('partial batch responses', () => {
    test('returns partial batch response when setting is enabled', async () => {
      const execute = new KinesisEventHandler({
        logger,
        parseEvent: testSerializer.parseEvent,
        createRunContext: () => ({}),
        usePartialBatchResponses: true,
        // Make sure partial batch responses are returned in order even
        // when using concurrency.
        concurrency: 2,
      })
        .onEvent((ctx, event) => {
          // let 2 pass
          if (event.id.includes('2')) {
            return;
          }
          throw new Error('Failed to process event: ' + event.id);
        })
        .lambda();

      const result = await execute(
        {
          Records: [
            {
              // @ts-expect-error We're specifying a subset of props here.
              kinesis: {
                sequenceNumber: 'one',
                partitionKey: uuid(),
                data: Buffer.from(JSON.stringify({ id: 'event-1' })).toString(
                  'base64',
                ),
              },
            },
            {
              // @ts-expect-error We're specifying a subset of props here.
              kinesis: {
                sequenceNumber: 'two',
                partitionKey: uuid(),
                data: Buffer.from(JSON.stringify({ id: 'event-2' })).toString(
                  'base64',
                ),
              },
            },
            {
              // @ts-expect-error We're specifying a subset of props here.
              kinesis: {
                sequenceNumber: 'three',
                partitionKey: uuid(),
                data: Buffer.from(JSON.stringify({ id: 'event-3' })).toString(
                  'base64',
                ),
              },
            },
          ],
        },
        { awsRequestId: uuid() },
      );

      // Expect that first event is logged
      expect(logger.error).toHaveBeenCalledWith(
        expect.objectContaining({
          itemIdentifier: 'one',
          failedRecord: {
            kinesis: {
              sequenceNumber: 'one',
              partitionKey: expect.any(String),
              data: Buffer.from(JSON.stringify({ id: 'event-1' })).toString(
                'base64',
              ),
            },
          },
          err: expect.objectContaining({
            message: 'Failed to process event: event-1',
          }),
        }),
        'Failed to process record',
      );

      // Expect that third event is logged
      expect(logger.error).toHaveBeenCalledWith(
        expect.objectContaining({
          itemIdentifier: 'three',
          failedRecord: {
            kinesis: {
              sequenceNumber: 'three',
              partitionKey: expect.any(String),
              data: Buffer.from(JSON.stringify({ id: 'event-3' })).toString(
                'base64',
              ),
            },
          },
          err: expect.objectContaining({
            message: 'Failed to process event: event-3',
          }),
        }),
        'Failed to process record',
      );

      const batchItemFailures = [
        { itemIdentifier: 'one' },
        { itemIdentifier: 'three' },
      ];

      expect(result).toEqual({
        batchItemFailures,
      });
      expect(logger.info).not.toHaveBeenCalledWith(
        'Successfully processed all messages',
      );
      expect(logger.info).toHaveBeenCalledWith(
        { batchItemFailures },
        'Completing with partial batch response',
      );
    });
  });

  test('throws when encountering an unparseable message', async () => {
    const lambda = new KinesisEventHandler({
      logger,
      parseEvent: testSerializer.parseEvent,
      createRunContext: () => ({}),
    }).lambda();

    await expect(
      lambda(
        {
          Records: [
            {
              kinesis: {
                partitionKey: uuid(),
                data: Buffer.from('not-a-json-string', 'utf-8').toString(
                  'base64',
                ),
              },
            },
          ] as any,
        },
        {} as any,
      ),
    ).rejects.toThrow('Unexpected token o in JSON at position 1');

    expect(logger.error).toHaveBeenCalledWith(
      expect.objectContaining({
        err: expect.objectContaining({
          message: 'Unexpected token o in JSON at position 1',
        }),
      }),
      'Failed to parse event',
    );
  });

  test('respects ignoreUnparseableEvents', async () => {
    const processor = jest.fn();
    const lambda = new KinesisEventHandler({
      logger,
      parseEvent: testSerializer.parseEvent,
      createRunContext: () => ({}),
      ignoreUnparseableEvents: true,
    })
      .onEvent((ctx, event) => processor(event))
      .lambda();

    await lambda(
      {
        Records: [
          {
            kinesis: {
              partitionKey: uuid(),
              data: Buffer.from('not-a-json-string', 'utf-8').toString(
                'base64',
              ),
            },
          },
          {
            kinesis: {
              partitionKey: uuid(),
              data: Buffer.from(
                JSON.stringify({ message: 'test-message' }),
                'utf-8',
              ).toString('base64'),
            },
          },
        ] as any,
      },
      {} as any,
    );

    expect(logger.error).toHaveBeenCalledWith(
      expect.objectContaining({
        err: expect.objectContaining({
          message: 'Unexpected token o in JSON at position 1',
        }),
      }),
      'Failed to parse event',
    );

    expect(logger.warn).toHaveBeenCalledWith(
      'ignoreUnparseableEvents is set to true. Ignoring message.',
    );

    expect(processor).toHaveBeenCalledTimes(1);
    expect(processor).toHaveBeenCalledWith({ message: 'test-message' });
  });
});

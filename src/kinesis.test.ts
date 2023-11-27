import { v4 as uuid } from 'uuid';
import { LoggerInterface } from '@lifeomic/logging';
import { KinesisEventHandler } from './kinesis';

const logger: jest.Mocked<LoggerInterface> = {
  info: jest.fn(),
  child: jest.fn(),
} as any;

beforeEach(() => {
  logger.info.mockReset();
  logger.child.mockReset();
  logger.child.mockImplementation(() => logger);
});

const testSerializer = {
  parseEvent: (msg: string) => JSON.parse(msg),
  stringifyEvent: (msg: any) => JSON.stringify(msg),
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
              data: JSON.stringify({ data: 'test-event-1' }),
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
              data: JSON.stringify({ data: 'test-event-1' }),
            },
          },
          {
            kinesis: {
              partitionKey: uuid(),
              data: JSON.stringify({ data: 'test-event-2' }),
            },
          },
          {
            kinesis: {
              partitionKey: uuid(),
              data: JSON.stringify({ data: 'test-event-3' }),
            },
          },
          {
            kinesis: {
              partitionKey: uuid(),
              data: JSON.stringify({ data: 'test-event-4' }),
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

    test('throws error if there are unprocessed records', async () => {
      let alreadyFailed = false;

      const handler = new KinesisEventHandler({
        logger,
        parseEvent: testSerializer.parseEvent,
        createRunContext: () => ({}),
      })
        .onEvent(() => {
          if (!alreadyFailed) {
            alreadyFailed = true;
            throw new Error('poison record!');
          }
        })
        .lambda();

      await expect(
        handler(
          {
            Records: [
              {
                kinesis: {
                  partitionKey: uuid(),
                  data: JSON.stringify({ data: 'test-event-1' }),
                },
              },
              {
                kinesis: {
                  partitionKey: uuid(),
                  data: JSON.stringify({ data: 'test-event-2' }),
                },
              },
              {
                kinesis: {
                  partitionKey: uuid(),
                  data: JSON.stringify({ data: 'test-event-3' }),
                },
              },
            ] as any,
          },
          {} as any,
        ),
      ).rejects.toThrow('Failed to process all Kinesis records');
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
                data: JSON.stringify({ data: 'test-event-1' }),
              },
            },
            {
              kinesis: {
                partitionKey: uuid(),
                data: JSON.stringify({ data: 'test-event-2' }),
              },
            },
            {
              kinesis: {
                partitionKey: uuid(),
                data: JSON.stringify({ data: 'test-event-3' }),
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
                data: JSON.stringify({ data: 'test-event-1' }),
              },
            },
            {
              kinesis: {
                partitionKey: uuid(),
                data: JSON.stringify({ data: 'test-event-2' }),
              },
            },
            {
              kinesis: {
                partitionKey: 'group-id-2',
                data: JSON.stringify({ data: 'test-event-other-1' }),
              },
            },
            {
              kinesis: {
                partitionKey: 'group-id',
                data: JSON.stringify({ data: 'test-event-3' }),
              },
            },
            {
              kinesis: {
                partitionKey: 'group-id-2',
                data: JSON.stringify({ data: 'test-event-other-2' }),
              },
            },
            {
              kinesis: {
                partitionKey: uuid(),
                data: JSON.stringify({ data: 'test-event-4' }),
              },
            },
          ] as any,
        },
        {} as any,
      );

      const end = Date.now();

      // This assertion confirms that the group doesn't process in less than 200ms.
      // If it did, then the events would be fully parallelized, which would be bad.
      expect(end - start).toBeGreaterThan(200);

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
});

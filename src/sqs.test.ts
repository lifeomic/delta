import { v4 as uuid } from 'uuid';
import { LoggerInterface } from '@lifeomic/logging';
import { SQSMessageHandler } from './sqs';

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
  parseMessage: (msg: string) => JSON.parse(msg),
  stringifyMessage: (msg: any) => JSON.stringify(msg),
};

describe('SQSMessageHandler', () => {
  test('responds to HTTP health checks', async () => {
    const lambda = new SQSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: () => ({}),
    }).lambda();

    const result = await lambda({ httpMethod: 'GET' } as any, {} as any);

    expect(result).toStrictEqual({
      statusCode: 200,
      body: '{"healthy":true}',
    });
  });

  test('responds to healthCheck events', async () => {
    const lambda = new SQSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: () => ({}),
    }).lambda();

    const result = await lambda({ healthCheck: true } as any, {} as any);

    expect(result).toStrictEqual({
      healthy: true,
    });
  });

  test('generates a correlation id', async () => {
    const lambda = new SQSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: (ctx) => {
        expect(typeof ctx.correlationId === 'string').toBe(true);
        return {};
      },
    }).lambda();

    await lambda(
      {
        Records: [
          { attributes: {}, body: JSON.stringify({ data: 'test-event-1' }) },
        ],
      } as any,
      {} as any,
    );

    expect(logger.child).toHaveBeenCalledWith(
      expect.objectContaining({ correlationId: expect.any(String) }),
    );

    expect.assertions(2);
  });

  test('sending messages with context', async () => {
    const dataSources = {
      doSomething: jest.fn(),
    };

    const lambda = new SQSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: () => dataSources,
      concurrency: 1,
    })
      .onMessage((ctx, message) => {
        ctx.doSomething('first-handler', message);
      })
      .onMessage((ctx, message) => {
        ctx.doSomething('second-handler', message);
      })
      .lambda();

    await lambda(
      {
        Records: [
          { attributes: {}, body: JSON.stringify({ data: 'test-event-1' }) },
          { attributes: {}, body: JSON.stringify({ data: 'test-event-2' }) },
          { attributes: {}, body: JSON.stringify({ data: 'test-event-3' }) },
          { attributes: {}, body: JSON.stringify({ data: 'test-event-4' }) },
        ],
      } as any,
      {} as any,
    );

    // Expect 8 calls. 2 message handlers * 4 events.
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

      const { sendEvent } = new SQSMessageHandler({
        logger,
        parseMessage: testSerializer.parseMessage,
        createRunContext: () => dataSources,
      })
        .onMessage((ctx, msg) => {
          ctx.doSomething(msg);
        })
        .harness({
          stringifyMessage: testSerializer.stringifyMessage,
        });

      await sendEvent({
        messages: [{ data: 'test-event-1' }, { data: 'test-event-2' }],
      });

      expect(dataSources.doSomething).toHaveBeenCalledTimes(2);
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(1, {
        data: 'test-event-1',
      });
      expect(dataSources.doSomething).toHaveBeenNthCalledWith(2, {
        data: 'test-event-2',
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

      const dataSources = {
        doSomething: jest.fn(),
      };

      const { sendEvent } = new SQSMessageHandler({
        logger,
        parseMessage: testSerializer.parseMessage,
        createRunContext: () => ({ dataSources }),
      })
        .onMessage((ctx) => {
          ctx.logger.info((ctx as any).testValue);
          ctx.dataSources.doSomething((ctx as any).testValue);
        })
        .harness({
          stringifyMessage: testSerializer.stringifyMessage,
          logger: overrideLogger as any,
          createRunContext: () => ({ dataSources, testValue }),
        });

      await sendEvent({ messages: [{}] });

      expect(logger.info).not.toHaveBeenCalled();
      expect(overrideLogger.info).toHaveBeenCalledWith(testValue);

      expect(dataSources.doSomething).toHaveBeenCalledWith(testValue);
    });
  });

  const wait = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));

  describe('parallelization', () => {
    test('processes events in parallel', async () => {
      const handler = new SQSMessageHandler({
        logger,
        parseMessage: testSerializer.parseMessage,
        createRunContext: () => ({}),
      })
        .onMessage(async () => {
          // We'll wait 100ms per event, then use that to make timing
          // assertions below.
          await wait(100);
        })
        .lambda();

      const start = Date.now();

      await handler(
        {
          Records: [
            { attributes: {}, body: JSON.stringify({ data: 'test-event-1' }) },
            { attributes: {}, body: JSON.stringify({ data: 'test-event-2' }) },
            { attributes: {}, body: JSON.stringify({ data: 'test-event-3' }) },
          ] as any,
        },
        {} as any,
      );

      const end = Date.now();

      // This assertion confirms there is parallel processing.
      expect(end - start).toBeLessThan(200);
    });

    test('maintains ordering by MessageGroupId', async () => {
      const messageStarted = jest.fn();
      const messageFinished = jest.fn();
      const handler = new SQSMessageHandler({
        logger,
        parseMessage: testSerializer.parseMessage,
        createRunContext: () => ({}),
      })
        .onMessage(async (ctx, msg) => {
          messageStarted(msg, Date.now());
          // We'll wait 100ms per event, then use that to make timing
          // assertions below.
          await wait(100);
          messageFinished(msg, Date.now());
        })
        .lambda();

      const start = Date.now();

      await handler(
        {
          Records: [
            {
              attributes: { MessageGroupId: 'group-id' },
              body: JSON.stringify({ data: 'test-event-1' }),
            },
            { attributes: {}, body: JSON.stringify({ data: 'test-event-2' }) },
            {
              attributes: { MessageGroupId: 'group-id-2' },
              body: JSON.stringify({ data: 'test-event-other-1' }),
            },
            {
              attributes: { MessageGroupId: 'group-id' },
              body: JSON.stringify({ data: 'test-event-3' }),
            },
            {
              attributes: { MessageGroupId: 'group-id-2' },
              body: JSON.stringify({ data: 'test-event-other-2' }),
            },
            { attributes: {}, body: JSON.stringify({ data: 'test-event-4' }) },
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
      const event1FinishedTime = messageFinished.mock.calls.find(
        (call) => call[0].data === 'test-event-1',
      )[1];

      const event3StartedTime = messageStarted.mock.calls.find(
        (call) => call[0].data === 'test-event-3',
      )[1];

      expect(event3StartedTime).toBeGreaterThanOrEqual(event1FinishedTime);

      const eventOther1FinishedTime = messageFinished.mock.calls.find(
        (call) => call[0].data === 'test-event-other-1',
      )[1];

      const eventOther2StartedTime = messageStarted.mock.calls.find(
        (call) => call[0].data === 'test-event-other-2',
      )[1];

      expect(eventOther2StartedTime).toBeGreaterThanOrEqual(
        eventOther1FinishedTime,
      );
    });
  });
});

import { v4 as uuid } from 'uuid';
import type { LoggerInterface } from '@lifeomic/logging';
import type {
  DynamoStreamHandler,
  DynamoStreamHandlerHarnessConfig,
  DynamoStreamHandlerHarnessContext,
} from './dynamo-streams';
import { BaseContext } from './utils';
import {
  SQSMessageHandler,
  SQSMessageHandlerHarnessContext,
  SQSMessageHandlerHarnessOptions,
} from './sqs';

/**
 * Returns a mock logger for use with assertions in a Jest environment.
 *
 * The `child(...)` function on the returned logger will just return the
 * same logger, to empower assertions such as:
 *
 * @example
 * const sourceCode = (logger) => {
 *   logger.info('base data');
 *
 *   const child = logger.child({ data: 'child data' });
 *
 *   child.info('more data');
 * };
 *
 * const mocked = useMockLogger();
 * const test = () => {
 *   sourceCode(mocked);
 *
 *   expect(mocked.info).toHaveBeenCalledWith('base data');
 *   expect(mocked.child).toHaveBeenCalledWith({ data: 'child data' });
 *   expect(mocked.info).toHaveBeenCalledWith('more data');
 * };
 */
export const useMockLogger = () => {
  const logger: jest.Mocked<LoggerInterface> = {
    debug: jest.fn(),
    trace: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    fatal: jest.fn(),
    child: jest.fn(),
  };

  beforeEach(() => {
    logger.debug.mockReset();
    logger.trace.mockReset();
    logger.info.mockReset();
    logger.warn.mockReset();
    logger.error.mockReset();
    logger.fatal.mockReset();
    logger.child.mockReset();
    logger.child.mockImplementation(() => logger);
  });

  return logger;
};

export type UseDynamoStreamHarnessContext<Entity, Context> =
  DynamoStreamHandlerHarnessContext<Entity> & {
    /**
     * The context in use by the stream handler.
     */
    context: BaseContext & Context;
  };

/**
 * Helper for creating a test harness to exercise a DynamoStreamHandler in
 * a Jest environment.
 *
 * @param stream The stream to harness.
 * @param config A harness configuration.
 */
export const useDynamoStreamHarness = <Entity, Context>(
  stream: DynamoStreamHandler<Entity, Context>,
  config: DynamoStreamHandlerHarnessConfig<Context>,
): UseDynamoStreamHarnessContext<Entity, Context> => {
  const context: UseDynamoStreamHarnessContext<Entity, Context> = {} as any;

  const logger = useMockLogger();

  beforeEach(async () => {
    const createContext =
      config.createRunContext ?? stream.config.createRunContext;

    const baseContext: BaseContext = {
      correlationId: uuid(),
      logger,
    };

    const runContext = await createContext(baseContext);

    const harnessContext = stream.harness({
      logger,
      createRunContext: () => runContext,
    });

    Object.assign(
      context,
      { context: { ...runContext, ...baseContext } },
      harnessContext,
    );
  });

  return context;
};

export type UseSQSQueueHarnessContext<Entity, Context> =
  SQSMessageHandlerHarnessContext<Entity> & {
    /**
     * The context in use by the stream handler.
     */
    context: BaseContext & Context;
  };

/**
 * Helper for creating a test harness to exercise an SQS handler in
 * a Jest environment.
 *
 * @param stream The stream to harness.
 * @param config A harness configuration.
 */
export const useSQSQueueHarness = <Entity, Context>(
  stream: SQSMessageHandler<Entity, Context>,
  config: SQSMessageHandlerHarnessOptions<Entity, Context>,
): UseSQSQueueHarnessContext<Entity, Context> => {
  const context: UseSQSQueueHarnessContext<Entity, Context> = {} as any;

  const logger = useMockLogger();

  beforeEach(async () => {
    const createContext =
      config.createRunContext ?? stream.config.createRunContext;

    const baseContext: BaseContext = {
      correlationId: uuid(),
      logger,
    };

    const runContext = await createContext(baseContext);

    const harnessContext = stream.harness({
      logger,
      stringifyMessage: config.stringifyMessage,
      createRunContext: () => runContext,
    });

    Object.assign(
      context,
      { context: { ...runContext, ...baseContext } },
      harnessContext,
    );
  });

  return context;
};

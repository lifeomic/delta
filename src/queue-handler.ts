import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { SQSEvent, Context as AWSContext } from 'aws-lambda';
import { BaseContext, withHealthCheck } from './utils';

export type QueueHandlerConfig<Entity, Context> = {
  /**
   * A logger to use in the context.
   */
  logger: LoggerInterface;
  /**
   * A function for parsing messages from the queue into your custom type.
   */
  parseMessage: (message: string) => Entity;
  /**
   * Create a "context" for the lambda execution. (e.g. "data sources")
   */
  createRunContext: (base: BaseContext) => Context | Promise<Context>;
};

export type MessageAction<Entity, Context> = (
  ctx: Context & BaseContext,
  message: Entity,
) => void | Promise<void>;

/* -- Test Harness Types -- */
export type QueueHandlerHarnessConfig<Entity, Context> = {
  /**
   * An optional override for the logger.
   */
  logger?: LoggerInterface;

  /**
   * A function for serialzing messages into queue-compatible message
   */
  serializeMessage: (message: Entity) => string;

  /**
   * An optional override for creating the run context.
   */
  createRunContext?: (base: BaseContext) => Context | Promise<Context>;
};

export type QueueHandlerHarnessContext<Entity> = {
  sendMessage: (event: QueueHandlerTestEvent<Entity>) => Promise<void>;
};

export type QueueHandlerTestRecord<Entity> = { body: Entity };

export type QueueHandlerTestEvent<Entity> = {
  records: QueueHandlerTestRecord<Entity>[];
};

/**
 * An abstraction for an SQS Lambda handler.
 */
export class QueueHandler<Entity, Context> {
  /**
   * The set of actions to perform per event type.
   */
  private actions: MessageAction<Entity, Context>[] = [];

  constructor(readonly config: QueueHandlerConfig<Entity, Context>) {}

  /**
   * Creates a replica of the stream handler, using the provided configuration
   * overrides.
   */
  private withOverrides(
    overrides: Partial<
      Pick<QueueHandlerConfig<Entity, Context>, 'createRunContext' | 'logger'>
    >,
  ): QueueHandler<Entity, Context> {
    const copy = new QueueHandler({
      parseMessage: this.config.parseMessage,
      logger: overrides.logger ?? this.config.logger,
      createRunContext:
        overrides.createRunContext ?? this.config.createRunContext,
    });

    for (const action of this.actions) {
      copy.onMessage(action);
    }

    return copy;
  }

  /**
   * Adds a message event handler.
   */
  onMessage(
    action: MessageAction<Entity, Context>,
  ): QueueHandler<Entity, Context> {
    this.actions.push(action);
    return this;
  }

  /**
   * Returns an SQS Lambda handler that will perform the configured
   * actions.
   */
  lambda(): (event: SQSEvent, context: AWSContext) => Promise<void> {
    // 1. Handle potential health checks.
    return withHealthCheck(async (event, ctx) => {
      const correlationId = uuid();

      const base: BaseContext = {
        correlationId,
        logger: this.config.logger.child({
          requestID: ctx.awsRequestId,
          correlationId,
        }),
      };

      const context: BaseContext & Context = {
        ...(await this.config.createRunContext(base)),
        ...base,
      };

      context.logger.info({ event }, 'Processing SQS event');

      // Iterate through every event.
      for (const record of event.Records) {
        const recordLogger = this.config.logger.child({
          messageId: record.messageId,
        });
        const entity = this.config.parseMessage(record.body);
        // Invoke every action
        for (const action of this.actions) {
          await action({ ...context, logger: recordLogger }, entity);
        }
      }
    });
  }

  /**
   * Returns a test harness for exercising the handler, with an optional
   * overriden context.
   */
  harness({
    logger,
    serializeMessage,
    createRunContext,
  }: QueueHandlerHarnessConfig<
    Entity,
    Context
  >): QueueHandlerHarnessContext<Entity> {
    const lambda = this.withOverrides({ logger, createRunContext }).lambda();

    return {
      sendMessage: async (event) => {
        const sqsEvent: SQSEvent = {
          Records: event.records.map<SQSEvent['Records'][number]>(
            ({ body }) => ({ body: serializeMessage(body) } as any),
          ),
        };
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        await lambda(sqsEvent, {} as any);
      },
    };
  }
}

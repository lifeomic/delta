import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { SQSEvent, Context as AWSContext } from 'aws-lambda';
import {
  BaseContext,
  processWithOrdering,
  withHealthCheckHandling,
} from './utils';

export type SQSMessageHandlerConfig<Message, Context> = {
  /**
   * A logger to use in the context.
   */
  logger: LoggerInterface;
  /**
   * A function for parsing SQS messages into your custom type.
   */
  parseMessage: (body: string) => Message;
  /**
   * Create a "context" for the lambda execution. (e.g. "data sources")
   */
  createRunContext: (base: BaseContext) => Context | Promise<Context>;
  /**
   * The maximum concurrency for processing messages.
   *
   * @default 5
   */
  concurrency?: number;
};

export type SQSMessageAction<Message, Context> = (
  context: Context & BaseContext,
  message: Message,
) => void | Promise<void>;

export type SQSMessageHandlerHarnessOptions<Message, Context> = {
  /**
   * A function for stringifying messages.
   */
  stringifyMessage: (message: Message) => string;

  /**
   * An optional override for the logger.
   */
  logger?: LoggerInterface;

  /**
   * An optional override for creating the run context.
   */
  createRunContext?: () => Context | Promise<Context>;
};

export type SQSMessageHandlerHarnessContext<Message> = {
  /** Sends the specified event through the handler. */
  sendEvent: (event: { messages: Message[] }) => Promise<void>;
};

/**
 * An abstraction for an SQS message handler.
 */
export class SQSMessageHandler<Message, Context> {
  private messageActions: SQSMessageAction<Message, Context>[] = [];

  constructor(readonly config: SQSMessageHandlerConfig<Message, Context>) {}

  /**
   * Adds a message action to the handler.
   *
   * @param handler The handler, for additional chaining.
   */
  onMessage(
    action: SQSMessageAction<Message, Context>,
  ): SQSMessageHandler<Message, Context> {
    this.messageActions.push(action);
    return this;
  }

  lambda(): (event: SQSEvent, context: AWSContext) => Promise<void> {
    return withHealthCheckHandling(async (event, awsContext) => {
      // 1. Build the context.
      const correlationId = uuid();
      const context: BaseContext & Context = {
        correlationId,
        logger: this.config.logger.child({
          requestID: awsContext.awsRequestId,
          correlationId,
        }),
      } as any;

      Object.assign(context, await this.config.createRunContext(context));

      // 2. Process all the records.
      context.logger.info({ event }, 'Processing SQS topic message');

      await processWithOrdering(
        {
          items: event.Records,
          // If there is not a MessageGroupId, then we don't care about
          // the ordering for the event. We can just generate a UUID for the
          // ordering key.
          orderBy: (record) => record.attributes.MessageGroupId ?? uuid(),
          concurrency: this.config.concurrency ?? 5,
          stopOnError: false,
        },
        async (record) => {
          const messageLogger = context.logger.child({
            messageId: record.messageId,
          });

          const parsedMessage = this.config.parseMessage(record.body);

          for (const action of this.messageActions) {
            await action({ ...context, logger: messageLogger }, parsedMessage);
          }

          messageLogger.info('Successfully processed message');
        },
      );

      context.logger.info('Succesfully processed all messages');
    });
  }

  harness({
    stringifyMessage,
    ...overrides
  }: SQSMessageHandlerHarnessOptions<
    Message,
    Context
  >): SQSMessageHandlerHarnessContext<Message> {
    // Make a copy of the handler.
    let handler = new SQSMessageHandler({ ...this.config, ...overrides });
    for (const action of this.messageActions) {
      handler = handler.onMessage(action);
    }
    const lambda = handler.lambda();

    return {
      sendEvent: async ({ messages }) => {
        const event: SQSEvent = {
          Records: messages.map(
            (msg) =>
              // We don't need to mock every field on this event -- there are lots.
              // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
              ({
                attributes: {},
                messageId: uuid(),
                body: stringifyMessage(msg),
              } as any),
          ),
        };

        await lambda(
          event,
          // We don't need to mock every field on the context -- there are lots.
          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
          { awsRequestId: uuid() } as any,
        );
      },
    };
  }
}

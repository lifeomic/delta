import { v4 as uuid } from 'uuid';
import { SQSEvent, Context as AWSContext, SQSRecord } from 'aws-lambda';
import bunyan from 'bunyan';
import get from 'lodash/get';
import {
  BaseContext,
  BaseHandlerConfig,
  PartialBatchResponse,
  handleUnprocessedRecords,
  processWithOrdering,
  withHealthCheckHandling,
} from './utils';
import { publicEncrypt } from 'crypto';
import { LoggerInterface } from './logging';

export type SQSMessageHandlerConfig<Message, Context> =
  BaseHandlerConfig<Context> & {
    /**
     * A function for parsing SQS messages into your custom type.
     */
    parseMessage: (body: string) => Message;

    redactionConfig?: {
      /**
       * This will be called to redact the message body before logging it. By
       * default, the full message body is logged.
       */
      redactMessageBody: (body: string) => string;

      /**
       * The public encryption key used for writing messages that contain
       * sensitive information but failed to be redacted.
       */
      publicEncryptionKey: string;

      /**
       * Logged with the encypted message to help identify the key used. For
       * example, this could explain who has access to the key or how to get it.
       */
      publicKeyDescription: string;
    };

    /**
     * Whether to ignore messages that fail to parse. If set to true,
     * throwing in the custom parsing function will cause the message
     * to be ignored, and never processed.
     */
    ignoreUnparseableMessages?: boolean;
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
  sendEvent: (event: { messages: Message[] }) => Promise<PartialBatchResponse>;
};

const safeRedactor =
  (
    logger: LoggerInterface,
    redactionConfig: NonNullable<
      SQSMessageHandlerConfig<any, any>['redactionConfig']
    >,
  ) =>
  (body: string) => {
    try {
      return redactionConfig.redactMessageBody(body);
    } catch (error) {
      let encryptedBody;

      // If redaction fails, then encrypt the message body and log it.
      // Encryption allows for developers to decrypt the message if needed
      // but does not log sensitive inforation the the log stream.
      try {
        encryptedBody = publicEncrypt(
          redactionConfig.publicEncryptionKey,
          Buffer.from(body),
        ).toString('base64');
      } catch (error) {
        // If encryption fails, then log the encryption error and replace
        // the body with dummy text.
        logger.error({ error }, 'Failed to encrypt message body');
        encryptedBody = '[ENCRYPTION FAILED]';
      }

      // Log the redaction error
      logger.error(
        {
          error,
          encryptedBody,
          publicKeyDescription: redactionConfig.publicKeyDescription,
        },
        'Failed to redact message body',
      );
      return '[REDACTION FAILED]';
    }
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

  lambda(): (
    event: SQSEvent,
    context: AWSContext,
  ) => Promise<PartialBatchResponse> {
    return withHealthCheckHandling(async (event, awsContext) => {
      // 1. Setup the logger.
      // The batch ID provides a way to correlate all messages that are processed together.
      // This is particularly useful for correlating handled and unhandled records.
      const batchId = uuid();

      /* istanbul ignore next */
      const logger =
        this.config.logger ??
        bunyan.createLogger({
          batchId,
          name: 'SQSMessageHandler',
          requestId: awsContext.awsRequestId,
        });

      // 2. Process all the records.
      const redactor = this.config.redactionConfig
        ? safeRedactor(logger, this.config.redactionConfig)
        : undefined;

      const redactRecord = (record: SQSRecord): SQSRecord =>
        redactor
          ? {
              ...record,
              body: redactor(record.body),
            }
          : record;

      const redactedEvent = redactor
        ? {
            ...event,
            Records: event.Records.map(redactRecord),
          }
        : event;
      logger.info({ event: redactedEvent }, 'Processing SQS topic message');

      const { unprocessedRecords } = await processWithOrdering(
        {
          items: event.Records,
          // If there is not a MessageGroupId, then we don't care about
          // the ordering for the event. We can just generate a UUID for the
          // ordering key.
          orderBy: (record: SQSRecord) =>
            record.attributes.MessageGroupId ?? uuid(),
          concurrency: this.config.concurrency ?? 5,
        },
        async (record) => {
          const messageLogger = logger.child({
            messageId: record.messageId,
          });

          let parsedMessage: Message;
          try {
            parsedMessage = this.config.parseMessage(record.body);
          } catch (err) {
            messageLogger.error({ err }, 'Failed to parse message');
            if (this.config.ignoreUnparseableMessages) {
              messageLogger.warn(
                'ignoreUnparseableMessages is set to true. Ignoring message.',
              );
              return;
            }
            throw err;
          }

          // Extracting the correlation ID from the message allows for
          // multiple messages to be correlated together.
          const correlationId = get(parsedMessage, 'correlationId', uuid());

          // Context is built per message to support message-specific correlation IDs.
          const context: BaseContext & Context = {
            correlationId,
            logger: messageLogger.child({ correlationId }),
          } as any;

          Object.assign(context, await this.config.createRunContext(context));

          for (const action of this.messageActions) {
            await action({ ...context }, parsedMessage);
          }

          context.logger.info('Successfully processed SQS message');
        },
      );

      return handleUnprocessedRecords({
        logger, // The base logger only logs the batch ID.
        unprocessedRecords,
        usePartialBatchResponses: !!this.config.usePartialBatchResponses,
        getItemIdentifier: (record) => record.messageId,
        redactRecord,
      });
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
      sendEvent: ({ messages }) => {
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

        return lambda(
          event,
          // We don't need to mock every field on the context -- there are lots.
          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
          { awsRequestId: uuid() } as any,
        );
      },
    };
  }
}

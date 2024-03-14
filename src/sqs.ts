import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { SQSEvent, Context as AWSContext } from 'aws-lambda';
import {
  BaseContext,
  BaseHandlerConfig,
  processWithOrdering,
  withHealthCheckHandling,
} from './utils';
import { publicEncrypt } from 'crypto';

export type SQSMessageHandlerConfig<Message, Context> =
  BaseHandlerConfig<Context> & {
    /**
     * A function for parsing SQS messages into your custom type.
     */
    parseMessage: (body: string) => Message;

    /**
     * Whether or not to use SQS partial batch responses. If set to true, make
     * sure to also turn on partial batch responses when configuring your event
     * source mapping by specifying ReportBatchItemFailures for the
     * FunctionResponseTypes action. For more details see:
     * https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting
     */
    usePartialBatchResponses?: boolean;

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

export type SQSPartialBatchResponse = {
  batchItemFailures: {
    itemIdentifier: string;
  }[];
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
  ) => Promise<void | SQSPartialBatchResponse> {
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
      const redactor = this.config.redactionConfig
        ? safeRedactor(context.logger, this.config.redactionConfig)
        : undefined;
      const redactedEvent = redactor
        ? {
            ...event,
            Records: event.Records.map((record) => ({
              ...record,
              body: redactor(record.body),
            })),
          }
        : event;
      context.logger.info(
        { event: redactedEvent },
        'Processing SQS topic message',
      );

      const processingResult = await processWithOrdering(
        {
          items: event.Records,
          // If there is not a MessageGroupId, then we don't care about
          // the ordering for the event. We can just generate a UUID for the
          // ordering key.
          orderBy: (record) => record.attributes.MessageGroupId ?? uuid(),
          concurrency: this.config.concurrency ?? 5,
        },
        async (record) => {
          const messageLogger = context.logger.child({
            messageId: record.messageId,
          });

          const parsedMessage = this.config.parseMessage(record.body);

          for (const action of this.messageActions) {
            await action({ ...context, logger: messageLogger }, parsedMessage);
          }

          messageLogger.info('Successfully processed SQS message');
        },
      );

      const unprocessedRecordsByGroupIdEntries = Object.entries(
        processingResult.unprocessedRecordsByGroupId,
      );

      if (!unprocessedRecordsByGroupIdEntries.length) {
        context.logger.info('Successfully processed all SQS messages');
        return;
      }

      if (!this.config.usePartialBatchResponses) {
        processingResult.throwOnUnprocessedRecords();
      }

      // SQS partial batching expects that you return an ordered list of
      // failures. We map through each group and add them to the batch item
      // failures in order for each group.
      const batchItemFailures = unprocessedRecordsByGroupIdEntries
        .map(([groupId, record]) => {
          const [failedRecord, ...subsequentUnprocessedRecords] = record.items;
          context.logger.error(
            {
              groupId,
              err: record.error,
              failedRecord,
              subsequentUnprocessedRecords,
            },
            'Failed to fully process message group',
          );

          return record.items.map((item) => ({
            itemIdentifier: item.messageId,
          }));
        })
        .flat();

      context.logger.info(
        { batchItemFailures },
        'Sending SQS partial batch response',
      );

      return { batchItemFailures };
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

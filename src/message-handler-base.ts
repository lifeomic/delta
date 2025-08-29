import { Context as AWSContext } from 'aws-lambda';
import bunyan from 'bunyan';
import { publicEncrypt } from 'crypto';
import get from 'lodash/get';
import { v4 as uuid } from 'uuid';
import { LoggerInterface } from './logging';
import {
  BaseContext,
  BaseHandlerConfig,
  PartialBatchResponse,
  handleUnprocessedRecords,
  processWithOrdering,
  withHealthCheckHandling,
} from './utils';

export interface MessageHandlerConfig<Message, Context>
  extends BaseHandlerConfig<Context> {
  /**
   * A function for parsing messages into your custom type.
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
     * Logged with the encrypted message to help identify the key used. For
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
}

export type MessageAction<Message, Context> = (
  context: Context & BaseContext,
  message: Message,
) => void | Promise<void>;

export interface MessageHandlerHarnessOptions<Message, Context> {
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
}

export interface MessageHandlerHarnessContext<Message> {
  /** Sends the specified event through the handler. */
  sendEvent: (event: { messages: Message[] }) => Promise<PartialBatchResponse>;
}

export interface RecordExtractor<TRecord> {
  extractMessage: (record: TRecord) => string;
  extractMessageId: (record: TRecord) => string;
  extractOrderingKey: (record: TRecord) => string;
  redactRecord: (record: TRecord, redactedMessage: string) => TRecord;
}

export interface EventAdapter<TEvent, TRecord> {
  extractRecords: (event: TEvent) => TRecord[];
  createMockEvent: (records: TRecord[]) => TEvent;
  createMockRecord: (messageBody: string, index: number) => TRecord;
  getLogMessage: () => string;
  getHandlerName: () => string;
}

const safeRedactor =
  (
    logger: LoggerInterface,
    redactionConfig: NonNullable<
      MessageHandlerConfig<any, any>['redactionConfig']
    >,
  ) =>
  (body: string) => {
    try {
      return redactionConfig.redactMessageBody(body);
    } catch (error) {
      let encryptedBody;

      // If redaction fails, then encrypt the message body and log it.
      // Encryption allows for developers to decrypt the message if needed
      // but does not log sensitive information to the log stream.
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
 * Base class for message handlers that provides common functionality.
 */
export abstract class MessageHandlerBase<TEvent, TRecord, TMessage, TContext> {
  protected messageActions: MessageAction<TMessage, TContext>[] = [];

  constructor(
    readonly config: MessageHandlerConfig<TMessage, TContext>,
    protected recordExtractor: RecordExtractor<TRecord>,
    protected eventAdapter: EventAdapter<TEvent, TRecord>,
  ) {}

  protected addAction(action: MessageAction<TMessage, TContext>): void {
    this.messageActions.push(action);
  }

  lambda(): (
    event: TEvent,
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
          name: this.eventAdapter.getHandlerName(),
          requestId: awsContext.awsRequestId,
        });

      // 2. Process all the records.
      const records = this.eventAdapter.extractRecords(event);
      const redactor = this.config.redactionConfig
        ? safeRedactor(logger, this.config.redactionConfig)
        : undefined;

      const redactRecord = (record: TRecord): TRecord =>
        redactor
          ? this.recordExtractor.redactRecord(
              record,
              redactor(this.recordExtractor.extractMessage(record)),
            )
          : record;

      const redactedEvent = redactor
        ? this.eventAdapter.createMockEvent(records.map(redactRecord))
        : event;

      logger.info({ event: redactedEvent }, this.eventAdapter.getLogMessage());

      const { unprocessedRecords } = await processWithOrdering(
        {
          items: records,
          orderBy: (record: TRecord) =>
            this.recordExtractor.extractOrderingKey(record),
          concurrency: this.config.concurrency ?? 5,
        },
        async (record) => {
          const messageLogger = logger.child({
            messageId: this.recordExtractor.extractMessageId(record),
          });

          let parsedMessage: TMessage;
          try {
            parsedMessage = this.config.parseMessage(
              this.recordExtractor.extractMessage(record),
            );
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
          const context: BaseContext & TContext = {
            correlationId,
            logger: messageLogger.child({ correlationId }),
          } as any;

          Object.assign(context, await this.config.createRunContext(context));

          for (const action of this.messageActions) {
            await action({ ...context }, parsedMessage);
          }

          context.logger.info(
            `Successfully processed ${this.eventAdapter
              .getHandlerName()
              .replace('Handler', '')
              .toLowerCase()} message`,
          );
        },
      );

      return handleUnprocessedRecords({
        logger, // The base logger only logs the batch ID.
        unprocessedRecords,
        usePartialBatchResponses: !!this.config.usePartialBatchResponses,
        getItemIdentifier: (record) =>
          this.recordExtractor.extractMessageId(record),
        redactRecord,
      });
    });
  }

  harness(
    options: MessageHandlerHarnessOptions<TMessage, TContext>,
    HandlerClass: new (config: MessageHandlerConfig<TMessage, TContext>) => any,
  ): MessageHandlerHarnessContext<TMessage> {
    const { stringifyMessage, ...overrides } = options;

    // Make a copy of the handler.
    const handler = new HandlerClass({ ...this.config, ...overrides });
    for (const action of this.messageActions) {
      handler.onMessage(action);
    }
    const lambda = handler.lambda();

    return {
      sendEvent: ({ messages }) => {
        const records = messages.map((msg, index) =>
          this.eventAdapter.createMockRecord(stringifyMessage(msg), index),
        );
        const event = this.eventAdapter.createMockEvent(records);

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

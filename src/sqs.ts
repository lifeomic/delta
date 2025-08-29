import { SQSEvent, SQSRecord } from 'aws-lambda';
import { v4 as uuid } from 'uuid';
import {
  EventAdapter,
  MessageAction,
  MessageHandlerBase,
  MessageHandlerConfig,
  MessageHandlerHarnessContext,
  MessageHandlerHarnessOptions,
  RecordExtractor,
} from './message-handler-base';

export type SQSMessageHandlerConfig<Message, Context> = MessageHandlerConfig<
  Message,
  Context
>;

export type SQSMessageAction<Message, Context> = MessageAction<
  Message,
  Context
>;

export type SQSMessageHandlerHarnessOptions<Message, Context> =
  MessageHandlerHarnessOptions<Message, Context>;

export type SQSMessageHandlerHarnessContext<Message> =
  MessageHandlerHarnessContext<Message>;

const SQS_RECORD_EXTRACTOR: RecordExtractor<SQSRecord> = {
  extractMessage: (record) => record.body,
  extractMessageId: (record) => record.messageId,
  extractOrderingKey: (record) => record.attributes.MessageGroupId ?? uuid(),
  redactRecord: (record, redactedMessage) => ({
    ...record,
    body: redactedMessage,
  }),
};

const SQS_EVENT_ADAPTER: EventAdapter<SQSEvent, SQSRecord> = {
  extractRecords: (event) => event.Records,
  createMockEvent: (records) => ({ Records: records }),
  createMockRecord: (messageBody) =>
    ({
      attributes: {},
      messageId: uuid(),
      body: messageBody,
    } as any),
  getLogMessage: () => 'Processing SQS topic message',
  getHandlerName: () => 'SQSMessageHandler',
};

/**
 * An abstraction for an SQS message handler.
 */
export class SQSMessageHandler<Message, Context> extends MessageHandlerBase<
  SQSEvent,
  SQSRecord,
  Message,
  Context
> {
  constructor(config: SQSMessageHandlerConfig<Message, Context>) {
    super(config, SQS_RECORD_EXTRACTOR, SQS_EVENT_ADAPTER);
  }

  /**
   * Adds a message action to the handler.
   *
   * @param handler The handler, for additional chaining.
   */
  onMessage(
    action: SQSMessageAction<Message, Context>,
  ): SQSMessageHandler<Message, Context> {
    this.addAction(action);
    return this;
  }

  harness(
    options: SQSMessageHandlerHarnessOptions<Message, Context>,
  ): SQSMessageHandlerHarnessContext<Message> {
    return super.harness(options, SQSMessageHandler);
  }
}

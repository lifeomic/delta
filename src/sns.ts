import { SNSEvent, SNSEventRecord } from 'aws-lambda';
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

export type SNSMessageHandlerConfig<Message, Context> = MessageHandlerConfig<
  Message,
  Context
>;

export type SNSMessageAction<Message, Context> = MessageAction<
  Message,
  Context
>;

export type SNSMessageHandlerHarnessOptions<Message, Context> =
  MessageHandlerHarnessOptions<Message, Context>;

export type SNSMessageHandlerHarnessContext<Message> =
  MessageHandlerHarnessContext<Message>;

const SNS_RECORD_EXTRACTOR: RecordExtractor<SNSEventRecord> = {
  extractMessage: (record) => record.Sns.Message,
  extractMessageId: (record) => record.Sns.MessageId ?? '',
  extractOrderingKey: (record) => record.Sns.MessageId ?? uuid(),
  redactRecord: (record, redactedMessage) => ({
    ...record,
    Sns: {
      ...record.Sns,
      Message: redactedMessage,
    },
  }),
};

const SNS_EVENT_ADAPTER: EventAdapter<SNSEvent, SNSEventRecord> = {
  extractRecords: (event) => event.Records,
  createMockEvent: (records) => ({ Records: records }),
  createMockRecord: (messageBody, index) =>
    ({
      EventVersion: '1.0',
      EventSubscriptionArn: 'test-subscription',
      EventSource: 'aws:sns',
      Sns: {
        SignatureVersion: '1',
        Timestamp: new Date().toISOString(),
        Signature: 'mock-signature',
        SigningCertUrl: 'mock-cert-url',
        MessageId: `test-message-${index}`,
        Message: messageBody,
        MessageAttributes: {},
        Type: 'Notification',
        UnsubscribeUrl: 'mock-unsubscribe-url',
        TopicArn: 'test-topic-arn',
        Subject: null,
      },
    } as any),
  getLogMessage: () => 'Processing SNS topic message',
  getHandlerName: () => 'SNSMessageHandler',
};

/**
 * An abstraction for an SNS message handler.
 */
export class SNSMessageHandler<Message, Context> extends MessageHandlerBase<
  SNSEvent,
  SNSEventRecord,
  Message,
  Context
> {
  constructor(config: SNSMessageHandlerConfig<Message, Context>) {
    super(config, SNS_RECORD_EXTRACTOR, SNS_EVENT_ADAPTER);
  }

  /**
   * Adds a message action to the handler.
   *
   * @param handler The handler, for additional chaining.
   */
  onMessage(
    action: SNSMessageAction<Message, Context>,
  ): SNSMessageHandler<Message, Context> {
    this.addAction(action);
    return this;
  }

  harness(
    options: SNSMessageHandlerHarnessOptions<Message, Context>,
  ): SNSMessageHandlerHarnessContext<Message> {
    return super.harness(options, SNSMessageHandler);
  }
}

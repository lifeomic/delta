import { privateDecrypt } from 'crypto';
import { promises as fs } from 'fs';
import { v4 as uuid } from 'uuid';

import { useMockLogger } from './jest-utils';
import { SNSMessageAction, SNSMessageHandler } from './sns';

const logger = useMockLogger();

let publicKey: string;
beforeAll(async () => {
  publicKey = await fs.readFile(
    __dirname + '/__fixtures__/public-key.pem',
    'utf8',
  );
});

const testSerializer = {
  parseMessage: (msg: string) => JSON.parse(msg),
  stringifyMessage: (msg: any) => JSON.stringify(msg),
};

describe('SNSMessageHandler', () => {
  test('responds to HTTP health checks', async () => {
    const lambda = new SNSMessageHandler({
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
    const lambda = new SNSMessageHandler({
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
    expect.assertions(3);

    const lambda = new SNSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: (ctx) => {
        expect(typeof ctx.correlationId === 'string').toBe(true);
        return {};
      },
    }).lambda();

    const response = await lambda(
      {
        Records: [
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: 'test-message-id',
              Message: JSON.stringify({ data: 'test-event-1' }),
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
        ],
      } as any,
      {} as any,
    );

    // Assert that when all messages are processed successfully and partial
    // batch responses are not used (the default setting), nothing is returned
    // as the lambda response.
    expect(response).toBeUndefined();
    expect(logger.child).toHaveBeenCalledWith(
      expect.objectContaining({ correlationId: expect.any(String) }),
    );
  });

  test('accepts a correlation id', async () => {
    expect.assertions(3);

    const lambda = new SNSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: (ctx) => {
        expect(typeof ctx.correlationId === 'string').toBe(true);
        return {};
      },
    }).lambda();

    const correlationId = uuid();
    const response = await lambda(
      {
        Records: [
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: 'test-message-id',
              Message: JSON.stringify({ correlationId, data: 'test-event-1' }),
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
        ],
      } as any,
      {} as any,
    );

    // Assert that when all messages are processed successfully and partial
    // batch responses are not used (the default setting), nothing is returned
    // as the lambda response.
    expect(response).toBeUndefined();
    expect(logger.child).toHaveBeenCalledWith(
      expect.objectContaining({ correlationId }),
    );
  });

  test('allows body redaction', async () => {
    expect.assertions(2);

    const lambda = new SNSMessageHandler({
      logger,
      redactionConfig: {
        redactMessageBody: () => 'REDACTED',
        publicEncryptionKey: publicKey,
        publicKeyDescription: 'test-public-key',
      },
      parseMessage: testSerializer.parseMessage,
      createRunContext: (ctx) => {
        expect(typeof ctx.correlationId === 'string').toBe(true);
        return {};
      },
    }).lambda();

    await lambda(
      {
        Records: [
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: 'test-message-id',
              Message: JSON.stringify({ data: 'test-event-1' }),
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
        ],
      } as any,
      {} as any,
    );

    // Assert that the message body was redacted.
    expect(logger.info).toHaveBeenCalledWith(
      {
        event: {
          Records: [
            {
              EventVersion: '1.0',
              EventSubscriptionArn: 'test-subscription',
              EventSource: 'aws:sns',
              Sns: {
                SignatureVersion: '1',
                Timestamp: '2023-01-01T00:00:00.000Z',
                Signature: 'test-signature',
                SigningCertUrl: 'test-cert-url',
                MessageId: 'test-message-id',
                Message: 'REDACTED',
                MessageAttributes: {},
                Type: 'Notification',
                UnsubscribeUrl: 'test-unsubscribe-url',
                TopicArn: 'test-topic-arn',
                Subject: null,
              },
            },
          ],
        },
      },
      'Processing SNS topic message',
    );
  });

  test('if redaction fails, a redacted body is logged with an encrypted body', async () => {
    expect.assertions(5);

    const error = new Error('Failed to redact message');
    const lambda = new SNSMessageHandler({
      logger,
      redactionConfig: {
        redactMessageBody: () => {
          throw error;
        },
        publicEncryptionKey: publicKey,
        publicKeyDescription: 'test-public-key',
      },
      parseMessage: testSerializer.parseMessage,
      createRunContext: (ctx) => {
        expect(typeof ctx.correlationId === 'string').toBe(true);
        return {};
      },
    }).lambda();

    const messageBody = JSON.stringify({ data: 'test-event-1' });
    const event = {
      Records: [
        {
          EventVersion: '1.0',
          EventSubscriptionArn: 'test-subscription',
          EventSource: 'aws:sns',
          Sns: {
            SignatureVersion: '1',
            Timestamp: '2023-01-01T00:00:00.000Z',
            Signature: 'test-signature',
            SigningCertUrl: 'test-cert-url',
            MessageId: 'test-message-id',
            Message: messageBody,
            MessageAttributes: {},
            Type: 'Notification',
            UnsubscribeUrl: 'test-unsubscribe-url',
            TopicArn: 'test-topic-arn',
            Subject: null,
          },
        },
      ],
    } as any;
    const response = await lambda(event, {} as any);

    // Expect no failure
    expect(response).toBeUndefined();

    // Assert that the message body was shown redacted. Along with the
    // redacted body, an encrypted body is also logged with the redaction
    // error to help debugging.
    expect(logger.error).toHaveBeenCalledWith(
      {
        error,
        encryptedBody: expect.any(String),
        publicKeyDescription: 'test-public-key',
      },
      'Failed to redact message body',
    );

    // Verify that the encrypted body can be decrypted.
    const privateKey = await fs.readFile(
      __dirname + '/__fixtures__/private-key.pem',
      'utf8',
    );
    const encryptedBody = logger.error.mock.calls[0][0].encryptedBody;
    const decrypted = privateDecrypt(
      privateKey,
      Buffer.from(encryptedBody, 'base64'),
    ).toString('utf8');
    expect(decrypted).toEqual(messageBody);

    // Verify the the body was redacted.
    expect(logger.info).toHaveBeenCalledWith(
      {
        event: {
          ...event,
          Records: event.Records.map((record: any) => ({
            ...record,
            Sns: {
              ...record.Sns,
              Message: '[REDACTION FAILED]',
            },
          })),
        },
      },
      'Processing SNS topic message',
    );
  });

  test('if redaction fails, and encryption fails, a redacted body is logged with the encryption failure', async () => {
    expect.assertions(5);

    const error = new Error('Failed to redact message');
    const lambda = new SNSMessageHandler({
      logger,
      redactionConfig: {
        redactMessageBody: () => {
          throw error;
        },
        publicEncryptionKey: 'not-a-valid-key',
        publicKeyDescription: 'test-public-key',
      },
      parseMessage: testSerializer.parseMessage,
      createRunContext: (ctx) => {
        expect(typeof ctx.correlationId === 'string').toBe(true);
        return {};
      },
    }).lambda();

    const messageBody = JSON.stringify({ data: 'test-event-1' });
    const event = {
      Records: [
        {
          EventVersion: '1.0',
          EventSubscriptionArn: 'test-subscription',
          EventSource: 'aws:sns',
          Sns: {
            SignatureVersion: '1',
            Timestamp: '2023-01-01T00:00:00.000Z',
            Signature: 'test-signature',
            SigningCertUrl: 'test-cert-url',
            MessageId: 'test-message-id',
            Message: messageBody,
            MessageAttributes: {},
            Type: 'Notification',
            UnsubscribeUrl: 'test-unsubscribe-url',
            TopicArn: 'test-topic-arn',
            Subject: null,
          },
        },
      ],
    } as any;
    const response = await lambda(event, {} as any);

    // Expect no failure
    expect(response).toBeUndefined();

    // Assert that the message body was shown redacted. Along with the
    // redacted body, an encrypted body is also logged with the redaction
    // error to help debugging.
    expect(logger.error).toHaveBeenCalledWith(
      {
        error,
        encryptedBody: '[ENCRYPTION FAILED]', // Signals that encryption failed
        publicKeyDescription: 'test-public-key',
      },
      'Failed to redact message body',
    );

    // When encryption fails, the failure is logged.
    expect(logger.error).toHaveBeenCalledWith(
      {
        error: expect.anything(),
      },
      'Failed to encrypt message body',
    );

    // Verify the the body was redacted.
    expect(logger.info).toHaveBeenCalledWith(
      {
        event: {
          ...event,
          Records: event.Records.map((record: any) => ({
            ...record,
            Sns: {
              ...record.Sns,
              Message: '[REDACTION FAILED]',
            },
          })),
        },
      },
      'Processing SNS topic message',
    );
  });

  describe('error handling', () => {
    const records = [
      {
        EventVersion: '1.0',
        EventSubscriptionArn: 'test-subscription',
        EventSource: 'aws:sns',
        Sns: {
          SignatureVersion: '1',
          Timestamp: '2023-01-01T00:00:00.000Z',
          Signature: 'test-signature',
          SigningCertUrl: 'test-cert-url',
          MessageId: 'message-1',
          Message: JSON.stringify({ name: 'test-event-1' }),
          MessageAttributes: {},
          Type: 'Notification',
          UnsubscribeUrl: 'test-unsubscribe-url',
          TopicArn: 'test-topic-arn',
          Subject: null,
        },
      },
      {
        EventVersion: '1.0',
        EventSubscriptionArn: 'test-subscription',
        EventSource: 'aws:sns',
        Sns: {
          SignatureVersion: '1',
          Timestamp: '2023-01-01T00:00:00.000Z',
          Signature: 'test-signature',
          SigningCertUrl: 'test-cert-url',
          MessageId: 'message-2',
          Message: JSON.stringify({ name: 'test-event-2' }),
          MessageAttributes: {},
          Type: 'Notification',
          UnsubscribeUrl: 'test-unsubscribe-url',
          TopicArn: 'test-topic-arn',
          Subject: null,
        },
      },
      {
        EventVersion: '1.0',
        EventSubscriptionArn: 'test-subscription',
        EventSource: 'aws:sns',
        Sns: {
          SignatureVersion: '1',
          Timestamp: '2023-01-01T00:00:00.000Z',
          Signature: 'test-signature',
          SigningCertUrl: 'test-cert-url',
          MessageId: 'message-3',
          Message: JSON.stringify({ name: 'test-event-3' }),
          MessageAttributes: {},
          Type: 'Notification',
          UnsubscribeUrl: 'test-unsubscribe-url',
          TopicArn: 'test-topic-arn',
          Subject: null,
        },
      },
      {
        EventVersion: '1.0',
        EventSubscriptionArn: 'test-subscription',
        EventSource: 'aws:sns',
        Sns: {
          SignatureVersion: '1',
          Timestamp: '2023-01-01T00:00:00.000Z',
          Signature: 'test-signature',
          SigningCertUrl: 'test-cert-url',
          MessageId: 'message-4',
          Message: JSON.stringify({ name: 'test-event-4' }),
          MessageAttributes: {},
          Type: 'Notification',
          UnsubscribeUrl: 'test-unsubscribe-url',
          TopicArn: 'test-topic-arn',
          Subject: null,
        },
      },
    ];

    const errorMessageHandler: SNSMessageAction<{ name: string }, any> = (
      ctx,
      message,
    ) => {
      // Fail on the third message
      if (message.name === 'test-event-3') {
        throw new Error(`Failed to process message ${message.name}`);
      }
    };

    const successMessageHandler: SNSMessageAction<
      { name: string },
      any
    > = () => {
      return Promise.resolve();
    };

    test('throws on unprocessed events by default', async () => {
      expect.assertions(1);
      const handler = new SNSMessageHandler({
        logger,
        parseMessage: testSerializer.parseMessage,
        createRunContext: () => ({}),
        concurrency: 2,
      })
        .onMessage(errorMessageHandler)
        .lambda();

      try {
        await handler(
          {
            Records: records,
          } as any,
          {} as any,
        );
      } catch (e: any) {
        expect(e.message).toContain(
          'Error: Failed to process message test-event-3',
        );
      }
    });

    test('returns partial batch response when setting is enabled', async () => {
      const handler = new SNSMessageHandler({
        logger,
        parseMessage: testSerializer.parseMessage,
        createRunContext: () => ({}),
        usePartialBatchResponses: true,
        concurrency: 2,
      })
        .onMessage(errorMessageHandler)
        .lambda();

      const result = await handler(
        {
          Records: records,
        } as any,
        {} as any,
      );

      // Expect that the bodies have not been redacted
      expect(logger.error).toHaveBeenCalledWith(
        expect.objectContaining({
          itemIdentifier: 'message-3',
          failedRecord: expect.objectContaining({
            Sns: expect.objectContaining({
              Message: JSON.stringify({
                name: `test-event-3`,
              }),
            }),
          }),
          err: expect.objectContaining({
            message: 'Failed to process message test-event-3',
          }),
        }),
        'Failed to process record',
      );

      const batchItemFailures = [{ itemIdentifier: 'message-3' }];

      expect(result).toEqual({
        batchItemFailures,
      });
      expect(logger.info).not.toHaveBeenCalledWith(
        'Successfully processed all messages',
      );
      expect(logger.info).toHaveBeenCalledWith(
        {
          batchItemFailures,
        },
        'Completing with partial batch response',
      );
    });

    test('returns nothing when all events are processed successfully', async () => {
      const handler = new SNSMessageHandler({
        logger,
        parseMessage: testSerializer.parseMessage,
        createRunContext: () => ({}),
        usePartialBatchResponses: true,
        concurrency: 2,
      })
        .onMessage(successMessageHandler)
        .lambda();

      const result = await handler(
        {
          Records: records,
        } as any,
        {} as any,
      );

      expect(result).toEqual(undefined);
      expect(logger.error).not.toHaveBeenCalledWith(
        expect.any(Object),
        'Failed to fully process message group',
      );
      expect(logger.info).toHaveBeenCalledWith(
        'Successfully processed all records',
      );
    });

    test('redacts bodies of partial failures when redaction is enabled', async () => {
      const handler = new SNSMessageHandler({
        logger,
        parseMessage: testSerializer.parseMessage,
        redactionConfig: {
          redactMessageBody: () => 'REDACTED',
          publicEncryptionKey: publicKey,
          publicKeyDescription: 'test-public-key',
        },
        createRunContext: () => ({}),
        usePartialBatchResponses: true,
        concurrency: 2,
      })
        .onMessage(errorMessageHandler)
        .lambda();

      await handler(
        {
          Records: records,
        } as any,
        {} as any,
      );

      // Expect that the bodies have been redacted
      expect(logger.error).toHaveBeenCalledWith(
        expect.objectContaining({
          itemIdentifier: 'message-3',
          failedRecord: expect.objectContaining({
            Sns: expect.objectContaining({
              Message: 'REDACTED',
              MessageId: 'message-3',
            }),
          }),
          err: expect.objectContaining({
            message: 'Failed to process message test-event-3',
          }),
        }),
        'Failed to process record',
      );
    });
  });

  test('sending messages with context', async () => {
    const dataSources = {
      doSomething: jest.fn(),
    };

    const lambda = new SNSMessageHandler({
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
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: 'message-1',
              Message: JSON.stringify({ data: 'test-event-1' }),
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: 'message-2',
              Message: JSON.stringify({ data: 'test-event-2' }),
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
        ],
      } as any,
      {} as any,
    );

    // Expect 4 calls. 2 message handlers * 2 events.
    expect(dataSources.doSomething).toHaveBeenCalledTimes(4);

    // Now, confirm the ordering.
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      1,
      'first-handler',
      {
        data: 'test-event-1',
      },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      2,
      'second-handler',
      {
        data: 'test-event-1',
      },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      3,
      'first-handler',
      {
        data: 'test-event-2',
      },
    );
    expect(dataSources.doSomething).toHaveBeenNthCalledWith(
      4,
      'second-handler',
      {
        data: 'test-event-2',
      },
    );
  });

  describe('harness', () => {
    test('sends events correctly', async () => {
      const dataSources = {
        doSomething: jest.fn(),
      };

      const { sendEvent } = new SNSMessageHandler({
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

      const { sendEvent } = new SNSMessageHandler({
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
      const handler = new SNSMessageHandler({
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
            {
              EventVersion: '1.0',
              EventSubscriptionArn: 'test-subscription',
              EventSource: 'aws:sns',
              Sns: {
                SignatureVersion: '1',
                Timestamp: '2023-01-01T00:00:00.000Z',
                Signature: 'test-signature',
                SigningCertUrl: 'test-cert-url',
                MessageId: 'message-1',
                Message: JSON.stringify({ data: 'test-event-1' }),
                MessageAttributes: {},
                Type: 'Notification',
                UnsubscribeUrl: 'test-unsubscribe-url',
                TopicArn: 'test-topic-arn',
                Subject: null,
              },
            },
            {
              EventVersion: '1.0',
              EventSubscriptionArn: 'test-subscription',
              EventSource: 'aws:sns',
              Sns: {
                SignatureVersion: '1',
                Timestamp: '2023-01-01T00:00:00.000Z',
                Signature: 'test-signature',
                SigningCertUrl: 'test-cert-url',
                MessageId: 'message-2',
                Message: JSON.stringify({ data: 'test-event-2' }),
                MessageAttributes: {},
                Type: 'Notification',
                UnsubscribeUrl: 'test-unsubscribe-url',
                TopicArn: 'test-topic-arn',
                Subject: null,
              },
            },
            {
              EventVersion: '1.0',
              EventSubscriptionArn: 'test-subscription',
              EventSource: 'aws:sns',
              Sns: {
                SignatureVersion: '1',
                Timestamp: '2023-01-01T00:00:00.000Z',
                Signature: 'test-signature',
                SigningCertUrl: 'test-cert-url',
                MessageId: 'message-3',
                Message: JSON.stringify({ data: 'test-event-3' }),
                MessageAttributes: {},
                Type: 'Notification',
                UnsubscribeUrl: 'test-unsubscribe-url',
                TopicArn: 'test-topic-arn',
                Subject: null,
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
  });

  test('throws when encountering an unparseable message', async () => {
    const lambda = new SNSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: () => ({}),
    }).lambda();

    await expect(
      lambda(
        {
          Records: [
            {
              EventVersion: '1.0',
              EventSubscriptionArn: 'test-subscription',
              EventSource: 'aws:sns',
              Sns: {
                SignatureVersion: '1',
                Timestamp: '2023-01-01T00:00:00.000Z',
                Signature: 'test-signature',
                SigningCertUrl: 'test-cert-url',
                MessageId: 'message-1',
                Message: 'not-a-json-string',
                MessageAttributes: {},
                Type: 'Notification',
                UnsubscribeUrl: 'test-unsubscribe-url',
                TopicArn: 'test-topic-arn',
                Subject: null,
              },
            },
          ],
        } as any,
        {} as any,
      ),
    ).rejects.toThrow(/Unexpected token/);

    expect(logger.error).toHaveBeenCalledWith(
      expect.objectContaining({
        err: expect.objectContaining({
          message: expect.stringContaining('Unexpected token'),
        }),
      }),
      'Failed to parse message',
    );
  });

  test('respects ignoreUnparseableMessages', async () => {
    const processor = jest.fn().mockResolvedValue(void 0);
    const lambda = new SNSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: () => ({}),
      ignoreUnparseableMessages: true,
    })
      .onMessage((ctx, message) => processor(message))
      .lambda();

    await lambda(
      {
        Records: [
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: 'message-1',
              Message: 'not-a-json-string',
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: 'message-2',
              Message: JSON.stringify({ message: 'test-message' }),
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
        ],
      } as any,
      {} as any,
    );

    expect(logger.error).toHaveBeenCalledWith(
      expect.objectContaining({
        err: expect.objectContaining({
          message: expect.stringContaining('Unexpected token'),
        }),
      }),
      'Failed to parse message',
    );

    expect(logger.warn).toHaveBeenCalledWith(
      'ignoreUnparseableMessages is set to true. Ignoring message.',
    );

    expect(processor).toHaveBeenCalledTimes(1);
    expect(processor).toHaveBeenCalledWith({ message: 'test-message' });
  });

  test('handles missing MessageId gracefully', async () => {
    const processor = jest.fn();

    const lambda = new SNSMessageHandler({
      logger,
      parseMessage: testSerializer.parseMessage,
      createRunContext: () => ({}),
    })
      .onMessage((ctx, message) => {
        processor(message);
      })
      .lambda();

    const response = await lambda(
      {
        Records: [
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: null, // This tests the ?? '' fallback
              Message: JSON.stringify({ message: 'test-message' }),
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
          {
            EventVersion: '1.0',
            EventSubscriptionArn: 'test-subscription',
            EventSource: 'aws:sns',
            Sns: {
              SignatureVersion: '1',
              Timestamp: '2023-01-01T00:00:00.000Z',
              Signature: 'test-signature',
              SigningCertUrl: 'test-cert-url',
              MessageId: undefined, // This tests the ?? uuid() fallback in ordering
              Message: JSON.stringify({ message: 'test-message-2' }),
              MessageAttributes: {},
              Type: 'Notification',
              UnsubscribeUrl: 'test-unsubscribe-url',
              TopicArn: 'test-topic-arn',
              Subject: null,
            },
          },
        ],
      } as any,
      {} as any,
    );

    expect(response).toBeUndefined();

    expect(processor).toHaveBeenCalledTimes(2);
    expect(processor).toHaveBeenCalledWith({ message: 'test-message' });
    expect(processor).toHaveBeenCalledWith({ message: 'test-message-2' });
  });
});

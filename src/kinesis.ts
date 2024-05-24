import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { KinesisStreamEvent, Context as AWSContext } from 'aws-lambda';
import {
  BaseContext,
  BaseHandlerConfig,
  PartialBatchResponse,
  handleUnprocessedRecords,
  processWithOrdering,
  withHealthCheckHandling,
} from './utils';

export type KinesisEventHandlerConfig<Event, Context> =
  BaseHandlerConfig<Context> & {
    /**
     * A function for parsing the Kinesis event data into your custom type.
     */
    parseEvent: (body: string) => Event;
  };

export type KinesisEventAction<Event, Context> = (
  context: Context & BaseContext,
  event: Event,
) => void | Promise<void>;

export type KinesisEventHandlerHarnessOptions<Event, Context> = {
  /**
   * A function for stringifying events.
   */
  stringifyEvent: (event: Event) => string;
  /**
   * An optional override for the logger.
   */
  logger?: LoggerInterface;

  /**
   * An optional override for creating the run context.
   */
  createRunContext?: () => Context | Promise<Context>;
};

export type KinesisEventHandlerHarnessContext<Event> = {
  /** Sends the specified event through the handler. */
  sendEvent: (event: { events: Event[] }) => Promise<PartialBatchResponse>;
};

/**
 * An abstraction for a Kinesis event handler.
 */
export class KinesisEventHandler<Event, Context> {
  private actions: KinesisEventAction<Event, Context>[] = [];

  constructor(readonly config: KinesisEventHandlerConfig<Event, Context>) {}

  /**
   * Adds a event action to the handler.
   *
   * @param handler The handler, for additional chaining.
   */
  onEvent(
    action: KinesisEventAction<Event, Context>,
  ): KinesisEventHandler<Event, Context> {
    this.actions.push(action);
    return this;
  }

  lambda(): (
    event: KinesisStreamEvent,
    context: AWSContext,
  ) => Promise<PartialBatchResponse> {
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
      const { unprocessedRecords } = await processWithOrdering(
        {
          items: event.Records,
          orderBy: (record) => record.kinesis.partitionKey,
          concurrency: this.config.concurrency ?? 5,
        },
        async (record) => {
          const eventLogger = context.logger.child({
            eventId: record.eventID,
          });

          const parsedEvent = this.config.parseEvent(
            Buffer.from(record.kinesis.data, 'base64').toString('utf8'),
          );

          for (const action of this.actions) {
            await action({ ...context, logger: eventLogger }, parsedEvent);
          }

          eventLogger.info('Successfully processed Kinesis record');
        },
      );

      // 3. Handle unprocessed records, if need be.
      return handleUnprocessedRecords({
        logger: context.logger,
        unprocessedRecords,
        usePartialBatchResponses: !!this.config.usePartialBatchResponses,
        getItemIdentifier: (record) => record.kinesis.sequenceNumber,
      });
    });
  }

  harness({
    stringifyEvent,
    ...overrides
  }: KinesisEventHandlerHarnessOptions<
    Event,
    Context
  >): KinesisEventHandlerHarnessContext<Event> {
    // Make a copy of the handler.
    let handler = new KinesisEventHandler({ ...this.config, ...overrides });
    for (const action of this.actions) {
      handler = handler.onEvent(action);
    }
    const lambda = handler.lambda();

    return {
      sendEvent: ({ events }) => {
        const event: KinesisStreamEvent = {
          // @ts-expect-error We don't need to mock every field on this event -- there are lots.
          Records: events.map((e) => ({
            eventID: uuid(),
            kinesis: {
              partitionKey: uuid(),
              data: Buffer.from(stringifyEvent(e)).toString('base64'),
            },
          })),
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

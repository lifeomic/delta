import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { KinesisStreamEvent, Context as AWSContext } from 'aws-lambda';
import {
  BaseContext,
  processWithOrdering,
  withHealthCheckHandling,
} from './utils';

export type KinesisEventHandlerConfig<Event, Context> = {
  /**
   * A logger to use in the context.
   */
  logger: LoggerInterface;
  /**
   * A function for parsing the Kinesis event data into your custom type.
   */
  parseEvent: (body: string) => Event;
  /**
   * Create a "context" for the lambda execution. (e.g. "data sources")
   */
  createRunContext: (base: BaseContext) => Context | Promise<Context>;
  /**
   * The maximum concurrency for processing events.
   *
   * @default 5
   */
  concurrency?: number;
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
  sendEvent: (event: { events: Event[] }) => Promise<void>;
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

  lambda(): (event: KinesisStreamEvent, context: AWSContext) => Promise<void> {
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
      const processingResult = await processWithOrdering(
        {
          items: event.Records,
          orderBy: (record) => record.kinesis.partitionKey,
          concurrency: this.config.concurrency ?? 5,
        },
        async (record) => {
          const eventLogger = context.logger.child({
            eventId: record.eventID,
          });

          const parsedEvent = this.config.parseEvent(record.kinesis.data);

          for (const action of this.actions) {
            await action({ ...context, logger: eventLogger }, parsedEvent);
          }

          eventLogger.info('Successfully processed Kinesis record');
        },
      );

      processingResult.throwOnUnprocessedRecords();
      context.logger.info('Successfully processed all Kinesis records');
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
      sendEvent: async ({ events }) => {
        const event: KinesisStreamEvent = {
          // We don't need to mock every field on this event -- there are lots.
          // @ts-expect-error
          Records: events.map((e) => ({
            eventID: uuid(),
            kinesis: {
              partitionKey: uuid(),
              data: stringifyEvent(e),
            },
          })),
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

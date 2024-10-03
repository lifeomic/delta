import { LoggerInterface } from '@lifeomic/logging';
import { Context } from 'aws-lambda';
import pMap from 'p-map';
import groupBy from 'lodash/groupBy';
import AggregateError from 'aggregate-error';

export type BaseContext = {
  logger: LoggerInterface;
  correlationId: string;
};

export type BaseHandlerConfig<Context> = {
  /**
   * A logger to use in the context.
   */
  logger: LoggerInterface;
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

  /**
   * Whether or not to use partial batch responses. If set to true, make
   * sure to also turn on partial batch responses when configuring your event
   * source mapping by specifying ReportBatchItemFailures for the
   * FunctionResponseTypes action. For more details see:
   * https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting
   */
  usePartialBatchResponses?: boolean;
};

export const withHealthCheckHandling =
  <Event, HandlerResponse>(
    handler: (event: Event, context: Context) => Promise<HandlerResponse>,
  ) =>
  (event: Event, context: Context): Promise<HandlerResponse> => {
    if ((event as any).httpMethod) {
      return {
        statusCode: 200,
        body: JSON.stringify({ healthy: true }),
      } as any;
    }

    if ((event as any).healthCheck) {
      return { healthy: true } as any;
    }

    return handler(event, context);
  };

export type PartialBatchResponse =
  | { batchItemFailures: { itemIdentifier: string }[] }
  | undefined;

export type ProcessWithOrderingParams<T> = {
  /**
   * The list of items to process.
   */
  items: T[];
  /**
   * A function for determining the "order" of an item. Should return a
   * string that will be used to order items lexicographically.
   */
  orderBy: (msg: T) => string;
  /**
   * The limit on the number of items to process concurrently at any time.
   */
  concurrency: number;
};

export type ProcessWithOrderingReturn<T> = {
  unprocessedRecords: { item: T; error?: any }[];
};

/**
 * A utility for performing parallel asynchronous processing of a
 * list of items, while also maintaining ordering.
 *
 * For example, in the case of DynamoDB streams:
 *
 * We want to maximize throughput, while also maintaining the ordering
 * guarantees from Dynamo.
 *
 * Dynamo guarantees that we will not receive events out-of-order for a
 * single item. But, it is possible that we will receive multiple events
 * for the same item in a single batch.
 *
 * So, we can handle events concurrently, but we need to ensure we never
 * handle multiple events for the same item at the same time. To prevent
 * that, we will:
 *  - Re-organize the list of events into a "list of lists", where each
 *    element corresponds to a single item.
 *  - Then, we'll process the lists in parallel.
 *
 * This same scenario is true for SQS FIFO queues, which will order messages
 * by MessageGroupId.
 *
 */
export const processWithOrdering = async <T>(
  params: ProcessWithOrderingParams<T>,
  process: (item: T) => Promise<void>,
): Promise<ProcessWithOrderingReturn<T>> => {
  const groupedItems = groupBy(params.items, params.orderBy);
  const groups = Object.values(groupedItems);
  const unsortedUnprocessedRecords: { item: T; error?: any }[] = [];

  await pMap(
    groups,
    async (group) => {
      for (let i = 0; i < group.length; i++) {
        const item = group[i];

        try {
          await process(item);
        } catch (error) {
          // Track the error on this item.
          unsortedUnprocessedRecords.push({ item, error });
          // Also, track any subsequent items in this group.
          unsortedUnprocessedRecords.push(
            ...group.slice(i + 1).map((item) => ({ item })),
          );
          return;
        }
      }
    },
    { concurrency: params.concurrency },
  );

  // Since unprocessed records can get tracked out-of-order, re-order
  // them according to the original order.
  const sortedUnprocessedRecords: { item: T; error?: any }[] = [];

  for (const item of params.items) {
    const unprocessed = unsortedUnprocessedRecords.find((i) => i.item === item);
    if (unprocessed) {
      sortedUnprocessedRecords.push(unprocessed);
    }
  }

  return { unprocessedRecords: sortedUnprocessedRecords };
};

export type HandleUnprocessedRecordsReturn =
  | { batchItemFailures: { itemIdentifier: string }[] }
  | undefined;

export const handleUnprocessedRecords = <Record>(params: {
  logger: LoggerInterface;
  unprocessedRecords: ProcessWithOrderingReturn<Record>['unprocessedRecords'];
  usePartialBatchResponses: boolean;
  getItemIdentifier: (record: Record) => string;
  redactRecord?: (record: Record) => any;
}): HandleUnprocessedRecordsReturn => {
  if (params.unprocessedRecords.length === 0) {
    params.logger.info('Successfully processed all records');
    return;
  }

  const batchItemFailures: { itemIdentifier: string }[] = [];
  const errors: any[] = [];
  // Even when not using partial batch responses, we still want to log all
  // the errors before throwing so we can easily correlate errors by the
  // logger correlation ID, request ID, and event ID.
  for (const { item, error } of params.unprocessedRecords) {
    const itemIdentifier = params.getItemIdentifier(item);
    batchItemFailures.push({ itemIdentifier });

    if (error) {
      errors.push(error);
      params.logger.error(
        {
          err: error,
          itemIdentifier,
          failedRecord: params.redactRecord ? params.redactRecord(item) : item,
          usePartialBatchResponses: params.usePartialBatchResponses,
        },
        'Failed to process record',
      );
    }
  }

  if (!params.usePartialBatchResponses) {
    throw new AggregateError(errors);
  }

  params.logger.info(
    { batchItemFailures },
    'Completing with partial batch response',
  );

  return { batchItemFailures };
};

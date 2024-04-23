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
) => {
  const groupedItems = groupBy(params.items, params.orderBy);
  const groupIds = Object.keys(groupedItems);
  const groups = Object.values(groupedItems);
  const unprocessedRecordsByGroupId: Record<
    string,
    {
      error: any;
      items: T[];
    }
  > = {};

  await pMap(
    groups,
    async (group, groupIndex) => {
      for (let i = 0; i < group.length; i++) {
        const item = group[i];

        try {
          await process(item);
        } catch (error) {
          // Keep track of all unprocessed items and stop processing the current
          // group as soon as we encounter the first error.
          unprocessedRecordsByGroupId[groupIds[groupIndex]] = {
            error,
            items: group.slice(i),
          };
          return;
        }
      }
    },
    {
      concurrency: params.concurrency,
    },
  );

  return {
    unprocessedRecordsByGroupId,
    throwOnUnprocessedRecords: () => {
      const aggregateErrors = Object.values(unprocessedRecordsByGroupId).map(
        (record) => record.error,
      );

      if (aggregateErrors.length) {
        throw new AggregateError(aggregateErrors);
      }
    },
  };
};

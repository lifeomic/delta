import { LoggerInterface } from '@lifeomic/logging';
import { Context } from 'aws-lambda';
import pMap from 'p-map';
import groupBy from 'lodash/groupBy';
import zipObject from 'lodash/zipObject';

export type BaseContext = {
  logger: LoggerInterface;
  correlationId: string;
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
  items: T[];
  orderBy: (msg: T) => string;
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
  const listIds = Object.keys(groupedItems);
  const lists = Object.values(groupedItems);
  const unprocessedRecordsByListId = zipObject<{ error: any; items: T[] }>(
    listIds,
    lists.map(() => ({ error: null, items: [] })),
  );

  await pMap(
    lists,
    async (list, listIndex) => {
      for (let i = 0; i < list.length; i++) {
        const item = list[i];

        try {
          await process(item);
        } catch (error) {
          // Keep track of all unprocessed items and stop processing the current
          // list as soon as we encounter the first error.
          unprocessedRecordsByListId[listIds[listIndex]] = {
            error,
            items: list.slice(i),
          };
          return;
        }
      }
    },
    {
      concurrency: params.concurrency,
    },
  );

  const aggregateErrors = Object.values(unprocessedRecordsByListId)
    .map((record) => record.error)
    .filter(Boolean)
    .flat();

  return {
    hasUnprocessedRecords: aggregateErrors.length > 0,
    unprocessedRecords: unprocessedRecordsByListId,
    throwOnUnprocessedRecords: () => {
      if (aggregateErrors.length) {
        throw new AggregateError(aggregateErrors);
      }
    },
  };
};

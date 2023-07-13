import { LoggerInterface } from '@lifeomic/logging';
import { Context } from 'aws-lambda';
import pMap from 'p-map';
import groupBy from 'lodash/groupBy';

export type BaseContext = {
  logger: LoggerInterface;
  correlationId: string;
};

export const withHealthCheckHandling =
  <Event>(handler: (event: Event, context: Context) => Promise<void>) =>
  (event: Event, context: Context): Promise<void> => {
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

const groupIntoLists = <T>(arr: T[], predicate: (item: T) => string): T[][] => {
  const grouped = groupBy(arr, (r) => predicate(r));
  const entries = Object.entries(grouped);
  return entries.map(([, items]) => items);
};

export type ProcessWithOrderingParams<T> = {
  items: T[];
  orderBy: (msg: T) => string;
  concurrency: number;
  stopOnError: boolean;
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
 */
export const processWithOrdering = async <T>(
  params: ProcessWithOrderingParams<T>,
  process: (item: T) => Promise<void>,
) => {
  const lists = groupIntoLists(params.items, params.orderBy);
  await pMap(
    lists,
    async (list) => {
      for (const item of list) {
        await process(item);
      }
    },
    { concurrency: params.concurrency, stopOnError: params.stopOnError },
  );
};

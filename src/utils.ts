import { Context } from 'aws-lambda';
import { LoggerInterface } from '@lifeomic/logging';

export type BaseContext = {
  logger: LoggerInterface;
  correlationId: string;
};

export const withHealthCheck =
  <Event, Return>(handler: (event: Event, context: Context) => Return) =>
  (event: Event, context: Context) => {
    if ((event as any).httpMethod) {
      return {
        statusCode: 200,
        body: JSON.stringify({ healthy: true }),
      } as unknown as void;
    }

    if ((event as any).healthCheck) {
      return { healthy: true } as any;
    }

    return handler(event, context);
  };

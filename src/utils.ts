import { LoggerInterface } from '@lifeomic/logging';
import { Context } from 'aws-lambda';

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

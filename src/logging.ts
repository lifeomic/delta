import Logger from 'bunyan';

export type LoggerInterface = Pick<
  Logger,
  'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal'
> & {
  child: (...args: Parameters<Logger['child']>) => LoggerInterface;
};

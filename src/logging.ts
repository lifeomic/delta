import Logger from 'bunyan';

export type LoggerInterface = Pick<
  Logger,
  'child' | 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal'
>;

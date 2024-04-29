import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import {
  DynamoDBStreamEvent,
  DynamoDBStreamHandler,
  DynamoDBRecord,
} from 'aws-lambda';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import {
  BaseContext,
  BaseHandlerConfig,
  processWithOrdering,
  withHealthCheckHandling,
} from './utils';

export type DynamoStreamHandlerConfig<Entity, Context> =
  BaseHandlerConfig<Context> & {
    /**
     * A function for parsing images from the stream into your custom type.
     *
     * The `object` parameter is an _already unmarshalled_ version of the Dynamo
     * record.
     */
    parse: (object: unknown) => Entity;

    /**
     * A listing of keys within a dynamo record's images to obfuscate in logging
     * output. This will not perform a deep obfuscation of 'M' AttributeValue
     * types and instead will simply obfuscate the entire value.
     */
    loggerObfuscateImageKeys?: string[];
  };

export type InsertAction<Entity, Context> = (
  ctx: Context & BaseContext,
  newEntity: Entity,
) => void | Promise<void>;

export type ModifyAction<Entity, Context> = (
  ctx: Context & BaseContext,
  oldEntity: Entity,
  newEntity: Entity,
) => void | Promise<void>;

export type RemoveAction<Entity, Context> = (
  ctx: Context & BaseContext,
  entity: Entity,
) => void | Promise<void>;

type InternalActionList<Entity, Context> = {
  insert: InsertAction<Entity, Context>[];
  modify: ModifyAction<Entity, Context>[];
  remove: RemoveAction<Entity, Context>[];
};

/* -- Test Harness Types -- */
export type DynamoStreamHandlerHarnessConfig<Context> = {
  /**
   * An optional override for the logger.
   */
  logger?: LoggerInterface;
  /**
   * An optional override for creating the run context.
   */
  createRunContext?: (base: BaseContext) => Context | Promise<Context>;
};

export type DynamoStreamHandlerHarnessContext<Entity> = {
  sendEvent: (event: TestEvent<Entity>) => Promise<void>;
};

export type TestRecord<Entity> =
  | { type: 'insert'; entity: Entity }
  | { type: 'modify'; oldEntity: Entity; newEntity: Entity }
  | { type: 'remove'; entity: Entity };

export type TestEvent<Entity> = {
  records: TestRecord<Entity>[];
};

/**
 * An abstraction for a DynamoDB stream handler.
 */
export class DynamoStreamHandler<Entity, Context> {
  /**
   * The set of actions to perform per event type.
   */
  private actions: InternalActionList<Entity, Context> = {
    insert: [],
    modify: [],
    remove: [],
  };

  constructor(readonly config: DynamoStreamHandlerConfig<Entity, Context>) {}

  /**
   * Creates a replica of the stream handler, using the provided configuration
   * overrides.
   */
  private withOverrides(
    overrides: Partial<
      Pick<
        DynamoStreamHandlerConfig<Entity, Context>,
        'createRunContext' | 'logger'
      >
    >,
  ): DynamoStreamHandler<Entity, Context> {
    const copy = new DynamoStreamHandler({
      parse: this.config.parse,
      logger: overrides.logger ?? this.config.logger,
      createRunContext:
        overrides.createRunContext ?? this.config.createRunContext,
    });

    for (const action of this.actions.insert) {
      copy.onInsert(action);
    }
    for (const action of this.actions.modify) {
      copy.onModify(action);
    }
    for (const action of this.actions.remove) {
      copy.onRemove(action);
    }

    return copy;
  }

  private obfuscate(blob: any, keys: string[]): any {
    if (blob === undefined) return undefined;
    const copy: any = { ...blob };
    keys.forEach((k) => {
      if (copy[k]) {
        copy[k] = { S: 'obfuscated' };
      }
    });
    return copy;
  }

  private obfuscateRecord(dynamoRecord: DynamoDBRecord): DynamoDBRecord {
    if (this.config.loggerObfuscateImageKeys && dynamoRecord.dynamodb) {
      return {
        ...dynamoRecord,
        dynamodb: {
          ...dynamoRecord.dynamodb,
          NewImage: this.obfuscate(
            dynamoRecord.dynamodb.NewImage,
            this.config.loggerObfuscateImageKeys,
          ),
          OldImage: this.obfuscate(
            dynamoRecord.dynamodb.OldImage,
            this.config.loggerObfuscateImageKeys,
          ),
        },
      };
    }
    return dynamoRecord;
  }

  private obfuscateEvent(
    dynamoEvent: DynamoDBStreamEvent,
  ): DynamoDBStreamEvent {
    return {
      Records: dynamoEvent.Records.map((r) => this.obfuscateRecord(r)),
    };
  }

  /**
   * Adds an "INSERT" event handler.
   */
  onInsert(
    action: InsertAction<Entity, Context>,
  ): DynamoStreamHandler<Entity, Context> {
    this.actions.insert.push(action);
    return this;
  }

  /**
   * Adds a "MODIFY" event handler.
   */
  onModify(
    action: ModifyAction<Entity, Context>,
  ): DynamoStreamHandler<Entity, Context> {
    this.actions.modify.push(action);
    return this;
  }

  /**
   * Adds a "REMOVE" event handler.
   */
  onRemove(
    action: RemoveAction<Entity, Context>,
  ): DynamoStreamHandler<Entity, Context> {
    this.actions.remove.push(action);
    return this;
  }

  /**
   * Returns a DynamoDB stream lambda handler that will perform the configured
   * actions.
   */
  lambda(): DynamoDBStreamHandler {
    return withHealthCheckHandling(async (event, ctx) => {
      const correlationId = uuid();

      const base: BaseContext = {
        correlationId,
        logger: this.config.logger.child({
          requestID: ctx.awsRequestId,
          correlationId,
        }),
      };

      const context: BaseContext & Context = {
        ...(await this.config.createRunContext(base)),
        ...base,
      };

      context.logger.info(
        { event: this.obfuscateEvent(event) },
        'Processing DynamoDB stream event',
      );

      const processingResult = await processWithOrdering(
        {
          items: event.Records,
          orderBy: (record) => {
            const KeyObject = record.dynamodb?.Keys;

            // This scenario should only ever happen in tests.
            if (!KeyObject) {
              return uuid();
            }

            // We need to order by key -- so, just stringify the key.
            //
            // But, add custom logic to ensure that the key object is stringified
            // deterministically, regardless of the order of its keys. (e.g. we
            // should stringify { a: 1, b: 2 } and { b: 2, a: 1 } to the same string)
            //
            // It's possible that AWS already ensures that the keys are deterministically
            // ordered, and therefore we don't need to do this. But we add this logic just
            // to be extra sure.
            return JSON.stringify(
              Object.keys(KeyObject)
                .sort()
                .map((key) => [key, KeyObject[key]]),
            );
          },
          concurrency: this.config.concurrency ?? 5,
        },
        async (record) => {
          const recordLogger = context.logger.child({
            record: this.obfuscateRecord(record),
          });
          if (!record.dynamodb) {
            recordLogger.error(
              'The dynamodb property was not present on event',
            );
            return;
          }

          // Unmarshall the entities.
          const oldEntity =
            record.dynamodb.OldImage &&
            // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
            this.config.parse(unmarshall(record.dynamodb.OldImage as any));

          const newEntity =
            record.dynamodb.NewImage &&
            // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
            this.config.parse(unmarshall(record.dynamodb.NewImage as any));

          // Handle INSERT events -- invoke the INSERT actions in order.
          if (record.eventName === 'INSERT') {
            if (!newEntity) {
              recordLogger.error('No NewImage was defined for an INSERT event');
              return;
            }

            for (const action of this.actions.insert) {
              await action({ ...context, logger: recordLogger }, newEntity);
            }
          }
          // Handle MODIFY events -- invoke the MODIFY actions in order.
          else if (record.eventName === 'MODIFY') {
            if (!oldEntity) {
              recordLogger.error('No OldImage was defined for a MODIFY event');
              return;
            }
            if (!newEntity) {
              recordLogger.error('No NewImage was defined for a MODIFY event');
              return;
            }

            for (const action of this.actions.modify) {
              await action(
                { ...context, logger: recordLogger },
                oldEntity,
                newEntity,
              );
            }
          }
          // Handle REMOVE events -- invoke the REMOVE actions in order.
          else if (record.eventName === 'REMOVE') {
            if (!oldEntity) {
              recordLogger.error('No OldImage was defined for a REMOVE event');
              return;
            }

            for (const action of this.actions.remove) {
              await action({ ...context, logger: recordLogger }, oldEntity);
            }
          }
        },
      );

      processingResult.throwOnUnprocessedRecords();
      context.logger.info('Successfully processed all DynamoDB stream records');
    });
  }

  /**
   * Returns a test harness for exercising the handler, with an optional
   * overridden context.
   */
  harness(
    options?: DynamoStreamHandlerHarnessConfig<Context>,
  ): DynamoStreamHandlerHarnessContext<Entity> {
    const lambda = this.withOverrides(options ?? {}).lambda();

    return {
      sendEvent: async (event) => {
        const dynamoEvent: DynamoDBStreamEvent = {
          Records: event.records.map<DynamoDBStreamEvent['Records'][number]>(
            (record) => {
              switch (record.type) {
                case 'insert':
                  return {
                    eventName: 'INSERT',
                    dynamodb: {
                      NewImage: marshall(record.entity) as any,
                    },
                  };
                case 'modify':
                  return {
                    eventName: 'MODIFY',
                    dynamodb: {
                      OldImage: marshall(record.oldEntity) as any,
                      NewImage: marshall(record.newEntity) as any,
                    },
                  };
                case 'remove':
                  return {
                    eventName: 'REMOVE',
                    dynamodb: {
                      OldImage: marshall(record.entity) as any,
                    },
                  };
              }
            },
          ),
        };

        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        await lambda(dynamoEvent, {} as any, null as any);
      },
    };
  }
}

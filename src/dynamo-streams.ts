import { LoggerInterface } from '@lifeomic/logging';
import { v4 as uuid } from 'uuid';
import { DynamoDBStreamEvent, DynamoDBStreamHandler } from 'aws-lambda';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { BaseContext, withHealthCheckHandling } from './utils';

export type DynamoStreamHandlerConfig<Entity, Context> = {
  /**
   * A logger to use in the context.
   */
  logger: LoggerInterface;
  /**
   * A function for parsing images from the stream into your custom type.
   *
   * The `object` parameter is an _already unmarshalled_ version of the Dynamo
   * record.
   */
  parse: (object: unknown) => Entity;
  /**
   * Create a "context" for the lambda execution. (e.g. "data sources")
   */
  createRunContext: (base: BaseContext) => Context | Promise<Context>;
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

      context.logger.info({ event }, 'Processing DynamoDB stream event');

      // Iterate through every event.
      for (const record of event.Records) {
        const recordLogger = this.config.logger.child({ record });
        if (!record.dynamodb) {
          recordLogger.error(
            { record },
            'The dynamodb property was not present on event',
          );
          continue;
        }

        // Unmarshall the entities.
        const oldEntity =
          record.dynamodb.OldImage &&
          this.config.parse(unmarshall(record.dynamodb.OldImage));

        const newEntity =
          record.dynamodb.NewImage &&
          this.config.parse(unmarshall(record.dynamodb.NewImage));

        // Handle INSERT events -- invoke the INSERT actions in order.
        if (record.eventName === 'INSERT') {
          if (!newEntity) {
            recordLogger.error(
              { record },
              'No NewImage was defined for an INSERT event',
            );
            continue;
          }

          for (const action of this.actions.insert) {
            await action({ ...context, logger: recordLogger }, newEntity);
          }
        }
        // Handle MODIFY events -- invoke the MODIFY actions in order.
        else if (record.eventName === 'MODIFY') {
          if (!oldEntity) {
            recordLogger.error(
              { record },
              'No OldImage was defined for a MODIFY event',
            );
            continue;
          }
          if (!newEntity) {
            recordLogger.error(
              { record },
              'No NewImage was defined for a MODIFY event',
            );
            continue;
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
            recordLogger.error(
              { record },
              'No OldImage was defined for a REMOVE event',
            );
            continue;
          }

          for (const action of this.actions.remove) {
            await action({ ...context, logger: recordLogger }, oldEntity);
          }
        }
      }
    });
  }

  /**
   * Returns a test harness for exercising the handler, with an optional
   * overriden context.
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
                      NewImage: marshall(record.entity),
                    },
                  };
                case 'modify':
                  return {
                    eventName: 'MODIFY',
                    dynamodb: {
                      OldImage: marshall(record.oldEntity),
                      NewImage: marshall(record.newEntity),
                    },
                  };
                case 'remove':
                  return {
                    eventName: 'REMOVE',
                    dynamodb: {
                      OldImage: marshall(record.entity),
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

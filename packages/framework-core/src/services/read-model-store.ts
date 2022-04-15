/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  BoosterConfig,
  ReadModelInterface,
  EventEnvelope,
  ProjectionMetadata,
  UUID,
  EntityInterface,
  ReadModelAction,
  OptimisticConcurrencyUnexpectedVersionError,
  SequenceKey,
} from '@boostercloud/framework-types'
import { Promises, retryIfError, createInstance, getLogger } from '@boostercloud/framework-common-helpers'

export class ReadModelStore {
  public constructor(readonly config: BoosterConfig) {}

  public async project(entitySnapshotEnvelope: EventEnvelope): Promise<void> {
    const logger = getLogger(this.config, 'ReadModelStore#project')
    const projections = this.config.projections[entitySnapshotEnvelope.entityTypeName]
    if (!projections) {
      logger.debug(
        `[ReadModelStore#project] No projections found for entity ${entitySnapshotEnvelope.entityTypeName}. Skipping...`
      )
      return
    }
    const entityMetadata = this.config.entities[entitySnapshotEnvelope.entityTypeName]
    await Promises.allSettledAndFulfilled(
      projections.map(async (projectionMetadata: ProjectionMetadata) => {
        const readModelName = projectionMetadata.class.name
        const entityInstance = createInstance(entityMetadata.class, entitySnapshotEnvelope.value)
        const readModelID = this.joinKeyForProjection(entityInstance, projectionMetadata)
        const sequenceKey = this.sequenceKeyForProjection(entityInstance, projectionMetadata)
        if (!readModelID) {
          logger.warn(
            `Couldn't find the joinKey named ${projectionMetadata.joinKey} in entity snapshot of ${entityMetadata.class.name}. Skipping...`
          )
          return
        }
        logger.debug(
          '[ReadModelStore#project] Projecting entity snapshot ',
          entitySnapshotEnvelope,
          ` to build new state of read model ${readModelName} with ID ${readModelID}`,
          sequenceKey ? ` sequencing by ${sequenceKey.name} with value ${sequenceKey.value}` : ''
        )

        return await retryIfError(
          () =>
            this.applyProjectionToReadModel(
              entityInstance,
              projectionMetadata,
              readModelName,
              readModelID,
              sequenceKey
            ),
          OptimisticConcurrencyUnexpectedVersionError
        )
      })
    )
  }

  private joinKeyForProjection(entity: EntityInterface, projectionMetadata: ProjectionMetadata): UUID {
    return (entity as any)[projectionMetadata.joinKey]
  }

  private sequenceKeyForProjection(
    entity: EntityInterface,
    projectionMetadata: ProjectionMetadata
  ): SequenceKey | undefined {
    const sequenceKeyName = this.config.readModelSequenceKeys[projectionMetadata.class.name]
    const sequenceKeyValue = (entity as any)[sequenceKeyName]
    if (sequenceKeyName && sequenceKeyValue) {
      return { name: sequenceKeyName, value: sequenceKeyValue }
    }
    return undefined
  }

  private async applyProjectionToReadModel(
    entity: EntityInterface,
    projectionMetadata: ProjectionMetadata,
    readModelName: string,
    readModelID: UUID,
    sequenceKey?: SequenceKey
  ): Promise<unknown> {
    const logger = getLogger(this.config, 'ReadModelStore#applyProjectionToReadModel')
    const readModel = await this.fetchReadModel(readModelName, readModelID, sequenceKey)
    const currentReadModelVersion: number = readModel?.boosterMetadata?.version ?? 0
    const newReadModel = this.projectionFunction(projectionMetadata)(entity, readModel)

    if (newReadModel === ReadModelAction.Delete) {
      logger.debug(`Deleting read model ${readModelName} with ID ${readModelID}:`, readModel)
      return this.config.provider.readModels.delete(this.config, readModelName, readModel)
    } else if (newReadModel === ReadModelAction.Nothing) {
      logger.debug(
        `[ReadModelStore#project] Skipping actions for ${readModelName} with ID ${readModelID}:`,
        newReadModel
      )
      return
    }
    // Increment the read model version in 1 before storing
    newReadModel.boosterMetadata = {
      ...readModel?.boosterMetadata,
      version: currentReadModelVersion + 1,
    }
    logger.debug(
      `[ReadModelStore#project] Storing new version of read model ${readModelName} with ID ${readModelID}:`,
      newReadModel
    )

    return this.config.provider.readModels.store(this.config, readModelName, newReadModel, currentReadModelVersion)
  }

  /**
   * Gets a specific read model instance referencing it by ID when it's a regular read model
   * or by ID + sequenceKey when it's a sequenced read model
   */
  public async fetchReadModel(
    readModelName: string,
    readModelID: UUID,
    sequenceKey?: SequenceKey
  ): Promise<ReadModelInterface | undefined> {
    const logger = getLogger(this.config, 'ReadModelStore#fetchReadModel')
    logger.debug(
      `[ReadModelStore#fetchReadModel] Looking for existing version of read model ${readModelName} with ID = ${readModelID}` +
        (sequenceKey ? ` and sequence key ${sequenceKey.name} = ${sequenceKey.value}` : '')
    )
    const rawReadModels = await this.config.provider.readModels.fetch(
      this.config,
      readModelName,
      readModelID,
      sequenceKey
    )
    if (rawReadModels?.length) {
      if (rawReadModels.length > 1) {
        throw 'Got multiple objects for a request by Id. If this is a sequenced read model you should also specify the sequenceKey field.'
      } else if (rawReadModels.length === 1 && rawReadModels[0]) {
        const readModelMetadata = this.config.readModels[readModelName]
        return createInstance(readModelMetadata.class, rawReadModels[0])
      }
    }
    return undefined
  }

  // eslint-disable-next-line @typescript-eslint/ban-types
  public projectionFunction(projectionMetadata: ProjectionMetadata): Function {
    try {
      return (projectionMetadata.class as any)[projectionMetadata.methodName]
    } catch {
      throw new Error(`Couldn't load the ReadModel class ${projectionMetadata.class.name}`)
    }
  }
}

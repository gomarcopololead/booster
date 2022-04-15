import { BoosterConfig, UUID, EventEnvelope, InvalidParameterError } from '@boostercloud/framework-types'
import { createInstance, getLogger } from '@boostercloud/framework-common-helpers'

const originOfTime = new Date(0).toISOString() // Unix epoch

export class EventStore {
  public constructor(readonly config: BoosterConfig) {}

  public async fetchEntitySnapshot(entityName: string, entityID: UUID): Promise<EventEnvelope | null> {
    const logger = getLogger(this.config, 'EventStore#fetchEntitySnapshot')
    logger.debug(`Fetching snapshot for entity ${entityName} with ID ${entityID}`)
    const latestSnapshotEnvelope = await this.loadLatestSnapshot(entityName, entityID)

    // eslint-disable-next-line @typescript-eslint/no-extra-parens
    const lastVisitedTime = latestSnapshotEnvelope?.snapshottedEventCreatedAt ?? originOfTime
    const pendingEvents = await this.loadEventStreamSince(entityName, entityID, lastVisitedTime)

    if (pendingEvents.length <= 0) {
      return latestSnapshotEnvelope
    } else {
      logger.debug(`Looking for the reducer for entity ${entityName} with ID ${entityID}`)
      const newEntitySnapshot = pendingEvents.reduce(this.entityReducer.bind(this), latestSnapshotEnvelope)
      logger.debug(`Reduced new snapshot for entity ${entityName} with ID ${entityID}: `, newEntitySnapshot)

      return newEntitySnapshot
    }
  }

  public async calculateAndStoreEntitySnapshot(
    entityName: string,
    entityID: UUID,
    pendingEnvelopes: Array<EventEnvelope>
  ): Promise<EventEnvelope | null> {
    const logger = getLogger(this.config, 'EventStore#calculateAndStoreEntitySnapshot')
    logger.debug('Processing events: ', pendingEnvelopes)
    logger.debug(`Fetching snapshot for entity ${entityName} with ID ${entityID}`)
    const latestSnapshotEnvelope = await this.loadLatestSnapshot(entityName, entityID)

    logger.debug(`Looking for the reducer for entity ${entityName} with ID ${entityID}`)
    const newEntitySnapshot = pendingEnvelopes.reduce(this.entityReducer.bind(this), latestSnapshotEnvelope)
    logger.debug(`Reduced new snapshot for entity ${entityName} with ID ${entityID}: `, newEntitySnapshot)

    if (!newEntitySnapshot) {
      logger.debug('New entity snapshot is null. Returning old one (which can also be null)')
      return latestSnapshotEnvelope
    }

    await this.storeSnapshot(newEntitySnapshot)

    return newEntitySnapshot
  }

  private async storeSnapshot(snapshot: EventEnvelope): Promise<void> {
    const logger = getLogger(this.config, 'EventStore#storeSnapshot')
    logger.debug('Storing snapshot in the event store:', snapshot)
    return this.config.provider.events.store([snapshot], this.config)
  }

  private loadLatestSnapshot(entityName: string, entityID: UUID): Promise<EventEnvelope | null> {
    const logger = getLogger(this.config, 'EventStore#loadLatestSnapshot')
    logger.debug(`Loading latest snapshot for entity ${entityName} and ID ${entityID}`)
    return this.config.provider.events.latestEntitySnapshot(this.config, entityName, entityID)
  }

  private loadEventStreamSince(entityTypeName: string, entityID: UUID, timestamp: string): Promise<EventEnvelope[]> {
    const logger = getLogger(this.config, 'EventStore#loadEventStreamSince')
    logger.debug(`Loading list of pending events for entity ${entityTypeName} with ID ${entityID} since ${timestamp}`)
    return this.config.provider.events.forEntitySince(this.config, entityTypeName, entityID, timestamp)
  }

  private entityReducer(latestSnapshot: EventEnvelope | null, eventEnvelope: EventEnvelope): EventEnvelope {
    const logger = getLogger(this.config, 'EventStore#entityReducer')
    try {
      logger.debug('Calling reducer with event: ', eventEnvelope, ' and entity snapshot ', latestSnapshot)
      const eventMetadata = this.config.events[eventEnvelope.typeName]
      const eventInstance = createInstance(eventMetadata.class, eventEnvelope.value)
      const entityMetadata = this.config.entities[eventEnvelope.entityTypeName]
      const snapshotInstance = latestSnapshot ? createInstance(entityMetadata.class, latestSnapshot.value) : null
      const newEntity = this.reducerForEvent(eventEnvelope.typeName)(eventInstance, snapshotInstance)
      const newSnapshot: EventEnvelope = {
        version: this.config.currentVersionFor(eventEnvelope.entityTypeName),
        kind: 'snapshot',
        requestID: eventEnvelope.requestID,
        entityID: eventEnvelope.entityID,
        entityTypeName: eventEnvelope.entityTypeName,
        typeName: eventEnvelope.entityTypeName,
        value: newEntity,
        createdAt: new Date().toISOString(), // TODO: This could be overridden by the provider. We should not set it. Ensure all providers set it
        snapshottedEventCreatedAt: eventEnvelope.createdAt,
      }
      logger.debug('Reducer result: ', newSnapshot)
      return newSnapshot
    } catch (e) {
      logger.error('Error when calling reducer', e)
      throw e
    }
  }

  // eslint-disable-next-line @typescript-eslint/ban-types
  private reducerForEvent(eventName: string): Function {
    const logger = getLogger(this.config, 'EventStore#reducerForEvent')
    const reducerMetadata = this.config.reducers[eventName]
    if (!reducerMetadata) {
      throw new InvalidParameterError(`No reducer registered for event ${eventName}`)
    } else {
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const reducer = (reducerMetadata.class as any)[reducerMetadata.methodName]
        logger.debug(
          `Found reducer for event ${eventName}: "${reducerMetadata.class.name}.${reducerMetadata.methodName}"`
        )
        return reducer
      } catch {
        throw new Error(`Couldn't load the Entity class ${reducerMetadata.class.name}`)
      }
    }
  }
}

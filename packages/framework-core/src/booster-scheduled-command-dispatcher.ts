import {
  BoosterConfig,
  ScheduledCommandEnvelope,
  Register,
  NotFoundError,
  ScheduledCommandInterface,
} from '@boostercloud/framework-types'
import { getLogger } from '@boostercloud/framework-common-helpers'
import { RegisterHandler } from './booster-register-handler'

export class BoosterScheduledCommandDispatcher {
  public constructor(readonly config: BoosterConfig) {}

  public async dispatchCommand(commandEnvelope: ScheduledCommandEnvelope): Promise<void> {
    const logger = getLogger(this.config, 'BoosterScheduledCommandDispatcher#dispatchCommand')
    logger.debug('Dispatching the following scheduled command envelope: ', commandEnvelope)

    const commandMetadata = this.config.scheduledCommandHandlers[commandEnvelope.typeName]
    if (!commandMetadata) {
      throw new NotFoundError(`Could not find a proper handler for ${commandEnvelope.typeName}`)
    }

    const commandClass = commandMetadata.class
    logger.debug('Found the following command:', commandClass.name)
    const command = commandClass as ScheduledCommandInterface
    const register = new Register(commandEnvelope.requestID, undefined, commandEnvelope.context)
    logger.debug('Calling "handle" method on command: ', command)
    await command.handle(register)
    logger.debug('Command dispatched with register: ', register)
    await RegisterHandler.handle(this.config, register)
  }
  /**
   * Entry point to dispatch events coming from the cloud provider.
   * @param request request from the cloud provider
   * @param logger
   */
  // eslint-disable-next-line
  public async dispatch(request: any): Promise<void> {
    const logger = getLogger(this.config, 'BoosterScheduledCommandDispatcher#dispatch')
    const envelopeOrError = await this.config.provider.scheduled.rawToEnvelope(this.config, request)
    logger.debug('Received ScheduledCommand envelope...', envelopeOrError)

    await this.dispatchCommand(envelopeOrError)
  }
}

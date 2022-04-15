import {
  BoosterConfig,
  CommandEnvelope,
  Register,
  InvalidParameterError,
  NotAuthorizedError,
  NotFoundError,
} from '@boostercloud/framework-types'
import { BoosterAuth } from './booster-auth'
import { RegisterHandler } from './booster-register-handler'
import { createInstance, getLogger } from '@boostercloud/framework-common-helpers'
import { applyBeforeFunctions } from './services/filter-helpers'

export class BoosterCommandDispatcher {
  public constructor(readonly config: BoosterConfig) {}

  public async dispatchCommand(commandEnvelope: CommandEnvelope): Promise<unknown> {
    const logger = getLogger(this.config, 'BoosterCommandDispatcher#dispatchCommand')
    logger.debug('Dispatching the following command envelope: ', commandEnvelope)
    if (!commandEnvelope.version) {
      throw new InvalidParameterError('The required command "version" was not present')
    }

    const commandMetadata = this.config.commandHandlers[commandEnvelope.typeName]
    if (!commandMetadata) {
      throw new NotFoundError(`Could not find a proper handler for ${commandEnvelope.typeName}`)
    }

    if (!BoosterAuth.isUserAuthorized(commandMetadata.authorizedRoles, commandEnvelope.currentUser)) {
      throw new NotAuthorizedError(`Access denied for command '${commandEnvelope.typeName}'`)
    }

    const commandClass = commandMetadata.class
    logger.debug('Found the following command:', commandClass.name)

    const commandInput = await applyBeforeFunctions(
      commandEnvelope.value,
      commandMetadata.before,
      commandEnvelope.currentUser
    )

    const commandInstance = createInstance(commandClass, commandInput)

    const register = new Register(commandEnvelope.requestID, commandEnvelope.currentUser, commandEnvelope.context)
    logger.debug('Calling "handle" method on command: ', commandClass)
    const result = await commandClass.handle(commandInstance, register)
    logger.debug('Command dispatched with register: ', register)
    await RegisterHandler.handle(this.config, register)
    return result
  }
}

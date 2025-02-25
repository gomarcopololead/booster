import { ReadModelInterface } from '.'
import { UserEnvelope, ReadModelRequestEnvelope } from '../envelope'
import { CommandInput } from './command'

export interface CommandFilterHooks {
  readonly before?: Array<CommandBeforeFunction>
}

export type CommandBeforeFunction = (input: CommandInput, currentUser?: UserEnvelope) => Promise<CommandInput>

export interface ReadModelFilterHooks {
  readonly before?: Array<ReadModelBeforeFunction>
}

export type ReadModelBeforeFunction = (
  readModelRequestEnvelope: ReadModelRequestEnvelope<ReadModelInterface>,
  currentUser?: UserEnvelope
) => Promise<ReadModelRequestEnvelope<ReadModelInterface>>

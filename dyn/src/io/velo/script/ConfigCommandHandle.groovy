package io.velo.script

import io.velo.command.CGroup
import io.velo.command.ConfigCommand

def cGroup = super.binding.getProperty('cGroup') as CGroup

def configCommand = new ConfigCommand(cGroup)
configCommand.from(cGroup)
configCommand.handle()
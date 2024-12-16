package io.velo.script

import io.velo.command.CGroup
import io.velo.command.CommandCommand

def cGroup = super.binding.getProperty('cGroup') as CGroup

def configCommand = new CommandCommand(cGroup)
configCommand.from(cGroup)
configCommand.handle()
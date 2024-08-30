package io.velo.script

import io.velo.command.IGroup
import io.velo.command.InfoCommand

def iGroup = super.binding.getProperty('iGroup') as IGroup

def infoCommand = new InfoCommand(iGroup)
infoCommand.from(iGroup)
infoCommand.handle()
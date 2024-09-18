package io.velo.script

import io.velo.command.EGroup
import io.velo.command.ExtendCommand

def eGroup = super.binding.getProperty('eGroup') as EGroup

def extendCommand = new ExtendCommand(eGroup)
extendCommand.from(eGroup)
extendCommand.handle()

package io.velo.script

import io.velo.command.SGroup
import io.velo.command.SentinelCommand

def sGroup = super.binding.getProperty('sGroup') as SGroup

def sentinelCommand = new SentinelCommand(sGroup)
sentinelCommand.from(sGroup)
sentinelCommand.handle()
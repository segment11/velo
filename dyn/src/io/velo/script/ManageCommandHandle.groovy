package io.velo.script

import io.velo.command.MGroup
import io.velo.command.ManageCommand

def mGroup = super.binding.getProperty('mGroup') as MGroup

def manageCommand = new ManageCommand(mGroup)
manageCommand.from(mGroup)
manageCommand.handle()
package io.velo.script

import io.velo.command.CGroup
import io.velo.command.ClusterxCommand

def cGroup = super.binding.getProperty('cGroup') as CGroup

def clusterxCommand = new ClusterxCommand(cGroup)
clusterxCommand.from(cGroup)
clusterxCommand.handle()
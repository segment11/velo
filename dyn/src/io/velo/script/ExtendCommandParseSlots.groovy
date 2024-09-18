package io.velo.script

import io.velo.command.ExtendCommand

def cmd = super.binding.getProperty('cmd') as String
def data = super.binding.getProperty('data') as byte[][]
def slotNumber = super.binding.getProperty('slotNumber') as int

new ExtendCommand().parseSlots(cmd, data, slotNumber)
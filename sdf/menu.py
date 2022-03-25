import os
import sys

from cliff.command import Command
from cliff.commandmanager import CommandManager

from cliff.show import ShowOne

import logging

class Select(ShowOne):
    'display details of journal with given title'

    log = logging.getLogger(__name__)

    def take_action(self, parsed_args):
        return (('Name', 'Size'),
                ((n, os.stat(n).st_size) for n in os.listdir('.'))
                )

class Menu(CommandManager):
    "A Manager class to register sub commands"
    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Menu,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ Select, ]:
            self.add_command( cmd.__name__.lower(), cmd )



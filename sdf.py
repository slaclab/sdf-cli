#!./bin/python3

import sys

from cliff.app import App
from cliff import help, complete
from cliff.commandmanager import CommandManager
from cliff.command import Command
from commands.menu import Menu

import logging
# logging.basicConfig( level=logging.WARNING, format='%(name)s:%(module)s (%(lineno)d): %(levelname)-5s: %(message)s' )


class MultiApp(App):
    """
    An App with many command_managers. the concept is that we can have a deeper hierachy of commands.
    """

    command_managers = {}
    
    def __init__(self, description, version, command_managers=[],
            stdin=None, stdout=None, stderr=None,
            deferred_help=False, convert_underscores=True):

        self.command_manager = CommandManager('root')
        self.command_manager.add_command('help', help.HelpCommand)
        # this probably doesn't work
        self.command_manager.add_command('complete', complete.CompleteCommand)
        self._set_streams(stdin, stdout, stderr)
        self.deferred_help = deferred_help
        self.parser = self.build_option_parser(description, version)
        self.interactive_mode = False
        self.interpreter = None
        self.convert_underscores = convert_underscores

        if len(command_managers) == 0:
            raise Exception("One or more CommandManager()'s needed.")
        
        for m in command_managers:
            self.command_managers[m.__name__.lower()] = m
    
    def help(self):
        action = help.HelpAction(None, None, default=self)
        action(self.parser, self.options, None, None)
        
    # disable interactive mode
    def interact(self):
        # dummy add the command managers as commands
        for k,v in self.command_managers.items():
            this = type(k, (Command,), { "__doc__": v.__doc__, "take_action": lambda self: None })
            self.command_manager.add_command( k, this )
        self.help()

    # we cheat by basically substituting in the correct command_manager into self before we run a command
    def run_subcommand(self, argv):
        manager = argv.pop(0)
        self.command_manager = self.command_managers[manager](manager,convert_underscores=self.convert_underscores)
        # if no argv left, then print help
        if len(argv) == 0:
            self.help()

        super(MultiApp, self).run_subcommand( argv )
        self.LOG.debug("FIN")

    

def main(argv=sys.argv[1:]):
    app = MultiApp( description="S3DF Command Line Tools", version=1.0, command_managers=[ Menu, ])
    return app.run(argv)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

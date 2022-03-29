#!./bin/python3

import sys

from cliff.app import App
from cliff.commandmanager import CommandManager
from cliff.command import Command
from cliff.help import HelpAction, HelpCommand

from commands.menu import Menu
from commands.repo import Repo
from commands.user import User

import inspect

import logging
import autopage.argparse

LOG = logging.getLogger()

LOG.setLevel(logging.DEBUG)

class ManagerCommand(Command):

    LOG = None

    def __init__(self, app, app_args, cmd_name=None):
        super( ManagerCommand, self).__init__(app, app_args, cmd_name=cmd_name)
        self.LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(ManagerCommand, self).get_parser(prog_name)
        parser.add_argument('cmd',
                            nargs='*',
                            help='name of the command',
                            )
        return parser

    def dump(self,cmd,command_manager,subcommand=''):
        try:
            the_cmd = command_manager.find_command(
                cmd,
            )
            cmd_factory, cmd_name, search_args = the_cmd
        except ValueError:
            # Did not find an exact match
            cmd = cmd[0]
            fuzzy_matches = [k[0] for k in command_manager
                             if k[0].startswith(cmd)
                             ]
            if not fuzzy_matches:
                raise
            self.app.stdout.write('Command "%s" matches:\n' % cmd)
            for fm in sorted(fuzzy_matches):
                self.app.stdout.write('  %s\n' % fm)
            return
        self.app_args.cmd = search_args
        kwargs = {}
        if 'cmd_name' in inspect.getfullargspec(cmd_factory.__init__).args:
            kwargs['cmd_name'] = cmd_name
        cmd = cmd_factory(self.app, self.app_args, **kwargs)
        full_name = (cmd_name
                     if self.app.interactive_mode
                     else ' '.join([self.app.NAME, subcommand, cmd_name])
                     )
        cmd_parser = cmd.get_parser(full_name)
        pager = autopage.argparse.help_pager(self.app.stdout)
        with pager as out:
            autopage.argparse.use_color_for_parser(cmd_parser,
                                                   pager.to_terminal())
            cmd_parser.print_help(out)
        

    def take_action(self, parsed_args):
        # note that the first verb help in parsed_args has already been taken out

        # if we have 'help manager command', then we just need to use the correct manager
        if parsed_args.cmd and len(parsed_args.cmd) == 2:

            # strip the ManagerCommand
            try:
                n = parsed_args.cmd.pop(0)
                manager = self.app.command_managers[n]
                # self.app.LOG.warning( f"using manager {parsed_args.cmd} -> {manager}" )
                # self.dump(parsed_args.cmd,manager)
                self.dump(parsed_args.cmd,manager, subcommand=n)

            except Exception as e:
                raise e
            
        # if we just have 'help manager', then we need to list the managers children as positionals
        else:

            n = parsed_args.cmd[0]

            # lazy; create temp app with this command manager
            cm = self.app.command_managers[n] 
            v = self.app.version
            # we redefine to remove unwanted initilisations (like 'complete' command) and redefine the HelCommand
            class ThisApp(App):
                def __init__(self):
                    self.command_manager = cm
                    self.command_manager.add_command('help', HelpCommand)
                    self._set_streams(None, None, None)
                    self.interactive_app_factory = None
                    self.deferred_help = False
                    self.parser = self.build_option_parser(cm.__doc__, v)
                    self.interactive_mode = False
                    self.interpreter = None
            sub = ThisApp()
            sub.parser.prog = f"{sub.parser.prog} {n}" 
            action = HelpAction(None, None, default=sub)
            options = sub.parser.parse_known_args()
            action(sub.parser, options, None, None)

        return 0



class MultiApp(App):

    command_managers = {}

    def __init__(self, description, version, command_managers=[],
                 stdin=None, stdout=None, stderr=None,
                 interactive_app_factory=None,
                 deferred_help=True):

        # coloredlogs.install(level='DEBUG',logger=logging.getLogger())
        
        self._set_streams(stdin, stdout, stderr)
        self.interactive_app_factory = interactive_app_factory
        self.deferred_help = deferred_help
        self.interactive_mode = False
        self.interpreter = None

        # dummy root command manager to get the help working
        self.command_manager = CommandManager('root')
        self.command_manager.add_command('help', HelpCommand)
        # self.command_manager.add_command('complete', complete.CompleteCommand)

        # setup the proxy command managers
        if len(command_managers) == 0:
            raise Exception("One or more CommandManager()'s needed.")
        self.command_managers = {}
        for m in command_managers:
            name = m.__name__.lower()
            self.command_managers[name] = m(name)
            self.command_managers[name].add_command( 'help', HelpCommand )

        # add a fake Command for each manager for the help
        self.command_manager = CommandManager('root')
        for k,v in self.command_managers.items():
            # self.LOG.error(f"K {k}, {v}")
            # BUG: this only results in teh last 'this's help to be ran regardless of which k is requested
            this = type(k, (Command,), { "__doc__": v.__doc__, "take_action": lambda x,y: x.app.run_subcommand(['help',k]) } )
#print(f"Need to print help for {x.app}, {y}") } )

            self.command_manager.add_command( k, this )
            self.command_manager.add_command( 'help', ManagerCommand )

        self.parser = self.build_option_parser(description, version)
        self.version = version
        

    # disable interactive mode
    def interact(self):
        action = HelpAction(None, None, default=self)
        action(self.parser, self.options, None, None)

    def run(self, argv):
        cm = argv.pop(0)
        self.command_manager = self.command_managers[cm]
        #self.LOG.warning(f"RUN command_manager {cm} {type(self.command_manager).__name__} -> {argv}")
        return super( MultiApp, self ).run( argv )

def main(argv=sys.argv[1:]):
    app = MultiApp( description="S3DF Command Line Tools", version=1.0,
         command_managers=[ User, Repo, Menu, ])
    return app.run(argv)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

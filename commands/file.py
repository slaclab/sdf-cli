import logging
import os

from cliff.show import ShowOne


class Single(ShowOne):
    "Show details about a file"

    log = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Single, self).get_parser(prog_name)
        parser.add_argument('filename', nargs='?')
        return parser

    def take_action(self, parsed_args):
        stat_data = os.stat(parsed_args.filename)
        columns = ('Name',
                   'Size',
                   'UID',
                   'GID',
                   'Modified Time',
                   )
        data = (parsed_args.filename,
                stat_data.st_size,
                stat_data.st_uid,
                stat_data.st_gid,
                stat_data.st_mtime,
                )
        return (columns, data)



from cliff.lister import Lister

class Many(Lister):
    """Show a list of files in the current directory.
    The file name and size are printed by default.
    """

    log = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(Many, self).get_parser(prog_name)
        parser.add_argument('many', nargs='?', default='.')
        return parser

    def take_action(self, parsed_args):
        return (('Name', 'Size'),
                ((n, os.stat(n).st_size) for n in os.listdir('.'))
                )

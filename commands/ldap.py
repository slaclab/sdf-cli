from cliff.command import Command
from cliff.commandmanager import CommandManager

from os.path import exists
import bonsai

from .utils.graphql import GraphQlClient

import logging

def get( conn, dn="ou=Group,dc=reg,o=slac", filter="(objectclass=*)", map={'field': { 'attr': 'name', 'array': True } }, fail_okay=[] ):
    res = conn.search(dn, bonsai.LDAPSearchScope.SUB, filter )
    for r in res:
      found = {}
      try:
        d = {}
        logging.debug(f"+ {r}")
        for f,attr in map.items():
          found[f] = False
          logging.debug(f" {f} with {attr}")
          if attr['attr'] in r:
            found[f] = True
            d[f] = r[attr['attr']]
            if not 'array' in attr or attr['array'] == False:
              d[f] = r[attr['attr']][0]
          if not found[f]:
            if f in fail_okay:
              d[f] = None
              if f in map.keys() and 'array' in map[f] and map[f]['array'] == True:
                d[f] = []
            else:
              logging.debug(f"could not find attr {attr['attr']}")
        logging.debug(f"- {d}\n")
        if False in [ v for k,v in found.items() ]:
            this = {}
            for k,v in found.items():
                if v == False:
                    this[k] = False
            logging.warning(f"W: uid {d['uid']} missing {this.keys()}")
        yield d, r
      except Exception as e:
        logging.warn(f"E: {e} for {r}")


def get_password( filepath ):
    password = None
    if filepath:
        if not exists(filepath):
            raise FileNotFoundError(f'BindPW file {filepath} not found.') 
        with open(filepath) as f:
          password = f.read().strip()
    return password

def get_ad_users( server, basedn, binddn=None, password=None ): 
        client = bonsai.LDAPClient( server )
        logging.error(f"connecting to {server} with {binddn}")
        if binddn and not password == None:
            client.set_credentials( 'SIMPLE', binddn, password )
        client.set_cert_policy('never')
        with client.connect() as conn:
            for d,ldif in get( conn, dn=basedn, fail_okay=[ 'uidNumber', 'eppns', 'lastLogonTimestamp', 'employeeID', ], map={
              'uid': { 'attr': 'sAMAccountName' },
              'uidNumber': { 'attr': 'uidNumber' },
              'employeeID': { 'attr': 'employeeID' },
              'eppns': { 'attr': 'userPrincipalName', 'array': True },
              'pwdLastSet': {'attr': 'pwdLastSet'}, # epoch?
              'accountExpires' : {'attr': 'accountExpires'},
              'lastLogonTimestamp': {'attr': 'lastLogonTimestamp'},

            }):
                if 'uid' in d:
                    yield d
                else:
                    for k in ('thumbnailPhoto', 'jpegPhoto', 'userCertificate'):
                        if k in ldif:
                            del ldif[k]
                    logging.warning(f"parsing {ldif}")
    

def get_unix_users( server, basedn ):
        client = bonsai.LDAPClient( server )
        logging.error(f"connecting to {client}")
        client.set_cert_policy('never')
        with client.connect() as conn:
            for d,ldif in get( conn, filter="(objectclass=posixAccount)", dn=basedn, fail_okay=[ 'eppns', ], map={
              'uid': { 'attr': 'uid' },
              'uidNumber': { 'attr': 'uidNumber' },
              'eppns': { 'attr': 'mail', 'array': True },

            }):
                if 'uid' in d:
                    yield d
                else:
                    for k in ('thumbnailPhoto', 'jpegPhoto', 'userCertificate'):
                        if k in ldif:
                            del ldif[k]
                    logging.warning(f"parsing {ldif}")
    



class PullUsers(Command,GraphQlClient):
    "get list of users from ldap into iris"
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(PullUsers, self).get_parser(prog_name)
        parser.add_argument('--bindpw_file',)
        return parser

    def take_action(self, parsed_args):


#        for user in get_ad_users( 'ldaps://dc01.win.slac.stanford.edu:636', 'OU=Users,OU=SCS,DC=win,DC=slac,DC=stanford,DC=edu', 'CN=osmaint,OU=Service-Accounts,OU=SCS,DC=win,DC=slac,DC=stanford,DC=edu', password=get_password(parsed_args.bindpw_file) ):
#             print( f"{user}" )

        self.connectGraphQl() 
        stats = {
            'db_entries': 0,
            'ldap_entries': 0,
            'added': 0,
            'changed': 0,
        }

        # prefetch all users in db, recast as dict for lookup purposes
        q = """query { users { id uid uidNumber eppns } }"""
        res = self.query(q)
        db_users = {}
        for i in res['users']:
            u = i['uid'] # key by uid or uidNumber?
            db_users[u] = i
        stats['db_entries'] = len(db_users.keys())

        #for k,v in db_users.items():
        #     self.LOG.warning(f" {k} = {v}")

        for ldap_user in get_unix_users( 'ldaps://ldap601.slac.stanford.edu:636', 'dc=slac,dc=stanford,dc=edu' ):
            stats['ldap_entries'] += 1

            # 1) new entry, create in db
            if not ldap_user['uid'] in db_users:
                create = """
                mutation{
                  createUser( data: {
                    uid: "%s",
                    uidNumber: %s,
                    eppns: [%s]
                  }) {
                    user {
                      uid eppns uidNumber
                    }
                  }
                }
                """ % (user['uid'], user['uidNumber'], ','.join([ '"'+e+'"' for e in user['eppns'] ]) ) 
                #self.LOG.warning( f"creating {create}" )
                res = self.query( create )
                stats['added'] += 1

            # 2) check for changes and push if needed
            else:

                db_user = db_users[ldap_user['uid']]
                self.LOG.info(f"comparing {ldap_user} to {db_user}")
                merged = db_user | ldap_user
                # merge eppns
                # assume local db always has more eppns than remote ldap
                # always assume that the eppns are alphabetically sorted
                new_eppns = list(set(ldap_user['eppns']) - set(db_user['eppns']))
                merged['eppns'] = sorted( db_user['eppns'] + new_eppns )
                
                # commit back to db if changed
                if not merged == db_user:
                    self.LOG.info(f"changed: {merged} from {db_user}")
                    update = """
                    mutation{
                      updateUser( data: {
                        id: "%s",
                        uid: "%s",
                        uidNumber: %s,
                        eppns: [%s]
                      }) {
                        user {
                          uid eppns uidNumber
                        }
                      }
                    }
                    """ % (merged['id'], merged['uid'], merged['uidNumber'], ','.join([ '"'+e+'"' for e in merged['eppns'] ]) )
                    #self.LOG.error(update)
                    self.query(update)
                    stats['changed'] += 1
                    
        self.LOG.warning(f"STATS: {stats}")



def dict2LdapEntry( d, basedn="ou=People,dc=sdf,dc=slac,dc=stanford,dc=edu,o=s3df" ):
    entry = bonsai.LDAPEntry( f"cn={d['uid']},{basedn}" )
    entry['objectClass'] = [ 'top', 'posixAccount', 'iNetOrgPerson' ]
    entry['cn'] = d['uid']
    entry['sn'] = d['uid']
    entry['uid'] = d['uid']
    entry['uidNumber'] = d['uidNumber']
    entry['gidNumber'] = d['uidNumber']
    entry['homeDirectory'] = '/sdf/home/' + d['uid'][0:1] + '/' + d['uid']
    entry['mail'] = d['eppns']
    entry['loginShell'] = '/bin/bash'
    return entry

class PushUsers(Command,GraphQlClient):
    "populate ldap from iris"
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(PushUsers, self).get_parser(prog_name)
        parser.add_argument('--server', default="ldap://sdfns001.slac.stanford.edu:389")
        parser.add_argument('--binddn', default="cn=Manager,dc=sdf,dc=slac,dc=stanford,dc=edu,o=s3df")
        parser.add_argument('--bindpw_file', required=True)
        return parser

    def take_action(self, parsed_args):
        client = bonsai.LDAPClient( parsed_args.server )
        logging.error(f"connecting to {parsed_args.server}")
        client.set_cert_policy('never')
        
        # get all the users
        self.connectGraphQl()
        q = "{ users { uid uidNumber eppns } }"
        res = self.query( q )
        # self.LOG.info(f"{res['users']}")

        stats = {
            'added': 0,
            'modified': 0,
            'nochange': 0,
        }
            
        client.set_credentials( 'SIMPLE', parsed_args.binddn, get_password(parsed_args.bindpw_file) )
        with client.connect() as conn:

            # cache the existing entries in ldap
            ldap_query = conn.search("dc=sdf,dc=slac,dc=stanford,dc=edu,o=s3df", bonsai.LDAPSearchScope.SUB, "(objectClass=posixAccount)" )
            ldap_users = {}
            for r in ldap_query:
                # self.LOG.info(f"{r}")
                ldap_users[r['uid'][0]] = r

            # make sure all entries in our db is in ldap
            for db_user in res['users']:
                e = dict2LdapEntry( db_user )
                uid = db_user['uid']
                if uid in ldap_users:
                    same = []
                    for k,v in e.items():
                        # can't match on object class, so skip it
                        if k in ('objectClass',):
                            continue
                        if not k in ldap_users[uid]:
                            same.append(False)
                            self.LOG.warning(f"field {k} is missing from ldap {ldap_users[uid]}")
                        elif ldap_users[uid][k] == v:
                            same.append(True)
                        else:
                            same.append(False)
                            self.LOG.warning(f"not same {k} {v}:\n iris {e}\n ldap {ldap_users[uid]}")
                    #self.LOG.info(f"same? {same}")
                    if not False in same:
                        self.LOG.debug(f"entries identical {uid} -> iris {e} / ldap {ldap_users[uid]}") 
                        stats['nochange'] += 1
                    else:
                        self.LOG.info(f"entries need updating {uid} -> \n iris {e}\n ldap {ldap_users[uid]}") 
                        # just push the iris entry to ldap
                        for k,v in e.items():
                            ldap_users[uid][k] = v
                        ldap_users[uid].modify()
                        stats['modified'] += 1
                else:
                    self.LOG.info(f"Add new {e}")
                    conn.add(e)
                    stats['added'] += 1

        self.LOG.info(f"STATS {stats}")
    

class Ldap(CommandManager):
    "Manage LDAP information"

    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Ldap,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ PullUsers, PushUsers ]:
            self.add_command( cmd.__name__.lower(), cmd )



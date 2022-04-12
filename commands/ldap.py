from cliff.command import Command
from cliff.commandmanager import CommandManager

from os.path import exists
import bonsai
import re

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
                if v == False and not k in fail_okay:
                    this[k] = False
            if False in this.keys():
                logging.warning(f"W: {d} missing {this.keys()}")
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
              'username': { 'attr': 'uid' },
              'uidNumber': { 'attr': 'uidNumber' },
              'eppns': { 'attr': 'mail', 'array': True },

            }):
                if 'username' in d:
                    yield d
                else:
                    for k in ('thumbnailPhoto', 'jpegPhoto', 'userCertificate'):
                        if k in ldif:
                            del ldif[k]
                    logging.warning(f"parsing {ldif}")
    


def get_unix_groups( server, basedn ):
    client = bonsai.LDAPClient( server )
    logging.info(f"connecting to {client}")
    client.set_cert_policy('never')
    with client.connect() as conn:
        for d,ldif in get( conn, filter="(objectclass=posixGroup)", dn=basedn, fail_okay=[ 'users', ], map={
          'name': { 'attr': 'cn' },
          'gidNumber': { 'attr': 'gidNumber' },
          'users': { 'attr': 'memberUid', 'array': True },

        }):
            if 'name' in d and not ' ' in str(d['name']):
                yield d
            else:
                logging.warning(f"parsing {ldif}")



class PullUsers(Command,GraphQlClient):
    "get list of users from ldap into iris"
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(PullUsers, self).get_parser(prog_name)
        parser.add_argument('--bindpw_file',)
        parser.add_argument('--dry_run', action='store_true', help='do not commit any changes', default=False)
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
            'nochange': 0,
        }

        # prefetch all users in db, recast as dict for lookup purposes
        self.LOG.info("Fetching existing users...")
        q = """query { users( filter: {} ) { Id username uidNumber eppns } }"""
        res = self.query(q)
        db_users = {}
        for i in res['users']:
            u = i['username'] # key by uid or uidNumber?
            db_users[u] = i
        stats['db_entries'] = len(db_users.keys())

        #for k,v in db_users.items():
        #     self.LOG.warning(f" {k} = {v}")

        self.LOG.info("Fetching ldap users...")
        for ldap_user in get_unix_users( 'ldaps://ldap601.slac.stanford.edu:636', 'dc=slac,dc=stanford,dc=edu' ):
            stats['ldap_entries'] += 1

            # 1) new entry, create in db
            if not ldap_user['username'] in db_users:
                create = """
                mutation{
                  userCreate( data: {
                    username: "%s",
                    uidNumber: %s,
                    eppns: [%s]
                  }) {
                    username eppns uidNumber
                  }
                }
                """ % (ldap_user['username'], ldap_user['uidNumber'], ','.join([ '"'+e+'"' for e in ldap_user['eppns'] ]) ) 
                self.LOG.info( f"  creating {create}" )
                if not parsed_args.dry_run:
                    res = self.query( create )
                stats['added'] += 1

            # 2) check for changes and push if needed
            else:

                db_user = db_users[ldap_user['username']]
                self.LOG.debug(f"  comparing {ldap_user} to {db_user}")
                merged = db_user | ldap_user
                # merge eppns
                # assume local db always has more eppns than remote ldap
                # always assume that the eppns are alphabetically sorted
                new_eppns = list(set(ldap_user['eppns']) - set(db_user['eppns']))
                merged['eppns'] = sorted( db_user['eppns'] + new_eppns )
                
                # commit back to db if changed
                if not merged == db_user:
                    self.LOG.info(f"  changed: {merged} from {db_user}")
                    update = """
                    mutation{
                      updateUser( data: {
                        Id: "%s",
                        username: "%s",
                        uidNumber: %s,
                        eppns: [%s]
                      }) {
                          username eppns uidNumber
                      }
                    }
                    """ % (merged['id'], merged['username'], merged['uidNumber'], ','.join([ '"'+e+'"' for e in merged['eppns'] ]) )
                    if not parsed_args.dry_run:
                      self.query(update)
                    stats['changed'] += 1

                else:
                    stats['nochange'] += 1
                    
        self.LOG.warning(f"STATS: {stats}")



def dict2LdapEntry( d, basedn="ou=People,dc=sdf,dc=slac,dc=stanford,dc=edu,o=s3df" ):
    entry = bonsai.LDAPEntry( f"cn={d['username']},{basedn}" )
    entry['objectClass'] = [ 'top', 'posixAccount', 'iNetOrgPerson' ]
    entry['cn'] = d['username']
    entry['sn'] = d['usermame']
    entry['uid'] = d['username']
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
        q = "{ users( filter: {} ) { username uidNumber eppns } }"
        res = self.query( q )
        stats = {
            'added': 0,
            'modified': 0,
            'nochange': 0,
        }
            
        # recast all the users in the db into ldifs to upload to the ldap server
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
                uid = db_user['username']
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
    


class PullGroups(Command,GraphQlClient):
    "get list of groups from ldap into iris"
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        parser = super(PullGroups, self).get_parser(prog_name)
        parser.add_argument('--source', choices=['unix-admin','pcds'], required=True )
        parser.add_argument('--server', required=False, help='ldap server uri' )
        parser.add_argument('--basedn', required=False, help='ldap basedn for group query' )
        parser.add_argument('--dry_run', action='store_true', help='do not commit any changes', default=False)
        return parser

    def take_action(self, parsed_args):

        # for each group in ldap, we must determine the AccessGroup (which is the unix group) and the associated Repo for the access group.
        # for now,lets just assume the name of the AccessGroup is the same as that of the Repo it is associated to.

        server = None
        basedn = None
        if str(parsed_args.source) == 'unix-admin':
            server = 'ldaps://ldap601.slac.stanford.edu:636' 
            basedn = 'dc=slac,dc=stanford,dc=edu'
        elif str(parsed_args.source) == 'pcds':
            server = 'ldap://psldap1'
            basedn = 'ou=Group,dc=reg,o=slac'
        else:
            if not parsed_args.server and not parsed_args.basedn:
                raise Exception("--server and --basedn required if --source undefined")
            server = parsed_args.server
            basedn = parsed_args.basedn

        self.connectGraphQl() 
        stats = {
            'db_accessGroup_entries': 0,
            'db_repo_entries': 0,
            'db_facility_entries': 0,
            'ldap_entries': 0,
            'access_group_added': 0,
            'access_group_changed': 0,
            'access_group_nochange': 0,
            'repos_added': 0,
            'repos_changed': 0,
            'repos_nochange': 0,
            'facilities_added': 0,
            'facilities_changed': 0,
            'facilities_nochange': 0,
        }

        # prefetch all users in db, recast as dict for lookup purposes
        q = """
          query accessGroups { accessGroups( filter: {} ) { Id name gidNumber } }
        """
        res = self.query(q)
        db_accessGroups = {}
        if 'accessGroups' in res:
            for i in res['accessGroups']:
                self.LOG.info(f"< {i}")
                u = i['gidNumber']
                db_accessGroups[u] = i
            stats['db_accessGroup_entries'] = len(db_accessGroups.keys())
        q = """
          query repos { repos( filter: {} ) { Id name accessGroups state facility principal leaders users } } 
        """
        res = self.query(q)
        db_repos = {}
        if 'repos' in res:
            for i in res['repos']:
                u = i['facility'] + ':' + i['name']
                db_repos[u] = i
            stats['db_repo_entries'] = len(db_repos.keys())
        q = """
          query facilities { facilities( filter: {} ) { Id name } }
        """
        res = self.query(q)
        db_facilities = {}
        if 'facilities' in res:
            for i in res['facilities']:
                u = i['name']
                self.LOG.warning(f"FOUND {u}")
                db_facilities[u] = i
            stats['db_facility_entries'] = len(db_facilities.keys())

        # as nis limits the number of entries per group, we overload the name but keep the same gid
        group_name_mapping = {
            r'^atlas-\w$': 'atlas',
            r'^lsst-\w$': 'lsst',
            r'^bfact-\w$': 'bfact',
            r'lcls-\w$': 'lcls',
        }

        # not eh most efficient, but we just iterate through all entries and store them under ldap_groups, keyed on the gid. if 
        # we've already seen the gid we append the list of members
        # we also do some name remapping at the same time
        ldap_groups = {}
        for entry in get_unix_groups( server, basedn ):
            stats['ldap_entries'] += 1

            name = entry['name']
            for r,setname in group_name_mapping.items():
                if re.match( r, str(name) ):
                    name = setname
                    break

            gid_number = entry['gidNumber']
            self.LOG.debug(f"found {name} for gid {gid_number} with {entry['users']}")
            # 1) new
            if not gid_number in ldap_groups:
                ldap_groups[gid_number] = { 'name': name, 'users': entry['users'] }
            # 2) append
            elif gid_number in ldap_groups and len(entry['users']):
                ldap_groups[gid_number]['users'] = list( set(ldap_groups[gid_number]['users']) | set(entry['users']) )
            # 3) empty
            elif gid_number in ldap_groups and len(entry['users']) == 0:
                # no members, okay to ignore?
                pass
            # 4) error!
            else:
                raise NotImplementedError(f"dunno what to do with {entry}")
            
        # lets populate the access groups
        for gid_number, entry in ldap_groups.items():
            #self.LOG.info(f"{gid_number} {entry['name']}\n  {entry['users']}")

            # 1) insert new entry
            if not gid_number in db_accessGroups:
                #self.LOG.info( f"Add repo {name}: {ldap_group}" )
                create = """
                  mutation {
                    accessGroupCreate( data: {
                      state: "Active",
                      name: "%s",
                      gidNumber: %s,
                    }){
                   	   Id name gidNumber
                    }
                  }
                """ % ( entry['name'], gid_number )
                concat = re.sub( r'\s+', ' ', create.replace('\n','') )
                self.LOG.info(f"Adding {concat}")   
                if not parsed_args.dry_run:
                    self.query( create )

                stats['access_group_added'] += 1

            # 2) gid exists, but differet name, update the name
            elif gid_number in db_accessGroups:

                if not str(db_accessGroups[gid_number]['name']) == str(entry['name']):
                    update = """mutation { accessGroupUpdate( data: { Id: "%s", name: "%s" } ) { Id gidNumber name } } """ % (db_accessGroups[gid_number]['Id'], entry['name'])
                    self.LOG.info("Updating {update}")
                    if not parsed_args.dry_run:
                        self.query( update )
                    #raise NotImplementedError(f"need to update access group {gid_number} name from {db_accessGroups[gid_number]['name']} to {entry['name']}: {update}") 
                    stats['access_group_changed'] += 1

                # 3) its fine
                else:
                    stats['access_group_nochange'] += 1

        facility_mapping = {
            r'^esd': 'ESD',
            r'^lcls': 'LCLS',
            r'^lsst': 'Rubin',
            r'^bfact': 'BFactory',
            r'^bbr': 'BaBar',
            r'^ltda': 'BaBar',
            r'^cdms': 'CDMS',
            r'^amo': 'LCLS',
            r'^exo': 'EXO',
            r'^cxi': 'LCLS',
            r'^dia': 'LCLS',
            r'^mec': 'LCLS',
            r'^mfx': 'LCLS',
            r'^mob': 'LCLS',
            r'^kipac': 'KIPAC',
            r'^ps-': 'LCLS',
            r'^glast': 'Fermi',
            r'^ilc': 'ILC',
            r'^rix': 'LCLS',
            r'^rubin': 'Rubin',
            r'^pulse': 'Pulse',
            r'^spear': 'Spear',
            r'^suncat': 'SUNCAT',
            r'^sxr': 'LCLS',
            r'^tmo': 'LCLS',
            r'^ued': 'UED',
            r'^usr': 'LCLS',
            r'^xcs': 'LCLS',
            r'^xpp': 'LCLS',
            r'^at$': 'USATLAS',
            r'^atlas': 'USATLAS',
            r'^xu$': 'LCLS',
        } 
        
        # now we have to create the facilities and repos for each group
        for gid_number, entry in ldap_groups.items():

            # first assume the facility is the same as the group
            the_facility = entry['name']

            # go through mappins defined above
            for k,v in facility_mapping.items():
                if re.match( k, str(entry['name']) ):
                    the_facility = v

            if not str(the_facility) in db_facilities:
                fac = """mutation { facilityCreate( data: { name: "%s" } ) { Id name } }""" % (the_facility,)
                self.LOG.info(f"adding facility {the_facility}: {fac} {db_facilities}")
                if not parsed_args.dry_run:
                    self.query( fac )
                stats['facilities_added'] += 1
                # cache for future
                db_facilities[str(the_facility)] = True

            # validate
            elif str(the_facility) in db_facilities:
                # check to ensure that there are no changes
                if not isinstance( db_facilities[str(the_facility)], bool) and not db_facilities[str(the_facility)]['name'] == str(the_facility):
                     stats['facilities_changed'] += 1
                     raise NotImplementedError(f"facility name change required from {db_facilities[str(the_facility)]} to {the_facility}")

                stats['facilities_nochange'] += 1

            #
            # now deal with the repo
            #
            the_repo = str(entry['name'])

            self.LOG.info(f"> {gid_number} ({entry['name']}) \t-> facility {the_facility} \t repo {the_repo}\t users: {entry['users']}")

            key = str(the_facility) + ':' + str(the_repo)

            # format the array into something taht we can pass into the graphql query
            def arrayify( array ):
                stuff = str(sorted(array)).replace("'", '"')
                if stuff == '':
                    stuff = '[]'
                return stuff

            users = arrayify( entry['users'] )
            # TODO: how do we deal with repos with multiple access groups? aer we just assuming a 1:1 mapping for now?
            access_groups = arrayify( [ str(the_repo), ] )

            # does not curently exist in db, add it
            if not key in db_repos:
                q = """mutation { repoCreate( data: { name: "%s", facility: "%s", accessGroups: %s, principal: "%s", leaders: %s, users: %s}) { Id name facility accessGroups principal leaders users } }""" % ( the_repo, the_facility, access_groups, 'TBD', '[]', users )
                self.LOG.info(f"adding repo {q}")
                if not parsed_args.dry_run:
                    self.query(q)
                stats['repos_added'] += 1

            # 2) entry already exist....
            if key in db_repos:

#                # lets see what changed
#                diff = []
#                for k,x in db_repos[key].items():
#                    if k == 'users':
#                        v = sorted(v)
#                    elif k in ( 'state', ):
#                        continue
#                    if db_repos[key][k] == v:
#                        diff.append(False)
#                    else:
#                        db_repos[key][k] = v
#                        diff.append(True)

                #self.LOG.warning(f"got {key} for {db_repos[key]} with {diff}")
#                if not True in diff:
#                    stats['repos_nochange'] += 1
#                else:

                if not [ str(the_repo), ] == db_repos[key]['accessGroups'] or \
                    not sorted(entry['users']) == sorted(db_repos[key]['users']):
                    modify = """
                      mutation {
                        repoUpdate( data: {
                          Id: "%s",
                          accessGroups: %s,
                          users: %s    
                        }){
                       	   Id description name principal leaders users
                        }
                      }
                    """ % ( db_repos[key]['Id'], access_groups, users )
                    concat = re.sub( r'\s+', ' ', modify.replace('\n','') ) 
                    self.LOG.info( f"changing repo {db_repos[key]} -> {concat}")
                    if not parsed_args.dry_run:
                        self.query( modify )
                    stats['repos_changed'] += 1
                else:
                    stats['repos_nochange'] += 1

        self.LOG.info(f"STATS {stats}")


class Ldap(CommandManager):
    "Manage LDAP information"

    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Ldap,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ PullUsers, PushUsers, PullGroups ]:
            self.add_command( cmd.__name__.lower(), cmd )



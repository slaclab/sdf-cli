import os
import sys
import inspect
from enum import Enum
from typing import Any, List, Optional
import math

import argparse
from cliff.command import Command
from cliff.commandmanager import CommandManager

from .utils.graphql import GraphQlClient

from gql import gql

import logging

from typing import Any
import pendulum as pdl
from tzlocal import get_localzone
local_tz = get_localzone()

import subprocess
import re


def parse_datetime( value: Any, timezone=local_tz, force_tz=False) -> pdl.DateTime:
    dt = None
    if isinstance( value, int ):
        dt = pdl.from_timestamp(value)
    else:
        dt = pdl.parse(value, tz=local_tz)
    #logging.warning(f'parse datetime ({type(value)}) {value} -> {dt} ({force_tz})')
    if force_tz:
        dt = dt.replace(tzinfo=timezone)
        #logging.warning(f'  -> {dt}')
    return dt

class SlurmDump(Command):
    'Dumps data from slurm into flat files for later ingestion'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        p = super(SlurmDump, self).get_parser(prog_name)
        p.add_argument('--verbose', help='verbose output', required=False)
        p.add_argument('--date', help='import jobs from this date', default='2023-10-18')
        p.add_argument('--starttime', help='start time of job imports', default='00:00:00')
        p.add_argument('--endtime', help='end time of job imports', default='23:59:59')
        return p

    def take_action(self, parsed_args):
        self.verbose = parsed_args.verbose
        first = True
        index = {}
        buffer = []
        for line in self.run_sacct( date=parsed_args.date, start_time=parsed_args.starttime, end_time=parsed_args.endtime ):
            print(f"{line}")

    def run_sacct(self, sacct_bin_path: str='sacct', date: str='2023-10-12', start_time: str='00:00:00', end_time: str='23:59:59' ):
        commandstr = f"""SLURM_TIME_FORMAT=%s {sacct_bin_path} --allusers --duplicates --allclusters --allocations --starttime="{date}T{start_time}" --endtime="{date}T{end_time}" --truncate --parsable2 --format=JobID,User,UID,Account,Partition,QOS,Submit,Start,End,Elapsed,NCPUS,AllocNodes,AllocTRES,CPUTimeRAW,NodeList,Reservation,ReservationId,State"""
        if self.verbose:
            self.LOG.info(f"cmd: {commandstr}")
        index = 0
        c = 0
        s = 0

        process = subprocess.Popen(commandstr, shell=True, stdout=subprocess.PIPE)
        for line in iter(process.stdout.readline, b''):
            fields = line.decode("utf-8").split("|")
            if index == 0 or  len(fields) >= 10: # and line.find(b"PENDING") == -1 ): 
                yield line.decode("utf-8").strip()
                if index > 0:
                    c += 1
                index += 1
            else:
                self.LOG.warning(f"skipping ({len(fields)}, {int(fields[7])} < {int(fields[8])}) {line}")
                
        #yield f"--- count={c}"


class SlurmRemap(Command):
    'Remaps/patches the slurm job data to prepare for import'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        p = super(SlurmRemap, self).get_parser(prog_name)
        p.add_argument('--verbose', help='verbose output', required=False)
        p.add_argument('--data', help='data to read from', type=argparse.FileType(), default=sys.stdin)
        return p

    def take_action(self, parsed_args):
        self.verbose = parsed_args.verbose
        first = True
        index = {}
        order = []
        for line in parsed_args.data.readlines():
            #print(f"in : {line.strip()}")
            if line:
                parts = line.split("|")
                if first:
                    index = { s: idx for idx, s in enumerate(parts) }
                    order = parts
                    first = False
                    print(f'{line}')
                else:
                    out = self.convert( index, parts, order )
                    if out:
                        print(f'{out}')

    def convert( self, index, parts, order ) -> dict:
        d = { field: parts[idx] for field, idx in index.items() }
        d = self.remap_job( d )
        if d:
            out = []
            for i in order:
                out.append(d[i])
            return '|'.join(out).strip()


    def remap_job( self, d ):
        """ deal with old jobs with wrong account info """
        #self.LOG.info(f"in: {d}") 
        if d['User'] in ( 'lsstsvc1' ) and d['Account'] in ( 'rubin', 'shared', '' ):
            d['Account'] = 'rubin:production'
        elif d['Account'] in ( 'shared', 'shared:default' ) or d['User'] in ( 'jonl', 'vanilla', 'yemi', 'yangw', 'pav', 'root', 'reranna', 'ppascual', 'renata'):
            return None
        elif d['User'] in ('csaunder','elhoward', 'laurenma','smau', 'bos', 'erykoff', 'ebellm', 'mccarthy','yesw','abrought', 'shuang92', 'aconnoll', 'daues', 'aheinze','zhaoyu','dagoret', 'kannawad', 'kherner', 'eske', 'cslater', "sierrav", 'jmeyers3', 'lskelvin', 'jchiang', 'yanny', 'ktl', 'jneveu', 'hchiang2', 'snyder18', 'fred3m', 'brycek', 'eiger', 'esteves', 'mxk', 'yusra', 'mrabus', 'ryczano', 'mgower', 'yoachim', 'scichris', ) and d['Account'] in ('', 'milano', 'roma'):
            d['Account'] = 'rubin:developers'
            if d['Partition'] == 'ampere':
                d['Partition'] = 'milano'
        elif d['User'] == 'kocevski' or ( d['User'] in  ('horner','mdimauro','burnett','laviron','omodei','tyrelj', 'echarles', 'bruel') and d['Account'] in ('','latba','ligo','repository') ):
            d['Account'] = 'fermi:users'
        elif d['User'] in ('glastraw',):
            d['Account'] = 'fermi:l1'
        elif d['User'] in ('vossj',):
            d['Partition'] = 'roma'
        elif d['User'] in ( 'dcesar', 'jytang', 'rafimah', ):
            d['Account'] = 'ad:beamphysics'
        elif d['User'] in ( 'kterao', 'kvtsang', 'anoronyo', 'bkroul', 'zhulcher', 'koh0207', 'drielsma', 'lkashur', 'dcarber', 'amogan', 'cyifan', 'yjwa', 'aj14' , 'jdyer', 'sindhuk', 'justinjm', 'mrmooney', 'bearc', 'fuhaoji', 'sfogarty', 'carsmith', 'yuntse'): #and d['Account'] in ( '', 'ampere', 'ml', 'roma', ):
            d['Account'] = 'neutrino:default'
            d['Partition'] = 'ampere'
        elif d['User'] in ('dougl215','zhezhang'): # and d['Account'] in ('ampere:default',):
            #self.LOG.error("HERE")
            d['Account'] = 'mli:default'
        elif d['User'] in ('jfkern',  'taisgork', 'valmar', 'tgrant', 'arijit01', 'mmdoyle', 'fpoitevi', 'ashojaei', 'monarin', 'claussen', 'batyuk', 'kevinkgu', 'tfujit27', 'haoyuan', 'aliang', 'jshenoy', 'dorlhiac', 'xjql',  ): # and d['Account'] in ('','milano', 'roma'):
            d['Account'] = 'lcls:default'
        elif d['User'] in ( 'psdatmgr', 'xiangli', 'sachsm', 'hekstra', 'cwang31', 'espov', 'thorsten', 'wilko', 'snelson') and d['Account'] in ( '', 'lcls:xpp', 'lcls:psmfx', 'lcls:data', 'ampere'):
            d['Account'] = 'lcls:default'
        elif d['User'] in ( 'lsstccs', 'rubinmgr' ):
            d['Account'] = 'rubin:commissioning'
        elif d['User'] in ( 'majernik', 'knetsch', ):
            d['Account'] = 'facet:default'
        elif d['User'] in ('jberger', ):
            d['Account'] = 'epptheory:default'
        elif d['User'] in ('tabel',):
            d['Account'] = 'kipac:kipac'
        elif d['User'] in ('melwan', 'zatschls', 'yanliu', 'aditi',):
            d['Account'] = 'supercdms:default'

        if d['Account'] == '':
            raise Exception(f"could not determine account for {d}")

        if ',' in d['Partition']:
            a = d['Partition'].split(',')[0]
            d['Partition'] = a

        if not ':' in d['Account']:
            d['Account'] = d["Account"] + ':default'

        #self.LOG.info(f"out: {d}") 
        return d


class SlurmImport(Command,GraphQlClient):
    'Reads sacctmgr info from slurm and translates it to coact accounting stats'
    LOG = logging.getLogger(__name__)

    verbose = False

    def get_parser(self, prog_name):
        p = super(SlurmImport, self).get_parser(prog_name)
        p.add_argument('--print', help='verbose output', action='store_true')
        p.add_argument('--debug', help='debug output', required=False, default=False, action='store_true')
        p.add_argument('--username', help='basic auth username for graphql service', default='sdf-bot')
        p.add_argument('--batch', help='batch upload size', default=1000)
        p.add_argument('--data', help='data to read from', type=argparse.FileType(), default=sys.stdin)
        p.add_argument('--password-file', help='basic auth password for graphql service', required=True)
        p.add_argument('--exit-on-error', help='terminate if cannot parse data', required=False, default=False, action='store_true')
        return p

    def take_action(self, parsed_args):
        self.verbose = parsed_args.print
        self.exit_on_error = parsed_args.exit_on_error

        level = logging.WARNING
        if self.verbose:
            level = logging.INFO
        if parsed_args.debug:
            level = logging.DEBUG
        for handler in self.LOG.handlers:
            if isinstance(handler, type(logging.StreamHandler())):
                handler.setLevel(level)

        # connect
        self.back_channel = self.connect_graph_ql( username=parsed_args.username, password_file=parsed_args.password_file )
        self.get_metadata()

        first = True
        index = {}
        buffer = []
        for line in parsed_args.data.readlines():
            if self.verbose:
                print(f"\n{line.strip()}")
            if line:
                parts = line.split("|")
                if first:
                    index = { s: idx for idx, s in enumerate(parts) }
                    first = False
                else:
                    # keep a buffer for bulk use
                    # and convert the string into a json desc
                    job = self.convert(index, parts)
                    #self.LOG.info(f'job: {job}')
                    if job:
                        buffer.append(job)
                        if len(buffer) >= int(parsed_args.batch):
                            self.upload_jobs(buffer)
                            buffer = []

        if len(buffer) > 0:
            self.upload_jobs(buffer)

        return True

    def get_metadata(self) -> bool:
        REPOS_GQL = gql("""
        query{
            repos(filter:{}){
                Id
                name
                facility
                principal
                leaders
                users
                currentComputeAllocations{
                    Id
                    clustername
                    start
                    end
                }
            }
            clusters(filter:{}){
                name
                memberprefixes
                cpu: nodecpucount
                gpu: nodegpucount
                mem: nodememgb
                gpumem: nodegpumemgb
            }
        }
        """)
        resp = self.back_channel.execute( REPOS_GQL )
        #self.LOG.info(f"{resp}")
        repos = { x['facility'].lower() + ':' + x["name"].lower() : x for x in resp["repos"] }

        # create a lookup table to determine a the relevant allocationId for the job to count against
        self._allocid = {}
        for repo in resp['repos']:
            for alloc in repo.get("currentComputeAllocations", []):
                #self.LOG.info(f'looking at {alloc}')
                key = ( repo['facility'].lower(), repo['name'].lower(), alloc["clustername"].lower())
                if not key in self._allocid:
                    self._allocid[key] = {}

                # sort?
                if 'start' in alloc and 'end' in alloc:
                    time_range = (parse_datetime(alloc['start']), parse_datetime(alloc['end']) )
                    self._allocid[key][time_range] = alloc["Id"]
                    if self.verbose:
                        print(f'found alloc: {key}: {time_range}')
                else:
                    self.LOG.warning(f'{key} has no allocations')
        #self.LOG.info(f'found repo compute allocations {self._allocid}')

        # populate the core, mem and gpu counts
        self._clusters = {}
        for cluster in resp['clusters']:
            name = cluster['name']
            self._clusters[name] = {}
            for k,v in cluster.items():
                if k in ( 'mem', ):
                    v = v * 1073741824
                self._clusters[name][k] = v

        #self.LOG.info(f'found clusters {self._clusters}')

        return True

    def get_alloc_id( self, facility: str, repo: str, cluster: str, time: pdl.DateTime ) -> str:
        key = ( facility, repo, cluster )
        #self.LOG.info(f'looking for key {key} from {self._allocid}')
        if key in self._allocid:
            self.LOG.debug(f'  find alloc for {key}')
            # go through time ranges to determine actual Id
            time_ranges = self._allocid[key]
            for t, _id in time_ranges.items():
                self.LOG.debug(f'    matching {t[0].isoformat()} <= {time.in_timezone(local_tz).isoformat()} < {t[1].isoformat()}?')
                if time >= t[0] and time < t[1]:
                    self.LOG.debug(f'      found match, returning alloc id {_id}')
                    return _id
        raise Exception(f'could not determine alloc_id for {facility}:{repo} at {cluster} at timestamp {time}')

    def upload_jobs(self, jobs):

        self.LOG.debug(f"upload ({len(jobs)}) {jobs}")

        #self.back_channel.execute( JOB_GQL, values )


    def convert( self, index, parts, default_facility='shared', default_repo='default' ) -> dict:

        def conv(s, fx, default=None):
            try:
                return fx(s)
            except:
                return default

        def kilos_to_int(s: str) -> int:
            m = re.match(r"(^[0-9.]+)([KMG])?", s.upper())
            if m:
                mul = 1
                g = m.group(2)
                if g:
                    p = "KMG".find(g)
                    if p >= 0:
                        mul = math.pow(2, (p + 1) * 10)
                    else:
                        raise Exception("Can't handle multiplier=%s for value=%s" % (g, s))
                return int(float(m.group(1)) * mul)
            else:
                raise Exception("Can't parse %s" % s)


        def calc_compute_time(startTs: pdl.DateTime, endTs: pdl.DateTime, submitTs: pdl.DateTime, tres: str, cluster: dict, alloc_nodes: Optional[int], ncpus: Optional[int]) -> float:
            # calculate elapsed time as UTC so we don't get bitten by DST
            elapsed_secs = (endTs - startTs).total_seconds()
            #wait_time = (startTs - submitTs).total_seconds() 

            # min time
            if elapsed_secs <= 0:
                elapsed_secs = 1.

            # TODO: determine maximal amounts
            # if a single node, then divide all metrics by the number of nodes
            used = {}
            if not tres == '':
                for x in tres.split(','):
                    k,v = x.split('=')
                    if 'gpu' in k:
                        k = 'gpu'
                    used[k] = kilos_to_int(v) * 1. / alloc_nodes
            #self.LOG.info(f'  cluster: {cluster}: {tres} -> {used}')
            
            # if node is exclusive
            # max % of cpu, mem or gpu's for servers
            ratios = {}
            max_ratio = 0
            for resource in ( 'cpu', 'gpu', 'mem' ):
                if resource in used:
                    ratios[resource] = used[resource] / cluster[resource]
                    if ratios[resource] > max_ratio:
                        max_ratio = ratios[resource]
                    self.LOG.debug(f'    {resource}: used {used[resource]} / {cluster[resource]}\t -> {ratios[resource]:.5}')

            compute_time = elapsed_secs * ncpus / 3600.0 # TODO: this isn't right...

            resource_time = elapsed_secs * alloc_nodes * max_ratio * cluster['cpu'] / 3600. 
            if self.verbose:
                print(f'  calc time: {elapsed_secs}s\t compute_hours: {resource_time:.5}\t core_hours: {compute_time:.5}')
            return resource_time

        d = { field: parts[idx] for field, idx in index.items() }

        facility = default_facility
        repo = default_repo
        try:
            facility, repo = d["Account"].split(':')
        except Exception as e:
            self.LOG.warn(f"could not determine facility and repo from {d['Account']}")

        #nodelist = d["NodeList"]

        # convert to datetime
        startTs = parse_datetime(int(d['Start']), force_tz=False)
        endTs = parse_datetime(int(d['End']), force_tz=False)
        submitTs = parse_datetime(int(d['Submit']), force_tz=False)

        # compute some values
        alloc_nodes = kilos_to_int(d['AllocNodes'])
        ncpus = conv(d['NCPUS'], int, 0)
        compute_time = calc_compute_time( startTs=startTs, endTs=endTs,
                tres=d["AllocTRES"], submitTs=submitTs,
                alloc_nodes=alloc_nodes, ncpus=ncpus, cluster=self._clusters[d['Partition']])

        # charge factor of job
        cf = 1.0

        # determine appropriate allocation to charge against
        # use submitTs instead of startTs?
        allocId = None
        try:
            allocId = self.get_alloc_id( facility, repo, d['Partition'], startTs )
        except Exception as e:
            self.LOG.warning(f'{e}: {d}')
            if self.exit_on_error:
                sys.exit(1)
            return None

        return {
            #'facility': facility,
            #'repo': repo,
            #'repoid': repoid,
            'jobId': conv(d['JobID'], int, 0),
            'username': d['User'],
            #'uid': conv(d['UID'], int, 0),
            #'accountName': d['Account'],
            #'partitionName': d['Partition'],
            'allocationId': allocId,
            #'qos': d['QOS'],
            'chargeFactor': cf,
            #'submitTs': submitTs,
            'startTs': startTs,
            #'endTs': endTs,
            #'clustername': d['Partition'],
            #'ncpus': ncpus,
            #'allocNodes': alloc_nodes,
            #'allocTres': d['AllocTRES'],
            #'nodelist': nodelist,
            #'reservation': None if d['Reservation'] == '' else d['Reservation'],
            #'reservationId': None if d['ReservationId'] == '' else d['ReservationId'],
            'resourceHours': compute_time,
            #'submitter': None,
            #'officialImport': True
        }

#        return {
#            'jobId': conv(d['JobID'], int, 0),
#            'username': d['User'],
#            'allocationId': allocId,
#            'chargeFactor': cf,
#            'date': startTs, #TODO yield multiple
#            'resourceHours': compute_time,
#            'chargeFactor': cf,
#        }


class Coact(CommandManager):
    "A Manager class to register sub commands"
    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Coact,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ SlurmDump, SlurmRemap, SlurmImport, ]:
            self.add_command( cmd.__name__.lower(), cmd )



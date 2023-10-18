import os
import sys
import inspect
from enum import Enum
from typing import Any

from cliff.command import Command
from cliff.commandmanager import CommandManager

from .utils.graphql import GraphQlClient

from gql import gql

import logging
import datetime
from dateutil import parser, tz
import pytz

import subprocess
import re


def parse_datetime(value, make_date=False, timezone=tz.tzlocal()):
    if not value:
        return None
    if re.match(r"^[0-9]+$", value):
        # assume the epoch time sent is in UTC
        dt = datetime.datetime.fromtimestamp(int(value), tz.gettz("UTC"))
    else:
        if value.find("T") > -1:
            dt = parser.isoparse(value)
        else:
            dt = parser.parse(value)

        # if "aware" timestamp (ie. has timezone), convert to PST
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            dt = dt.replace(tzinfo=timezone)
    return datetime.datetime.combine(datetime.date(dt.year, dt.month, dt.day), datetime.time()) if make_date else dt



class SlurmImport(Command,GraphQlClient):
    'Reads sacctmgr info from slurm and translates it to coact accounting stats'
    LOG = logging.getLogger(__name__)

    def get_parser(self, prog_name):
        p = super(SlurmImport, self).get_parser(prog_name)
        p.add_argument('--verbose', help='verbose output', required=False)
        p.add_argument('--username', help='basic auth username for graphql service', default='sdf-bot')
        p.add_argument('--password-file', help='basic auth password for graphql service', required=True)
        p.add_argument('--batch', help='batch upload size', default=1000)
        p.add_argument('--date', help='import jobs from this date', default='20231018')
        p.add_argument('--starttime', help='start time of job imports', default='00:00:00')
        p.add_argument('--endtime', help='end time of job imports', default='23:59:59')

        return p

    def take_action(self, parsed_args):
        # connect
        self.back_channel = self.connect_graph_ql( username=parsed_args.username, password_file=parsed_args.password_file )


        self.LOG.info("gathering meta data...")
        self.get_meta_data()

        self.LOG.info("gathering slurm job info...")

        first = True
        index = {}
        buffer = []
        job_count = 0
        total_secs = 0

        for line in self.run_sacct( date=parsed_args.date, start_time=parsed_args.starttime, end_time=parsed_args.endtime ):
            self.LOG.info(f"{line}")

            if line:
                parts = line.split("|")
                #self.LOG.debug(f"{parts}")
                if first:
                    index = { s: idx for idx, s in enumerate(parts) }
                    first = False
                else:
                    if line.startswith("--- "):
                        # verification info follows: line_count and total time
                        s = line.split(" ")
                        job_count = int(s[1].strip())
                        total_secs = int(s[2].strip())
                        break

                    # keep a buffer for bulk use
                    # and convert the string into a json desc
                    buffer.append(self.convert(index, parts))
                    if len(buffer) >= int(parsed_args.batch):
                        self.LOG.info(f"Upserting {len(buffer)} jobs:")
                        self.upload_jobs(buffer)
                        buffer = []

        if len(buffer) > 0:
            self.LOG.info(f"Upsert of {len(buffer)} jobs:")
            self.upload_jobs(buffer)

        return True

    def get_meta_data(self):
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
                chargefactor
                members
                memberprefixes
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
                self.LOG.info(f'looking at {alloc}')
                key = ( repo['facility'].lower(), repo['name'].lower(), alloc["clustername"].lower())
                if not key in self._allocid:
                    self._allocid[key] = {}

                # sort?
                if 'start' in alloc and 'end' in alloc:
                    time_range = (parse_datetime(alloc['start']), parse_datetime(alloc['end']) )
                    self._allocid[key][time_range] = alloc["Id"]
                    self.LOG.info(f'  {time_range}')
                else:
                    self.LOG.warning(f'{key} has no allocations')

        self.LOG.info(f'found {self._allocid}')
        return repos

    def get_alloc_id( self, facility, repo, cluster, time ):
        key = ( facility, repo, cluster )
        if key in self._allocid:
            self.LOG.info(f'found alloc for {key}: {self._allocid[key]}')
            # go through time ranges to determine actual Id
            time_ranges = self._allocid[key]
            for t, _id in time_ranges.iteritems():
                self.LOG.info(f'  matching {time} against {t}')
                if time > t[0] and time < t[1]:
                    self.LOG.info(f'    found match, returning alloc id {_id}')
                    return self._allocid[key]
        raise Exception(f'could not determine alloc id for {facility}:{repo} at {cluster} at timestamp {time}')

    def upload_jobs(self, jobs):

        values = []
        days = set()
        seen = set()
        ids = []
        for inp in jobs:
            # skip dupes (they cause a sql exception)
            key = "%s,%s" % (inp["jobId"], inp["startTs"])
            if key in seen:
                continue
            seen.add(key)

            days.add(datetime.date(inp["startTs"].year, inp["startTs"].month, inp["startTs"].day))

            values.append(inp)

            ids.append([inp["jobId"], inp["startTs"]])

        self.LOG.info(f"{values}")

        #self.back_channel.execute( JOB_GQL, values )



    def run_sacct(self, sacct_bin_path='sacct', date='20231012', start_time='00:00:00', end_time='23:59:59' ):
        day = datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]))
        start = day.strftime("%Y-%m-%d")
        commandstr = f"""SLURM_TIME_FORMAT=%s {sacct_bin_path} --allusers --duplicates --allclusters --allocations --starttime="{start}T{start_time}" --endtime="{start}T{end_time}" --truncate --parsable2 --format=JobID,User,UID,Account,Partition,QOS,Submit,Start,End,Elapsed,NCPUS,AllocNodes,AllocTRES,CPUTimeRAW,NodeList,Reservation,ReservationId,State"""
        index = 0
        c = 0
        s = 0

        process = subprocess.Popen(commandstr, shell=True, stdout=subprocess.PIPE)

        for line in iter(process.stdout.readline, b''):
            fields = line.decode("utf-8").split("|")
            if index == 0 or ( len(fields) >= 10 and line.find(b"PENDING") == -1 and int(fields[7]) < int(fields[8]) and fields[9] != f"{start_time}" ):
                yield line.decode("utf-8").strip()
                if index > 0:
                    c += 1
                    s += int(fields[8]) - int(fields[7])
                index += 1
            else:
                self.LOG.info(f"skipping {line}")
        yield "--- %d %d" % (c, s)

    def convert( self, index, parts, default_facility='shared', default_repo='default' ):

        def conv(s, fx, default=None):
            try:
                return fx(s)
            except:
                return default

        def kilos_to_int(s):
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

        def calc_compute_time(startTs, endTs, submitTs, alloc_nodes, ncpus, tres):
            # calculate elapsed time as UTC so we don't get bitten by DST
            elapsed_secs = (endTs.astimezone(pytz.utc) - startTs.astimezone(pytz.utc)).total_seconds()
            wait_time = (startTs.astimezone(pytz.utc) - submitTs.astimezone(pytz.utc)).total_seconds() if submitTs else None

            # TODO: determine maximal amounts
            # if node is exclusive
            # max % of cpu, mem or gpu's for servers
            compute_time = elapsed_secs * ncpus / 3600.0 # TODO: this isn't right...
            return compute_time

        d = { field: parts[idx] for field, idx in index.items() }
        facility = default_facility
        repo = default_repo
        try:
            facility, repo = d["Account"].split(':')
        except Exception as e:
            self.LOG.warn(f"could not determine facility and repo from {d['Acount']}")

        nodelist = d["NodeList"]

        # convert to datetime
        startTs = parse_datetime(d['Start'])
        endTs = parse_datetime(d['End'])
        submitTs = parse_datetime(d['Submit'])

        # compute some values
        alloc_nodes = kilos_to_int(d['AllocNodes'])
        ncpus = conv(d['NCPUS'], int, 0)
        compute_time = calc_compute_time( startTs, endTs, submitTs,
                alloc_nodes, ncpus, d["AllocTRES"])

        #repoid = None #self.repos[f'{facility}:{repo}']['Id']
        #try:
        #    repoid = self.repos[d['Account']]['Id']
        #except Exception as e:
        #    self.LOG.warning(f"could not determine repoid from {d['Account']}")

        # determine appropriate allocation to charge against
        # use submitTs instead of startTs?
        allocId = self.get_alloc_id( facility, repo, d['Partition'], startTs )

        return {
            #'facility': facility,
            #'repo': repo,
            #'repoid': repoid,
            'jobId': conv(d['JobID'], int, 0),
            'username': d['User'],
            'uid': conv(d['UID'], int, 0),
            'accountName': d['Account'],
            'partitionName': d['Partition'],
            'allocationId': allocId,
            'qos': d['QOS'],
            #'submitTs': submitTs,
            'startTs': startTs,
            'endTs': endTs,
            'clustername': d['Partition'],
            'ncpus': ncpus,
            'allocNodes': alloc_nodes,
            'allocTres': d['AllocTRES'],
            'nodelist': nodelist,
            'reservation': None if d['Reservation'] == '' else d['Reservation'],
            'reservationId': None if d['ReservationId'] == '' else d['ReservationId'],
            'slachours': compute_time,
            #'submitter': None,
            #'officialImport': True
        }


class Coact(CommandManager):
    "A Manager class to register sub commands"
    LOG = logging.getLogger(__name__)

    def __init__(self, namespace, convert_underscores=True):
        super(Coact,self).__init__(namespace, convert_underscores=convert_underscores)
        for cmd in [ SlurmImport, ]:
            self.add_command( cmd.__name__.lower(), cmd )



#!/usr/bin/env python3

"""
Add to a group a list of users
"""


import os
import requests
from requests.auth import HTTPBasicAuth
import logging
import argparse
import json


logger = logging.getLogger(__name__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action='store_true', help="Turn on verbose logging")
    parser.add_argument("--url", help="URL to the grouper web service", default="https://identity.slac.stanford.edu/grouper-ws/servicesRest/v5_17_000/")
    parser.add_argument("--stem", help="The stem where we can find the groups ", default="app:Unix:posixGroups:s3df")
    parser.add_argument("--user", help="The user id to use", default="osmaint@slac.stanford.edu")
    parser.add_argument("--pwdfile", help="File containing the password", default=".pass")
    parser.add_argument("groupname", help="The name of the group")
    parser.add_argument("usernames", help="The names of the users", nargs="+")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger("requests.packages.urllib3").setLevel(logging.DEBUG)

    with open(args.pwdfile, "r") as f:
        auth=HTTPBasicAuth(args.user, f.read())

    reqjson = {
        "WsRestAddMemberRequest":{
            "wsGroupLookup":{
                "groupName": args.stem + ":" + args.groupname
            },
            "subjectLookups": [
            ]
        }
    }

    for user in args.usernames:
        reqjson["WsRestAddMemberRequest"]["subjectLookups"].append({
            "subjectId": user
        })

    logger.debug("Req json %s", json.dumps(reqjson, indent=4))

    resp = requests.post(args.url + "groups", auth=auth, json=reqjson)
    resp.raise_for_status()
    ret = resp.json()
    logger.debug(json.dumps(ret, indent=4))

    status = ret.get("WsAddMemberResults", {}).get("resultMetadata").get("resultCode",None)
    if status != "SUCCESS":
        raise Exception("Invalid Status")


#!/usr/bin/env python3

"""
Get the members in an S3DF group
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
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    with open(args.pwdfile, "r") as f:
        auth=HTTPBasicAuth(args.user, f.read())

    resp = requests.get(args.url + "groups/" + args.stem + ":" + args.groupname + "/members", auth=auth)
    resp.raise_for_status()
    ret = resp.json()
    logger.debug(json.dumps(ret, indent=4))

    status = ret.get("WsGetMembersLiteResult", {}).get("resultMetadata").get("resultCode",None)
    if status != "SUCCESS":
        raise Exception("Invalid Status")

    members = [ "--> " + x["id"] for x in ret.get("WsGetMembersLiteResult", {}).get("wsSubjects",[]) if x["sourceId"] == "slacPerson" ]
    print("Member for group "
        + ret.get("WsGetMembersLiteResult", {}).get("wsGroup", {}).get("extension") 
        + " integer index "
        + ret.get("WsGetMembersLiteResult", {}).get("wsGroup", {}).get("idIndex")
    )
    print("\n".join(members))

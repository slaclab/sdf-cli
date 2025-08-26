#!/usr/bin/env python3

"""
Find the user ( so that we can get their ID ) given their SLAC user id ( most likely AD ) 
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
    parser.add_argument("userid", help="The SLAC user id for the user; this will get expanded to userid@slac.stanford.edu")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    with open(args.pwdfile, "r") as f:
        auth=HTTPBasicAuth(args.user, f.read())

    resp = requests.post(args.url + "subjects", auth=auth, json={
        "WsRestGetSubjectsLiteRequest":{
            "searchString": args.userid
        }
    })
    resp.raise_for_status()
    ret = resp.json()
    logger.debug(json.dumps(ret, indent=4))

    status = ret.get("WsGetSubjectsResults", {}).get("resultMetadata").get("resultCode",None)
    if status != "SUCCESS":
        raise Exception("Invalid Status")

    for sub in ret.get("WsGetSubjectsResults", {}).get("wsSubjects", []):
       print("Subject --> "
           + sub.get("name")
           + " Id: "
           + sub.get("id")
       )

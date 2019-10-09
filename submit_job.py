#!/usr/bin/env python

'''
Submits a standard job via a REST call
'''

from __future__ import print_function
import json
import argparse
from hysds_commons.job_utils import submit_mozart_job

def parse_job_tags(tag_string):
    if tag_string == None or tag_string == '' or (type(tag_string) is list and tag_string == []) :
        return ''
    tag_list = tag_string.split(',')
    tag_list = ['"{0}"'.format(tag) for tag in tag_list]
    return ','.join(tag_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job_name', help='Job name', dest='job_name', required=True)
    parser.add_argument('--job_spec', help='Job spec', dest='job_spec', required=True)
    parser.add_argument('--params', help='Input params dict', dest='params', required=True)
    parser.add_argument('--rule', help='Input rule dict', dest='rule', required=True)
    args = parser.parse_args()

    rule = json.loads(args.rule)
    params = json.loads(args.params)


    print("submitting job of type {}".format(args.job_spec))
    print('submitting jobs with params:')
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    print('submitting jobs with rule:')
    print(json.dumps(rule, sort_keys=True, indent=4, separators=(',', ': ')))


    submit_mozart_job({}, rule,
        hysdsio={"id": "internal-temporary-wiring",
                 "params": params,
                 "job-specification": args.job_spec},
        job_name=args.job_name)



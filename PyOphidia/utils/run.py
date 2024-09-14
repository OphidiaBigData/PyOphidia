#!/usr/bin/env python

import os, argparse
import cwltool, cwltool.factory
from PyOphidia import Workflow, Experiment, client

parser = argparse.ArgumentParser()
parser.add_argument('name', type=str, help='Workflow file name')
parser.add_argument('--args', type=str, help='Workflow arguments')
args = parser.parse_args()

cwl_args = {}
param = None
if args.args:
    for i in args.args.split():
        if param is None:
            param = i
        else:
            cwl_args[param[2:]] = int(i) if i.isdigit() else i;
            param = None

fac = cwltool.factory.Factory()
fac.runtime_context.rm_tmpdir = False
cwl_tool = fac.make("./" + args.name)
result = cwl_tool(**cwl_args)

print(result)

json_request = result["outputexperiment"]["location"][7:]
#with open(json_request) as f:
#    print(f.read())

e1 = Experiment.load(json_request)
#e1.check()

os.remove(json_request)
os.rmdir(json_request.rsplit('/', 1)[0])

ophclient = client.Client(read_env=True)
Workflow.setclient(ophclient)

w1 = Workflow(e1)
w1.submit()


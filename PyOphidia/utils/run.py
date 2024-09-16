#
#     PyOphidia - Python bindings for Ophidia
#     Copyright (C) 2015-2024 CMCC Foundation
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

#!/usr/bin/env python

import os, argparse
import cwltool, cwltool.factory
from PyOphidia import Workflow, Experiment, client

parser = argparse.ArgumentParser()
parser.add_argument('name', type = str, help = 'Workflow file name')
parser.add_argument('--args', type = str, help = 'Workflow arguments')
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
if not e1.check():
    raise Exception("Experiment is not valid")

os.remove(json_request)
os.rmdir(json_request.rsplit('/', 1)[0])

ophclient = client.Client(read_env = True)
Workflow.setclient(ophclient)

w1 = Workflow(e1)
w1.submit()


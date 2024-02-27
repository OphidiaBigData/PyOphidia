#!/usr/bin/env python

import sys, getopt, os
from esdm_pav_client import Workflow, Experiment, Task
from PyOphidia import cube,client
import argparse

print("Parse arguments", file=sys.stderr)
parser = argparse.ArgumentParser()
# GENERAL TASK PARAMETERS
parser.add_argument('operator', type=str, help='Ophidia operator')
parser.add_argument('--name', type=str, help='Task name', required=True)
parser.add_argument('--experiment', type=str, help='JSON Experiment')
parser.add_argument('--experiment1', type=str, help='JSON Experiment')
parser.add_argument('--experiment2', type=str, help='JSON Experiment')
parser.add_argument('--description', type=str, help='Task description (optional)', default='-')
parser.add_argument('--dependencies', type=str, help='Task on which it depends')
parser.add_argument('--ncores', type=int, default=1)
parser.add_argument('--nthreads', type=int, default=1)
parser.add_argument('--on_error', type=str, default='abort')
parser.add_argument('--input', type=str, default='')
parser.add_argument('--output', type=str, default='')
# SPECIFIC TASK PARAMETERS 
parser.add_argument('--container', type=str, default='-')
parser.add_argument('--dim', type=str, default='-')
parser.add_argument('--dim_size', type=str)
parser.add_argument('--dim_type', type=str, default='double')
parser.add_argument('--exp_ndim', type=int)
parser.add_argument('--hierarchy', type=str, default='oph_base')
parser.add_argument('--exp_concept_level', type=str, default='c')
parser.add_argument('--exp_dim', type=str, default='auto')
parser.add_argument('--imp_concept_level', type=str, default='c')
parser.add_argument('--imp_dim', type=str, default='auto')
parser.add_argument('--src_path', type=str)
parser.add_argument('--measure', type=str)
parser.add_argument('--measure_type', type=str, default='double')
parser.add_argument('--import_metadata', type=str, default='yes')
parser.add_argument('--nfrag', type=int, default=0)
parser.add_argument('--ntuple', type=int, default=1)
parser.add_argument('--nhost', type=int, default=0)
parser.add_argument('--operation', type=str, default='sub')
parser.add_argument('--output_name', type=str, default='default')
parser.add_argument('--output_path', type=str, default='default')
# SCRIPT-SPECIFIC
parser.add_argument('--command', type=str, default=':')
parser.add_argument('--script', type=str, default=':')
parser.add_argument('--args', type=str, default='')
args = parser.parse_args()

print("Process task '" + args.name + "'", file=sys.stderr)

# Main parent task (if any)
e1 = None
t1 = None

# Other parent task (if any)
e2 = None
t2 = None

if args.experiment:
    e1 = Experiment.load(args.experiment)
    t1 = e1.tasks[-1]
elif args.experiment1:
    e1 = Experiment.load(args.experiment1)
    t1 = e1.tasks[-1]
    if args.experiment2:
        e2 = Experiment.load(args.experiment2)
        t2 = e2.tasks[-1]
else:
    e1 = Experiment("CWL workflow");

# Overwrite arguments
if args.input and len(args.input) > 0:
    if args.src_path:
        args.src_path = args.input
#if args.output and len(args.output) > 0:
#    if args.output_path:
#        args.output_path = args.output # TODO
#    if args.output_name:
#        args.output_name = args.output # TODO

# DEPENDENCIES between tasks
if args.dependencies and len(args.dependencies) > 0:
    t1 = e1.getTask(taskname=args.dependencies)

# ON_ERROR argument
on_error = args.on_error if args.on_error else 'abort'

# DESCRIPTION argument
description = args.description if args.description else '-'

# OPERATORS
if args.operator == 'oph_createcontainer':
    e1.newTask(name=args.name, type="ophidia", operator=args.operator, on_error=on_error,
                arguments={'container': args.container,
                           'dim': args.dim,
                           'dim_type': args.dim_type, 
                           'hierarchy': args.hierarchy, 
                           'description': description},
                dependencies={t1: ""} if t1 else {})
elif args.operator == 'oph_randcube':
    if not args.container:
        parser.error("Randcube operator requires container parameter")
    e1.newTask(name=args.name, type="ophidia", operator=args.operator, on_error=on_error,
                arguments={'container': args.container,
                           'measure_type': args.measure_type,
                           'exp_ndim': str(args.exp_ndim),
                           'dim': args.dim,
                           'dim_size': args.dim_size,
                           'measure': args.measure,
                           'nfrag': str(args.nfrag),
                           'ntuple': str(args.ntuple),
                           'description': description},
                dependencies={t1: ""} if t1 else {})
elif args.operator == 'oph_importnc2':
    if not args.measure or not args.src_path:
        parser.error("Import operator requires measure and input path parameters")
    e1.newTask(name=args.name, type="ophidia", operator=args.operator, on_error=on_error,
               arguments={'measure': args.measure,
                          'container': args.container,
                          'import_metadata': args.import_metadata,
                          'exp_dim': args.exp_dim, 
                          'exp_concept_level': args.exp_concept_level,
                          'imp_dim': args.imp_dim, 
                          'imp_concept_level': args.imp_concept_level,
                          'hierarchy': args.hierarchy,
                          'description': description, 
                          'input': args.src_path},
               dependencies={t1: ""} if t1 else {})
elif args.operator == 'oph_reduce':
    e1.newTask(name=args.name, type="ophidia", operator=args.operator, on_error=on_error,
                arguments={'operation': args.operation,
                           'description': description},
                dependencies={t1: "cube"} if t1 else {})
elif args.operator == 'oph_reduce2':
    if not args.operation:
        parser.error("Reduce2 operator requires operation parameters")
    e1.newTask(name=args.name, type="ophidia", operator=args.operator, on_error=on_error,
                arguments={'operation': args.operation,
                           'dim': args.dim,
                           'description': description},
                dependencies={t1: "cube"} if t1 else {})
elif args.operator == 'oph_intercube':
    if e2 is None:
        parser.error("Intercube operator requires two input cubes")
    for task in e2.tasks:
        if e1.getTask(task.name) is None:
            e1.addTask(task)
            print("Add task '" + task.name + "'", file=sys.stderr)
    e1.newTask(name=args.name, type="ophidia", operator=args.operator, on_error=on_error,
                arguments={'operation': args.operation,
                           'description': description},
                dependencies={t1: "cube", t2: "cube2"} if t1 and t2 else {})
elif args.operator == 'oph_script':
    e1.newTask(name=args.name, type="ophidia", operator=args.operator, on_error=on_error,
                arguments={'command': args.script,
                           'args': args.args,
                           'output': args.output,
                           'output_path': args.output_path,
                           'output_name': args.output_name,
                           'description': description},
                dependencies={t1: ""} if t1 else {})
else:
    # TODO: this part should be completed in order to set up a task 'oph_generic' used to run possible non-Ophidia tasks
    if args.command != ':':
        string_element = args.command.split()
        args.script = string_element.pop(0)
        if len(string_element) > 0:
            args.args += ' ' + ' '.join(str(x) for x in string_element)
    e1.newTask(name=args.name, type="ophidia", operator="oph_generic", on_error=on_error,
                arguments={'command': args.script,
                           'args': args.args,
                           'output': 'null', # Used to skip this Ophidia parameter
                           'description': description},
                dependencies={t1: ""} if t1 else {})
print("Add task '" + args.name + "'", file=sys.stderr)

print(repr(e1))

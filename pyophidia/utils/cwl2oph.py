#!/usr/bin/env python
#
#     PyOphidia - Python bindings for Ophidia
#     Copyright (C) 2015-2025 CMCC Foundation
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

import sys
import os
import argparse
from pyophidia import Experiment

previous_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.dirname(previous_dir))
sys.path.insert(0, "..")


def run():
    print("Parse arguments", file=sys.stderr)
    parser = argparse.ArgumentParser()
    parser.add_argument("operator", type=str, help="Ophidia operator")
    # GENERAL TASK PARAMETERS
    parser.add_argument("--cube", type=str, help="Input cube", default="")
    parser.add_argument("--cube2", type=str, help="Input cube", default="")
    parser.add_argument("--cubes", type=str, help="Input cube", default="")
    parser.add_argument(
        "--description", type=str, help="Task description", default="-"
    )
    parser.add_argument(
        "--dependencies", type=str, help="Task on which it depends"
    )
    parser.add_argument("--experiment", type=str, help="JSON Experiment")
    parser.add_argument("--experiment1", type=str, help="JSON Experiment")
    parser.add_argument("--experiment2", type=str, help="JSON Experiment")
    parser.add_argument("--input", type=str, help="Input", default="")
    parser.add_argument("--name", type=str, help="Task name", required=True)
    parser.add_argument(
        "--ncores", type=int, help="Number of cores", default=1
    )
    parser.add_argument(
        "--nthreads", type=int, help="Number of threads", default=1
    )
    parser.add_argument(
        "--on_error",
        type=str,
        help="Behaivior in case of errors",
        default="abort",
    )
    parser.add_argument("--output", type=str, help="Output", default="")
    # SPECIFIC TASK PARAMETERS
    parser.add_argument("--args", type=str, default="")
    parser.add_argument("--command", type=str, default=":")
    parser.add_argument("--concept_level", type=str, default="c")
    parser.add_argument("--concept_level_reduce", type=str, default="A")
    parser.add_argument("--condition", type=str, default="1")
    parser.add_argument("--container", type=str, default="-")
    parser.add_argument("--counter", type=str, default="-")
    parser.add_argument("--cube2_is_array", type=str, default="no")
    parser.add_argument("--dim", type=str, default="-")
    parser.add_argument("--dim_pos", type=str)
    parser.add_argument("--dim_size", type=str)
    parser.add_argument("--dim_type", type=str, default="double")
    parser.add_argument("--exp_concept_level", type=str, default="c")
    parser.add_argument("--exp_dim", type=str, default="auto")
    parser.add_argument("--exp_ndim", type=int)
    parser.add_argument("--export_metadata", type=str, default="yes")
    parser.add_argument("--extension_type", type=str, default="none")
    parser.add_argument("--force", type=str, default="no")
    parser.add_argument("--forward", type=str, default="no")
    parser.add_argument("--group_size", type=str, default="all")
    parser.add_argument("--hierarchy", type=str, default="oph_base")
    parser.add_argument("--hold_values", type=str, default="no")
    parser.add_argument("--host_partition", type=str, default="auto")
    parser.add_argument("--imp_concept_level", type=str, default="c")
    parser.add_argument("--imp_dim", type=str, default="auto")
    parser.add_argument("--import_metadata", type=str, default="yes")
    parser.add_argument("--ioserver", type=str, default="ophidiaio_memory")
    parser.add_argument("--key", type=str)
    parser.add_argument("--keys", type=str, default="-")
    parser.add_argument("--measure", type=str)
    parser.add_argument("--measure_name", type=str, default="-")
    parser.add_argument("--measure_type", type=str, default="manual")
    parser.add_argument("--metadata_key", type=str, default="all")
    parser.add_argument("--metadata_type", type=str, default="text")
    parser.add_argument("--metadata_value", type=str, default="-")
    parser.add_argument("--mode", type=str, default="read")
    parser.add_argument("--ndim", type=int, default=1)
    parser.add_argument("--nfrag", type=int, default=0)
    parser.add_argument("--nhost", type=int, default=0)
    parser.add_argument("--number", type=int, default=1)
    parser.add_argument("--ntuple", type=int, default=1)
    parser.add_argument("--operation", type=str, default="sub")
    parser.add_argument("--output_name", type=str, default="default")
    parser.add_argument("--output_path", type=str, default="default")
    parser.add_argument("--parallel", type=str, default="no")
    parser.add_argument("--query", type=str, default="measure")
    parser.add_argument("--script", type=str, default=":")
    parser.add_argument("--space", type=str, default="no")
    parser.add_argument("--src_path", type=str, default="")
    parser.add_argument("--subset_dims", type=str, default="none")
    parser.add_argument("--subset_filter", type=str, default="all")
    parser.add_argument("--subset_type", type=str, default="index")
    parser.add_argument("--value", type=str, default="-")
    parser.add_argument("--values", type=str, default="-")
    parser.add_argument("--variable", type=str, default="global")
    args = parser.parse_args()

    print("Process task '" + args.name + "'", file=sys.stderr)

    # Main parent task (if any)
    e1 = None
    t1 = None

    # Other parent tasks (if any)
    en = []

    if args.experiment:
        exps = args.experiment.split(",")
        nexps = len(exps)
        if nexps > 0:
            e1 = Experiment.load(exps[0])
            t1 = e1.tasks[-1]
            if nexps > 1:
                for i in range(1, nexps):
                    en.append(Experiment.load(exps[i]))
    elif args.experiment1:
        e1 = Experiment.load(args.experiment1)
        t1 = e1.tasks[-1]
        if args.experiment2:
            e2 = Experiment.load(args.experiment2)
            t2 = e2.tasks[-1]
    else:
        e1 = Experiment("CWL workflow")

    # Overwrite arguments
    if args.input and len(args.input) > 0:
        if args.src_path:
            args.src_path = args.input
    # if args.output and len(args.output) > 0:
    #    if args.output_path:
    #        args.output_path = args.output # TODO
    #    if args.output_name:
    #        args.output_name = args.output # TODO

    # DEPENDENCIES between tasks
    if args.dependencies and len(args.dependencies) > 0:
        t1 = e1.getTask(taskname=args.dependencies)

    # ON_ERROR argument
    on_error = args.on_error if args.on_error else "abort"

    # DESCRIPTION argument
    description = args.description if args.description else "-"

    # CUBE argument
    arg_cube = "cube"
    arg_cube2 = "cube2"
    if args.cube and len(args.cube) > 0:
        arg_cube = ""
    if args.cube2 and len(args.cube2) > 0:
        arg_cube2 = ""

    # DEPENDENCIES
    dependencies = {t1: arg_cube} if t1 else {}
    for ee in en:
        for task in ee.tasks:
            if e1.getTask(task.name) is None:
                e1.addTask(task)
                print("Add task '" + task.name + "'", file=sys.stderr)
        tt = ee.tasks[-1]
        if tt:
            dependencies[tt] = arg_cube

    # OPERATORS
    if args.operator == "oph_apply":
        arguments = {
            "query": args.query,
            "measure_type": args.measure_type,
            "ncores": str(args.ncores),
            "nthreads": str(args.nthreads),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        if args.measure_name != "-":
            arguments["measure"] = args.measure_name
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_createcontainer":
        for task in dependencies.keys():
            dependencies[task] = ""
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments={
                "container": args.container,
                "dim": args.dim,
                "dim_type": args.dim_type,
                "hierarchy": args.hierarchy,
                "description": description,
            },
            dependencies=dependencies,
        )
    elif args.operator == "oph_delete":
        arguments = {
            "force": args.force,
            "ncores": str(args.ncores),
            "nthreads": str(args.nthreads),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_deletecontainer":
        for task in dependencies.keys():
            dependencies[task] = ""
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments={
                "container": args.container,
                "force": args.force,
                "description": description,
            },
            dependencies=dependencies,
        )
    elif (
        args.operator == "oph_else"
        or args.operator == "oph_endif"
        or args.operator == "oph_endfor"
    ):
        arguments = {
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_elseif":
        arguments = {
            "condition": args.condition,
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_exportnc":
        arguments = {
            "force": args.force,
            "export_metadata": args.export_metadata,
            "output": args.output,
            "output_path": args.output_path,
            "output_name": args.output_name,
            "ncores": str(args.ncores),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_exportnc2":
        arguments = {
            "force": args.force,
            "export_metadata": args.export_metadata,
            "output": args.output,
            "output_path": args.output_path,
            "output_name": args.output_name,
            "ncores": str(args.ncores),
            "nthreads": str(args.nthreads),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_for":
        arguments = {
            "key": args.key,
            "values": args.values,
            "counter": args.counter,
            "input": args.input,
            "parallel": args.parallel,
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_generic":
        if args.command == ":":
            parser.error("Generic operator requires a command")
        for task in dependencies.keys():
            dependencies[task] = ""
        arguments = {
            "command": args.command,
            "args": args.args,
            "space": args.space,
            "input": args.input,
            "output": args.output,
            "output_path": args.output_path,
            "output_name": args.output_name,
            "force": args.force,
            "ncores": str(args.ncores),
            "description": description,
        }
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_if":
        arguments = {
            "condition": args.condition,
            "forward": args.forward,
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_importnc":
        for task in dependencies.keys():
            dependencies[task] = ""
        if not args.measure or not args.src_path:
            parser.error(
                "Import operator requires measure and input path parameters"
            )
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments={
                "measure": args.measure,
                "container": args.container,
                "import_metadata": args.import_metadata,
                "exp_dim": args.exp_dim,
                "exp_concept_level": args.exp_concept_level,
                "imp_dim": args.imp_dim,
                "imp_concept_level": args.imp_concept_level,
                "hierarchy": args.hierarchy,
                "host_partition": args.host_partition,
                "ioserver": str(args.ioserver),
                "subset_dims": args.subset_dims,
                "subset_filter": args.subset_filter,
                "subset_type": args.subset_type,
                "ncores": str(args.ncores),
                "description": description,
                "input": args.src_path,
            },
            dependencies=dependencies,
        )
    elif args.operator == "oph_importnc2" or args.operator == "oph_importncs":
        for task in dependencies.keys():
            dependencies[task] = ""
        if not args.measure or not args.src_path:
            parser.error(
                "Import operator requires measure and input path parameters"
            )
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments={
                "measure": args.measure,
                "container": args.container,
                "import_metadata": args.import_metadata,
                "exp_dim": args.exp_dim,
                "exp_concept_level": args.exp_concept_level,
                "imp_dim": args.imp_dim,
                "imp_concept_level": args.imp_concept_level,
                "hierarchy": args.hierarchy,
                "host_partition": args.host_partition,
                "subset_dims": args.subset_dims,
                "subset_filter": args.subset_filter,
                "subset_type": args.subset_type,
                "ncores": str(args.ncores),
                "nthreads": str(args.nthreads),
                "description": description,
                "input": args.src_path,
            },
            dependencies=dependencies,
        )
    elif args.operator == "oph_intercube":
        if e2 is None:
            parser.error("Intercube operator requires two input cubes")
        for task in e2.tasks:
            if e1.getTask(task.name) is None:
                e1.addTask(task)
                print("Add task '" + task.name + "'", file=sys.stderr)
        arguments = {
            "operation": args.operation,
            "measure": args.measure,
            "cube2_is_array": args.cube2_is_array,
            "extension_type": args.extension_type,
            "container": args.container,
            "ncores": str(args.ncores),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        if args.cube2 and len(args.cube2) > 0:
            arguments["cube2"] = args.cube2
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies={t1: arg_cube, t2: arg_cube2} if t1 and t2 else {},
        )
    elif args.operator == "oph_intercube2":
        for task in dependencies.keys():
            dependencies[task] = "cubes"
        arguments = {
            "operation": args.operation,
            "measure": args.measure,
            "ncores": str(args.ncores),
            "container": args.container,
            "description": description,
        }
        if args.cubes and len(args.cubes) > 0:
            arguments["cubes"] = args.cubes
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_mergecubes":
        for task in dependencies.keys():
            dependencies[task] = "cubes"
        arguments = {
            "mode": args.mode,
            "hold_values": args.hold_values,
            "number": str(args.number),
            "ncores": str(args.ncores),
            "container": args.container,
            "description": description,
        }
        if args.cubes and len(args.cubes) > 0:
            arguments["cubes"] = args.cubes
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_mergecubes2":
        for task in dependencies.keys():
            dependencies[task] = "cubes"
        arguments = {
            "dim": args.dim,
            "dim_type": args.dim_type,
            "number": str(args.number),
            "ncores": str(args.ncores),
            "container": args.container,
            "description": description,
        }
        if args.cubes and len(args.cubes) > 0:
            arguments["cubes"] = args.cubes
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_metadata":
        arguments = {
            "mode": args.mode,
            "variable": args.variable,
            "metadata_key": args.metadata_key,
            "metadata_type": args.metadata_type,
            "metadata_value": args.metadata_value,
            "force": args.force,
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_permute":
        arguments = {
            "dim_pos": args.dim_pos,
            "ncores": str(args.ncores),
            "nthreads": str(args.nthreads),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_randcube":
        if not args.container:
            parser.error("Randcube operator requires container parameter")
        for task in dependencies.keys():
            dependencies[task] = ""
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments={
                "container": args.container,
                "measure_type": args.measure_type,
                "exp_ndim": str(args.exp_ndim),
                "dim": args.dim,
                "concept_level": args.concept_level,
                "dim_size": args.dim_size,
                "measure": args.measure,
                "nfrag": str(args.nfrag),
                "ntuple": str(args.ntuple),
                "host_partition": args.host_partition,
                "ioserver": str(args.ioserver),
                "ncores": str(args.ncores),
                "description": description,
            },
            dependencies=dependencies,
        )
    elif args.operator == "oph_randcube2":
        if not args.container:
            parser.error("Randcube operator requires container parameter")
        for task in dependencies.keys():
            dependencies[task] = ""
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments={
                "container": args.container,
                "measure_type": args.measure_type,
                "exp_ndim": str(args.exp_ndim),
                "dim": args.dim,
                "concept_level": args.concept_level,
                "dim_size": args.dim_size,
                "measure": args.measure,
                "nfrag": str(args.nfrag),
                "ntuple": str(args.ntuple),
                "host_partition": args.host_partition,
                "ncores": str(args.ncores),
                "nthreads": str(args.nthreads),
                "description": description,
            },
            dependencies=dependencies,
        )
    elif args.operator == "oph_reduce":
        arguments = {
            "operation": args.operation,
            "group_size": args.group_size,
            "ncores": str(args.ncores),
            "nthreads": str(args.nthreads),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_reduce2":
        if not args.operation:
            parser.error("Reduce2 operator requires operation parameters")
        arguments = {
            "operation": args.operation,
            "dim": args.dim,
            "concept_level": args.concept_level_reduce,
            "ncores": str(args.ncores),
            "nthreads": str(args.nthreads),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_rollup":
        arguments = {
            "ndim": str(args.ndim),
            "ncores": str(args.ncores),
            "nthreads": str(args.nthreads),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_script":
        for task in dependencies.keys():
            dependencies[task] = ""
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments={
                "script": args.script,
                "args": args.args,
                "space": args.space,
                "description": description,
            },
            dependencies=dependencies,
        )
    elif args.operator == "oph_set":
        arguments = {
            "key": args.key,
            "keys": args.keys,
            "value": args.value,
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    elif args.operator == "oph_subset":
        arguments = {
            "subset_dims": args.subset_dims,
            "subset_filter": args.subset_filter,
            "subset_type": args.subset_type,
            "ncores": str(args.ncores),
            "nthreads": str(args.nthreads),
            "description": description,
        }
        if args.cube and len(args.cube) > 0:
            arguments["cube"] = args.cube
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator=args.operator,
            on_error=on_error,
            arguments=arguments,
            dependencies=dependencies,
        )
    else:
        # TODO
        # This part should be completed in order to set up a task
        # 'oph_generic' used to run possible non-Ophidia tasks
        if args.command != ":":
            string_element = args.command.split()
            args.script = string_element.pop(0)
            if len(string_element) > 0:
                args.args += " " + " ".join(str(x) for x in string_element)
        for task in dependencies.keys():
            dependencies[task] = ""
        e1.newTask(
            name=args.name,
            type="ophidia",
            operator="oph_generic",
            on_error=on_error,
            arguments={
                "command": args.script,
                "args": args.args,
                "space": args.space,
                "output": "null",
                "description": description,
            },  # Used to skip this Ophidia parameter
            dependencies=dependencies,
        )
    print("Add task '" + args.name + "'", file=sys.stderr)
    print(repr(e1))


if __name__ == "__main__":
    run()

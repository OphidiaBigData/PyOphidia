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

import click
import sys
import os

previous_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.dirname(previous_dir))
sys.path.insert(0, "..")
from PyOphidia import Workflow, Experiment


def verbose_check_display(verbose, text):
    if verbose:
        click.echo(text)


def print_help():
    ctx = click.get_current_context()
    click.echo(ctx.get_help())
    ctx.exit()


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
    )
)
@click.option("-v", "--verbose", is_flag=True, help="Will print verbose messages")
@click.option(
    "-S",
    "--server",
    help="Ophidia Server address (used in case of remote submission)",
    default="127.0.0.1",
    metavar="<IP address>",
)
@click.option(
    "-P",
    "--port",
    help="Ophidia Server port (used in case of remote submission)",
    default="11732",
    metavar="<port number>",
)
@click.option(
    "-m",
    "--monitor",
    is_flag=True,
    help="Display a graph of the experiment workflow execution status",
)
@click.option(
    "-s",
    "--sync_mode",
    is_flag=True,
    help="Change the execution mode to synchronous",
)
@click.option(
    "-c",
    "--cancel",
    help="Cancel the experiment execution. It only works in asynchronous mode",
    is_flag=True,
)
# @click.argument("workflow", type=click.UNPROCESSED)
@click.option(
    "-i",
    "--id",
    help="Id of a running/completed/failed experiment workflow to cancel/monitor/restart",
    type=int,
    metavar="<id>",
)
@click.option(
    "-w",
    "--workflow",
    help="Run the experiment workflow from the provided document (JSON file)",
    type=str,
    metavar="<JSON document>",
)
@click.option(
    "-p",
    "--checkpoint",
    help="The checkpoint used to restart the experiment workflow",
    type=str,
    metavar="<checkpoint name>",
)
@click.argument("workflow_args", nargs=-1, type=click.UNPROCESSED)
def run(verbose, server, port, monitor, sync_mode, cancel, workflow, workflow_args, id, checkpoint):
    """Command Line Interface to run an experiment\n
    Example: wclient -w experiment.json 1 2"""

    def modify_args(workflow, server, port):
        if workflow.startswith("="):
            workflow = workflow[1:]
        if server.startswith("="):
            server = server[1:]
        if port.startswith("="):
            port = port[1:]
        return workflow, server, port

    def extract_other_args(wf_args):
        args = []
        for c in wf_args:
            if c.startswith("="):
                args.append(c[1:])
            elif not c.startswith("-"):
                args.append(c)
        return args

    if workflow:
        workflow, server, port = modify_args(workflow, server, port)
        args = extract_other_args(workflow_args)
        verbose_check_display(verbose, "Reading the experiment document")
        e1 = Experiment.load(workflow)
        w1 = Workflow(e1)
        if not sync_mode:
            e1.exec_mode = "sync"
            verbose_check_display(
                verbose,
                "Submitting the experiment workflow in synchronous mode",
            )
            w1.submit(server=server, port=port, *args)
            verbose_check_display(
                True,
                "Submitted! Workflow id = {0}".format((str(w1.workflow_id))),
            )
            if monitor:
                w1.monitor(frequency=5, iterative=True, visual_mode=True)
        else:
            verbose_check_display(
                verbose,
                "Submitting the experiment workflow in asynchronous mode",
            )
            w1.submit(server=server, port=port, *args)
            verbose_check_display(
                True,
                "Submitted! Workflow id = {0}".format((str(w1.workflow_id))),
            )
            w1.monitor(frequency=5, iterative=True, visual_mode=True)
        return 0
    elif cancel:
        if not id:
            verbose = True
            verbose_check_display(
                verbose,
                "Id of the experiment workflow to be cancelled is required",
            )
            return 1
        else:
            verbose_check_display(
                verbose,
                "Will cancel the experiment workflow execution: {0}".format(str(id)),
            )
            w1 = Workflow(id)
            w1.cancel()
            return 0
    elif monitor:
        if not id:
            verbose = True
            verbose_check_display(
                verbose,
                "Id of the experiment workflow to be monitored is required",
            )
            return 1
        else:
            verbose_check_display(
                verbose,
                "Will monitor the experiment workflow execution: {0}".format(str(id)),
            )
            w1 = Workflow(id)
            w1.monitor(frequency=5, iterative=True, visual_mode=True)
            return 0
    elif checkpoint:
        if not id:
            verbose = True
            verbose_check_display(
                verbose,
                "Id of the experiment workflow to be restarted from checkpoint is required",
            )
            return 1
        w1 = Workflow(id)
        if not sync_mode:
            e1.exec_mode = "sync"
            verbose_check_display(
                verbose,
                "Submitting the experiment workflow in synchronous mode",
            )
            w1.submit(server=server, port=port, *args, checkpoint=checkpoint)
            verbose_check_display(
                True,
                "Submitted! Workflow id = {0}".format((str(w1.workflow_id))),
            )
            if monitor:
                w1.monitor(frequency=5, iterative=True, visual_mode=True)
        else:
            verbose_check_display(
                verbose,
                "Submitting the experiment workflow in asynchronous mode",
            )
            w1.submit(server=server, port=port, *args, checkpoint=checkpoint)
            verbose_check_display(
                True,
                "Submitted! Workflow id = {0}".format((str(w1.workflow_id))),
            )
            w1.monitor(frequency=5, iterative=True, visual_mode=True)
        return 0
    else:
        print_help()


if __name__ == "__main__":
    run()

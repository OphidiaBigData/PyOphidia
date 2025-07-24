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

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import os
import json
import time
import re
import copy
from inspect import currentframe

sys.path.append(os.path.dirname(__file__))


def _get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno


def _dependency_check(dependency):
    from importlib.util import find_spec

    if dependency == "prov":
        if not find_spec("prov.model") or not find_spec("prov.dot"):
            raise ImportError("prov and/or pydot are not installed")
    elif dependency == "cwltool":
        if not find_spec("cwltool") or not find_spec("cwltool.factory"):
            raise ImportError("cwltool is not installed")
    elif dependency == "graphviz":
        if not find_spec("graphviz") or not find_spec("IPython.display"):
            raise ImportError("graphviz and/or ipython are not installed")
    else:
        raise AttributeError("Dependency must be prov, cwl or graphviz")


class Task:
    """
    Creates a Task object that can be embedded in a workflow experiment
    workflow

    Construction::
    t1 = Task(name="Sample task", operator="oph_reduce",
                arguments={'operation': 'avg'})

    Parameters
    ----------
    operator: str
        operator name
    arguments: dict, optional
        list of user-defined operator arguments as key=value pairs
    name: str, optional
        unique task name
    type: str, optional
        type of the task
    on_error: str, optional
        behaviour in case of error
    """

    attributes = ["run", "on_error", "type"]
    active_attributes = ["name", "operator", "arguments"]

    def __init__(self, operator, arguments={}, name=None, type=None, **kwargs):
        for k in kwargs.keys():
            if k not in self.attributes:
                raise AttributeError("Unknown Task argument: {0}".format(k))
        self.type = type if type else "ophidia"
        self.name = name
        self.operator = operator
        self.arguments = [
            "{0}={1}".format(k, arguments[k]) for k in arguments.keys()
        ]
        self.dependencies = []
        self.extra = {}
        self.__dict__.update(kwargs)

    def deinit(self):
        """
        Reverse the initialization of the object
        """
        for k in self.active_attributes:
            self.__delattr__(k)

    def addDependency(self, task, argument=None):
        """
        Adds task as a dependency of the current one

        Parameters
        ----------
        task: <class 'pyophidia.workflow.Task'>
            task the current one depends on
        argument: str, optional
            argument to be set with the output of the task 'task'

        Raises
        ------
        AttributeError
            When one of the parameters has the wrong type

        Example
        -------
        t2 = Task(name="Sample task1", operator='oph_reduce',
                    arguments={'operation': 'avg'})
        t3 = Task(name="Sample task2", operator='oph_aggregate',
                    arguments={'operation': 'max'})
        t3.addDependency(t2)
        """

        def parameter_check(task, argument):
            if argument is not None and not isinstance(argument, str):
                raise AttributeError("argument must be string")
            if not isinstance(task, Task):
                raise AttributeError("task must be Task object")

        parameter_check(task, argument)
        dependency_dict = {}
        if argument:
            dependency_dict["argument"] = argument
        dependency_dict["task"] = task.__dict__["name"]
        self.dependencies.append(dependency_dict)

    def copyDependency(self, dependency):
        """
        Copy a dependency instead of using addDependency, when it has the
        proper format

        Parameters
        ----------
        dependency: dict
            Copy a dependency to a task
        """
        self.dependencies.append(dependency)

    def reverted_arguments(self):
        """
        Changes the format of the arguments

        Returns
        -------
        arguments: dict
            returns the arguments with the newest format
        """
        arguments = {}
        for arg in self.arguments:
            arguments[arg.split("=")[0]] = arg.split("=")[1]
        return arguments


class Experiment:
    """
    Creates or loads a workflow experiment.

    An experiment is a sequence of tasks. Each task can be either independent
    or dependent on other tasks, for instance it processes the output of other
    tasks.

    Construction::
    e1 = Experiment(name="sample", author="sample author",
                    abstract="sample abstract", on_error=None, run=None,
                    ncores=1, nthreads=None, host_partition=None)

    Parameters
    ----------
    name: str
        experiment name
    author: str, optional
        experiment author
    abstract: str, optional
        experiment description
    on_error: str, optional
        behaviour in case of error
    on_exit: str, optional
        behaviour in case of completion
    run: str, optional
        enable actual execution, yes or no
    nthreads: str, optional
        number of threads
    host_partition: str, optional
        name of the host partition to be used

    """

    attributes = [
        "exec_mode",
        "on_error",
        "on_exit",
        "run",
        "nthreads",
        "ncores",
        "host_partition",
    ]
    active_attributes = ["name", "author", "abstract"]
    task_attributes = ["run", "on_error", "on_exit", "type"]
    task_name_counter = 1
    subexperiment_names = []

    def __init__(self, name, author=None, abstract=None, **kwargs):
        for k in kwargs.keys():
            if k not in self.attributes:
                raise AttributeError(
                    "Unknown experiment argument: {0}".format(k)
                )
            self.active_attributes.append(k)
        self.name = name
        self.author = author if author is not None else ""
        self.abstract = abstract if abstract is not None else ""
        self.exec_mode = "sync"
        self.tasks = []
        self.__dict__.update(kwargs)

    @staticmethod
    def _notebook_check():
        try:
            shell = get_ipython().__class__.__name__
            if shell == "ZMQInteractiveShell":
                return True
            elif shell == "TerminalInteractiveShell":
                return False
            else:
                return False
        except NameError:
            return False

    def __param_check(self, params=[]):
        for param in params:
            if "NoneValue" in param.keys():
                if (
                    not isinstance(param["value"], param["type"])
                    and param["value"] is not None
                ):
                    raise AttributeError(
                        "{0} should be {1}".format(
                            param["name"], param["type"]
                        )
                    )
            else:
                if not isinstance(param["value"], param["type"]):
                    raise AttributeError(
                        "{0} should be {1}".format(
                            param["name"], param["type"]
                        )
                    )

    def workflow_to_json(self):
        non_experiment_fields = ["task_name_counter"]
        new_experiment = {
            k: dict(self.__dict__)[k]
            for k in dict(self.__dict__).keys()
            if k not in non_experiment_fields
        }
        if "tasks" in new_experiment.keys():
            new_experiment["tasks"] = [
                t.__dict__ for t in new_experiment["tasks"]
            ]
        return new_experiment

    def __repr__(self):
        return json.dumps(self.workflow_to_json())

    def deinit(self):
        """
        Reverse the initialization of the object
        """
        for k in self.active_attributes:
            self.__delattr__(k)

    def addTask(self, task):
        """
        Add a task to the experiment

        Parameters
        ----------
        task: <class 'pyophidia.workflow.Task'>
            Task to be added to the experiment

        Raises
        ------
        AttributeError
            If the task name is already in the experiment or if a dependency is
            not fulfilled

        Example
        -------
        t1 = Task(name="sample task", operator='oph_reduce',
                    arguments={'operation': 'avg'})
        e1.addTask(t1)
        """
        if "name" not in task.__dict__.keys() or task.name is None:
            task.name = self.name + "_{0}".format(self.task_name_counter)
        if task.__dict__["name"] in [t.__dict__["name"] for t in self.tasks]:
            raise AttributeError("task already exists")
        if task.__dict__["dependencies"]:
            for dependency in task.__dict__["dependencies"]:
                if dependency["task"] not in [
                    task.__dict__["name"] for task in self.tasks
                ]:
                    raise AttributeError("dependency not fulfilled")
        self.task_name_counter += 1
        self.tasks.append(task)

    def getTask(self, taskname):
        """
        Retrieve the Task object from the workflow experiment with the given
            task name

        Parameters
        ----------
        taskname: str
            The name of the task to be found in the experiment

        Returns
        -------
        task: <class 'pyophidia.workflow.Task'>
            Returns the first task found
        None: Nonetype
            If no task was found then returns None

        Example
        -------
        t1 = Task(name="task_one", operator="oph_reduce",
                    arguments={'operation': 'avg'})
        task = e1.getTask(taskname="task_one")
        """
        tasks = [t for t in self.tasks if t.name == taskname]
        if len(tasks) == 1:
            return tasks[0]
        elif len(tasks) == 0:
            return None

    def save(self, experimentname, format="json"):
        """
        Save the experiment as a JSON document

        Parameters
        ----------
        experimentname: str
            The path to the file where the experiment is being saved
        format: str
            The format of the file to be created, extension to be append to the
            file name

        Example
        -------
        from pyophidia import Experiment
        e1 = Experiment(name="sample name", author="sample author",
                        abstract="sample abstract")
        e1.save("sample_experiment")

        Raises
        ------
        AttributeError
            If worfklowname is not a string or it is empty
        """

        if not isinstance(experimentname, str):
            raise AttributeError("experimentname must be string")
        if len(experimentname) == 0:
            raise AttributeError(
                "experimentname must contain more than 1 characters"
            )
        if not experimentname.endswith("." + format):
            experimentname += "." + format
        with open(os.path.join(os.getcwd(), experimentname), "w") as fp:
            if format == "json":
                data = self.workflow_to_json()
                json.dump(data, fp, indent=4)
            else:
                raise AttributeError("format not allowed")

    def newTask(
        self, operator, arguments={}, dependencies={}, name=None, **kwargs
    ):
        """
        Add a new Task in the experiment without the need of creating a Task
            object

        Attributes
        ----------
        operator: str
            operator name
        arguments: dict, optional
            dict of user-defined operator arguments as key=value pairs
        dependencies: dict, optional
            a dict of dependencies for the task
        name: str, optional
            the name of the task
        type: str, optional
            type of the task
        on_error: str, optional
            behaviour in case of error
        on_exit: str, optional
            behaviour in case of completion
        run: str, optional
            enable actual execution, yes or no

        Returns
        -------
        t: <class 'pyophidia.workflow.Task'>
            Returns the task that was created and added to the experiment

        Raises
        ------
        AttributeError
            Raises an AttributeError if the given arguments are not of the
            proper type or are not defined by the schema

        Example
        -------
        e1 = Experiment(name="Experiment 1", author="sample author",
                        abstract="sample abstract")
        t1 = e1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                          dependencies={})
        """
        self.__param_check(
            [
                {"name": "operator", "value": operator, "type": str},
                {"name": "arguments", "value": arguments, "type": dict},
                {"name": "dependencies", "value": dependencies, "type": dict},
                {
                    "name": "name",
                    "value": name,
                    "type": str,
                    "NoneValue": True,
                },
            ]
        )
        t = Task(operator=operator, arguments=arguments, name=name)
        if dependencies:
            for k in dependencies.keys():
                if dependencies[k]:
                    t.addDependency(task=k, argument=dependencies[k])
                else:
                    t.addDependency(task=k)
        for k in kwargs.keys():
            if k not in self.task_attributes:
                raise AttributeError("Unknown Task argument: {0}".format(k))
        t.__dict__.update(kwargs)
        self.addTask(t)
        return t

    def newSubexperiment(self, experiment, params, dependency={}):
        """
        Embed an experiment into another experiment

        Parameters
        ----------
        experiment: <class 'pyophidia.workflow.Experiment'>
            The experiment that will be embeded into our main experiment
        params: dict of keywords
            a dict of keywords that will be used to replace placeholders in
            the tasks
        dependencies: dict, optional
            list of dependencies

        Returns
        -------
        The last task of the subexperiment in case the user wants to use it as
        a dependency.

        Raises
        ------
        AttributeError
            Raises AttributeError when there's an error with the experiments
            (same name or non-existent), or when the dependencies are not
            fulfilled

        Example
        -------
        e1 = Experiment(name="Experiment 1", author="sample author 1",
                        abstract="sample abstract 1")
        e2 = Experiment(name="Experiment 2", author="sample author 2",
                        abstract="sample abstract 2")
        t1 = e2.newTask(operator='oph_reduce', arguments={'operation': 'avg'})
        task_array = e1.newSubexperiment(experiment=e2, params={},
                        dependency={})
        """

        def validate_experiment(e1, e2):
            if not isinstance(e2, Experiment) or e1.name == e2.name:
                raise AttributeError("Wrong experiment or same experiments")

        def rename_tasks(e2):
            def _get_flag_id():

                greatest_id = 1
                for task in self.tasks:
                    if "_{subexperiment_" in task.name:
                        task_id = int(
                            re.findall(r"_{subexperiment_(.*)}", task.name)[0]
                        )
                        if task_id >= greatest_id:
                            greatest_id = task_id + 1
                return greatest_id

            flag_id = _get_flag_id()
            for task in e2.tasks:
                task.name = task.name + "_{subexperiment_ID}".replace(
                    "ID", str(flag_id)
                )
            return e2

        def check_replace_args(params, task_arguments):

            new_task_arguments = {}
            for k in task_arguments:
                if re.search(r"(\$.*)", k):
                    if re.findall(r"(\$.*)", k)[0] in params.keys():
                        new_task_arguments[
                            re.sub(
                                r"(\$.*)",
                                params[re.findall(r"(\$.*)", k)[0]],
                                k,
                            )
                        ] = task_arguments[k]
                    else:
                        new_task_arguments[k] = task_arguments[k]
                else:
                    new_task_arguments[k] = task_arguments[k]
            for k in task_arguments:
                if re.search(r"(\$.*)", task_arguments[k]):
                    if (
                        re.findall(r"(\$.*)", task_arguments[k])[0]
                        in params.keys()
                    ):
                        new_task_arguments[k] = re.sub(
                            r"(\$.*)",
                            params[
                                re.findall(r"(\$.*)", task_arguments[k])[0]
                            ],
                            task_arguments[k],
                        )
                    else:
                        new_task_arguments[k] = task_arguments[k]
                else:
                    new_task_arguments[k] = task_arguments[k]

            return new_task_arguments

        self.__param_check(
            [
                {
                    "name": "experiment",
                    "value": experiment,
                    "type": Experiment,
                },
                {"name": "params", "value": params, "type": dict},
                {"name": "dependency", "value": dependency, "type": dict},
                # {"name": "name", "value": name, "type": str,
                #  "NoneValue": True},
            ]
        )
        copied_experiment = copy.deepcopy(experiment)
        validate_experiment(self, copied_experiment)
        copied_experiment = rename_tasks(copied_experiment)
        for task in copied_experiment.tasks:
            new_arguments = check_replace_args(
                params, task.reverted_arguments()
            )
            task.arguments = new_arguments
            self.tasks.append(task)
        return copied_experiment.tasks[-1]

    @staticmethod
    def json_open(filename):
        if not os.path.isfile(filename):
            raise IOError("File does not exist")

        try:
            from client import Client
        except ImportError:
            from .client import Client
        client = Client(
            local_mode=True,
        )
        try:
            with open(filename, "r") as f:
                workflow = f.read()
            return client.remove_comments(workflow)
        except ValueError:
            raise ValueError("File cannot be opened")

    @staticmethod
    def load(file):
        """
        Load an experiment from the JSON document

        Parameters
        ----------
        file: str
            The path/name of the file to be loaded

        Returns
        -------
        experiment: <class 'pyophidia.workflow.Experiment'>
            Returns the experiment object as it was loaded from the file

        Raises
        ------
        IOError
            Raises IOError if the file does not exist
        JSONDecodeError
            Raises JSONDecodeError if the file does not contain a valid JSON
            structure

        Example
        -------
        e1 = Experiment.load("json_file.json")
        """

        def check_experiment_name(data):
            if "name" not in data.keys():
                raise AttributeError("experiment doesn't have a key")

        def start_experiment(data):
            experiment = Experiment(name=data["name"])
            del data["name"]
            attrs = {k: data[k] for k in data if k != "name" and k != "tasks"}
            experiment.__dict__.update(attrs)
            for d in data["tasks"]:
                new_task = Task(
                    operator=d["operator"],
                    name=d["name"],
                    arguments={
                        a.split("=")[0]: a.split("=", 1)[1]
                        for a in d["arguments"]
                    },
                )
                new_task.__dict__.update(
                    {
                        k: d[k]
                        for k in d
                        if k != "name" and k != "operator" and k != "arguments"
                    }
                )
                experiment.addTask(new_task)
            return experiment

        json_string = __class__.json_open(file)
        try:
            data = json.loads(json_string)
        except json.decoder.JSONDecodeError:
            raise ValueError("File is not a valid JSON")
        check_experiment_name(data)
        experiment = start_experiment(data)
        return experiment

    @staticmethod
    def load_cwl(file, args=""):
        """
        Load an experiment from the CWL document

        Parameters
        ----------
        file: str
            The path/name of the file to be loaded

        Returns
        -------
        experiment: <class 'pyophidia.workflow.Experiment'>
            Returns the experiment object as it was loaded from the file

        Raises
        ------
        IOError
            Raises IOError if the file does not exist

        Example
        -------
        e1 = Experiment.load_cwl("cwl_file.cwl")
        """

        if not os.path.isfile(file):
            raise IOError("File does not exist")

        _dependency_check("cwltool")
        import cwltool
        import cwltool.factory

        cwl_args = {}
        param = None
        if args:
            for i in args.split():
                if param is None:
                    param = i
                else:
                    cwl_args[param[2:]] = int(i) if i.isdigit() else i
                    param = None

        fac = cwltool.factory.Factory()
        fac.runtime_context.rm_tmpdir = False
        cwl_tool = fac.make(file)
        result = cwl_tool(**cwl_args)

        json_request = result["outputexperiment"]["location"][7:]

        experiment = Experiment.load(json_request)

        try:
            os.remove(json_request)
        except OSError:
            print("JSON file cannot be removed")
        try:
            os.rmdir(json_request.rsplit("/", 1)[0])
        except OSError:
            print("Temporary folder cannot be removed")

        return experiment

    @staticmethod
    def __validate(json_string, *params):
        try:
            from client import Client
        except ImportError:
            from .client import Client
        client = Client(
            local_mode=True,
        )
        return client.wisvalid(json_string, *params)

    @staticmethod
    def validate(file, *params):
        """
        Check the workflow experiment definition validity

        Returns
        -------
        True in case of valid workflow, False otherwise

        Example
        -------
        Experiment.validate("json_file.json")
        """
        return __class__.__validate(__class__.json_open(file), *params)

    def isvalid(self, *params):
        """
        Check the workflow experiment definition validity

        Returns
        -------
        True in case of valid workflow, False otherwise

        Example
        -------
        e1 = Experiment(name="Experiment 1", author="sample author",
                       abstract="sample abstract")
        t1 = e1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                         dependencies={})
        e1.isvalid()
        """
        experiment_validity = self.__validate(self.workflow_to_json(), *params)
        return experiment_validity[0]

    def check(self, filename="", display=True, save=False, *params):
        """
        Check the experiment definition validity, display the graph of the
            experiment structure and store the graph a file

        Parameters
        ----------
        filename: str, optional
            The name of the file that will contain the diagram
        display: bool
            True for receiving the workflow status as an image or False to
            receive updates only in text
        save: bool
            True to save the image

        Returns
        -------
        None

        Example
        -------
        e1 = Experiment(name="Experiment 1", author="sample author",
                       abstract="sample abstract")
        t1 = e1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                         dependencies={})
        e1.check("myfile.dot")
        """

        if display is True:
            _dependency_check("graphviz")
            import graphviz

        def _trim_text(text):
            return text[:7] + "..." if len(text) > 10 else text

        def _find_subgraphs(tasks):
            list_of_operators = [t.operator for t in tasks]
            subgraphs_list = [
                {"start_index": start_index, "operator": "if"}
                for start_index in [
                    i for i, t in enumerate(list_of_operators) if t == "if"
                ]
            ]
            subgraphs_list += [
                {"start_index": start_index, "operator": "for"}
                for start_index in [
                    i for i, t in enumerate(list_of_operators) if t == "for"
                ]
            ]
            subgraphs_list = sorted(
                subgraphs_list, key=lambda i: i["start_index"]
            )
            closing_indexes = sorted(
                [
                    i
                    for i, t in enumerate(list_of_operators)
                    if t == "endfor" or t == "endif"
                ]
            )[::-1]
            for i in range(0, len(subgraphs_list)):
                subgraphs_list[i]["end_index"] = closing_indexes[i]

            cluster_counter = 0
            for subgraph in subgraphs_list:
                new_dot = graphviz.Digraph(
                    name="cluster_{0}".format(str(cluster_counter))
                )
                for i in range(
                    subgraph["start_index"], subgraph["end_index"] + 1
                ):
                    new_dot.attr("node")
                    new_dot.node(
                        tasks[i].name,
                        _trim_text(tasks[i].name)
                        + "\n"
                        + _trim_text(tasks[i].type)
                        + "\n"
                        + _trim_text(tasks[i].operator),
                    )
                subgraph["dot"] = new_dot
                cluster_counter += 1
            return subgraphs_list

        experiment_validity = self.isvalid(*params)
        self.__param_check(
            [
                {"name": "filename", "value": filename, "type": str},
                {"name": "display", "value": display, "type": bool},
                {"name": "save", "value": save, "type": bool},
            ]
        )
        if display is False:
            return experiment_validity

        diamond_commands = ["if", "endif", "else"]
        hexagonal_commands = ["for", "endfor"]
        dot = graphviz.Digraph(comment=self.name)
        for task in self.tasks:
            dot.attr(
                "node",
                shape="circle",
                width="1",
                penwidth="1",
                fontsize="10pt",
            )
            dot.attr("edge", penwidth="1")
            if task.operator in diamond_commands:
                dot.attr("node", shape="diamond")
            elif task.operator in hexagonal_commands:
                dot.attr("node", shape="hexagon")
            dot.node(
                task.name,
                _trim_text(task.name)
                + "\n"
                + _trim_text(task.type)
                + "\n"
                + _trim_text(task.operator),
            )
            dot.attr("edge", style="solid")
            for d in task.dependencies:
                if "argument" not in d.keys():
                    dot.attr("edge", style="dashed")
                dot.edge(d["task"], task.name)
        subgraphs = _find_subgraphs(self.tasks)
        if len(subgraphs) > 1:
            for i in range(0, len(subgraphs) - 1):
                subgraphs[i]["dot"].subgraph(subgraphs[i + 1]["dot"])
            dot.subgraph(subgraphs[0]["dot"])
        notebook_check = self._notebook_check()
        if notebook_check is True:
            # TODO change the image dimensions
            from IPython.display import display

            display(dot)
        if notebook_check is False or save is True:
            if not filename:
                filename = self.name
            dot.render(filename, view=True)
        return experiment_validity


class Workflow:
    """
    Submits, cancels and monitors a workflow experiment execution (a workflow)

    Construction::
    w1 = Workflow(experiment=e1)

    Parameters
    ----------
    experiment: int or <class 'pyophidia.workflow.Experiment'>
        Id of a running experiment or Experiment object

    Raises
    ------
    ValueError
        Raises ValueError if the provided parameter is not int or an Experiment
        object
    """

    client = None
    experiment_name = None
    runtime_task_graph = None

    def __init__(self, experiment):
        if isinstance(experiment, int):
            self.workflow_id = experiment
            self.experiment_object = None
        elif experiment.__class__.__name__ == "Experiment":
            self.experiment_object = experiment
            self.workflow_id = None
        else:
            raise ValueError("experiment argument must be int or experiment")

    def deinit(self):
        """
        Reverse the initialization of the object
        """
        for k in dict(self.__dict__):
            self.__delattr__(k)

    @classmethod
    def setclient(
        cls,
        client,
    ):
        """
        Instantiate the Client, common for all Workflow objects, for submitting
            requests


        Parameters
        ----------
        client: <class 'pyophidia.client.Client'>
            PyOhidia client object

        Returns
        -------
        None

        Raises
        ------
        Exception
            Raises an Exception in case of connection error

        Example
        -------
        Workflow.setclient(client)
        """

        cls.client = client

        if client is None or cls.client.last_return_value != 0:
            raise AttributeError("Connection to Ophidia server is not valid")
        else:
            cls.client.resume_session()

    def cancel(self):
        """
        Cancel the running workflow

        Returns
        -------
        None

        Example
        -------
        w1 = Workflow(name="Experiment 1", author="sample author",
                      abstract="sample abstract")
        t1 = w1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                        dependencies={})
        w1.submit()
        w1.cancel()
        """
        if Workflow.client is None or self.workflow_id is None:
            raise AttributeError(
                "Cancel requires workflow_id or Workflow.client is None"
            )
        self.client.submit(
            query="oph_cancel id={0};exec_mode=async;".format(self.workflow_id)
        )

    def submit(self, *args, checkpoint="all"):
        """
        Submit the experiment on the Ophidia Server

        Parameters
        ----------
        args: list
            list of arguments to be substituted in the workflow
        checkpoint: str, optional
            name of the checkpoint which the execution has to start from

        Raises
        ------
        AttributeError
            Raises AttributeError in case of connection error
            runtime

        Example
        -------
        w1.submit("test")
        """

        if Workflow.client is None:
            raise AttributeError("Workflow.client is None")
        exec_mode = self.experiment_object.exec_mode
        self.experiment_object.exec_mode = "async"
        self.experiment_object.output_format = "extended_compact"

        if checkpoint == "all":

            if self.workflow_id is not None:
                raise AttributeError(
                    "You can't submit a workflow that was already" "submitted"
                )
            dict_workflow = json.dumps(self.workflow_to_json())
            str_workflow = str(dict_workflow)
            self.client.wsubmit(str_workflow, *args)

        else:

            query = "oph_resume document_type=request;execute=yes;"
            query += "id=" + self.workflow_id + ";"
            query += "checkpoint=" + checkpoint + ";"
            self.client.submit(query)

        if self.client.last_jobid is None:
            raise AttributeError(
                "Something went wrong during the submission: "
                + str(self.client.last_error)
                if self.client.last_error is not None
                else ""
            )
        self.workflow_id = self.client.last_jobid.split("?")[1].split("#")[0]
        self.experiment_object.exec_mode = exec_mode
        return self.workflow_id

    def monitor(
        self,
        frequency=10,
        iterative=True,
        filename="",
        display=True,
        save=False,
    ):
        """
        Monitor the progress of the workflow execution

        Parameters
        ----------
        frequency: int
            The frequency in seconds to receive the updates
        iterative: bool
            True for receiving updates periodically, based on the frequency, or
            False to receive updates only once
        filename: str, optional
            The name of the file that will contain the diagram
        display: bool
            True for receiving the workflow status as an image or False to
            receive updates only in text
        save: bool
            True to save the image

        Returns
        -------
        workflow_status: <class 'str'>
            Returns the workflow status as a string

        Raises
        ------
        AttributeError
            Raises AttributeError when experiment is not valid

        Example
        -------
         e1 = Experiment(name="Experiment 1", author="sample author",
                        abstract="sample abstract")
         t1 = e1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                          dependencies={})
         w1 = Workflow(e1)
         w1.submit()
         w1.monitor(frequency=10, iterative=True, display=True)
        """

        if display is True:
            _dependency_check("graphviz")
            import graphviz

        def _trim_text(text):
            return text[:7] + "..." if len(text) > 10 else text

        def _find_matches(d, item):
            for k in d:
                if re.match(k, item):
                    return d[k]

        def _check_workflow_validity():
            self.__runtime_connect()
            workflow_validity = self.client.wisvalid(
                json.dumps(self.workflow_to_json())
            )
            if workflow_validity[0] is False:
                raise AttributeError(workflow_validity[1])

        def _find_subgraphs(tasks):
            list_of_operators = [t.operator for t in tasks]
            subgraphs_list = [
                {"start_index": start_index, "operator": "if"}
                for start_index in [
                    i for i, t in enumerate(list_of_operators) if t == "if"
                ]
            ]
            subgraphs_list += [
                {"start_index": start_index, "operator": "for"}
                for start_index in [
                    i for i, t in enumerate(list_of_operators) if t == "for"
                ]
            ]
            subgraphs_list = sorted(
                subgraphs_list, key=lambda i: i["start_index"]
            )
            closing_indexes = sorted(
                [
                    i
                    for i, t in enumerate(list_of_operators)
                    if re.match("(?i).*endfor", t)
                    or re.match("(?i).*endif", t)
                ]
            )[::-1]
            for i in range(0, len(subgraphs_list)):
                subgraphs_list[i]["end_index"] = closing_indexes[i]

            cluster_counter = 0
            for subgraph in subgraphs_list:
                new_dot = graphviz.Digraph(
                    name="cluster_{0}".format(str(cluster_counter))
                )
                for i in range(
                    subgraph["start_index"], subgraph["end_index"] + 1
                ):
                    new_dot.attr("node")
                    new_dot.node(
                        tasks[i].name,
                        _trim_text(tasks[i].name)
                        + "\n"
                        + _trim_text(tasks[i].type)
                        + "\n"
                        + _trim_text(tasks[i].operator),
                    )
                subgraph["dot"] = new_dot
                cluster_counter += 1
            return subgraphs_list

        def _check_workflow_status(json_response):
            for res in json_response["response"]:
                if res["objkey"] == "workflow_status":
                    return res["objcontent"][0]["message"]

        def _extract_info(json_response):
            task_dict = {}
            for res in json_response["response"]:
                print(res)
                if res["objkey"] == "workflow_list":
                    exec_keys = [
                        "TASK NAME",
                        "EXIT STATUS",
                        "INPUT",
                        "OUTPUT",
                        "BEGIN TIME",
                        "END TIME",
                    ]
                    index = []
                    if all(
                        idx in res["objcontent"][0]["rowkeys"]
                        for idx in exec_keys
                    ):
                        for k in exec_keys:
                            index.append(
                                int(res["objcontent"][0]["rowkeys"].index(k))
                            )
                        for task in res["objcontent"][0]["rowvalues"]:
                            task_dict[task[index[0]]] = dict(
                                (exec_keys[i], task[index[i]])
                                for i in range(1, len(exec_keys))
                            )
                        return task_dict
                    else:
                        return None

        def _match_shapes(operator, commands):
            for command in commands:
                if re.match("(?i).*" + command, operator):
                    return True
            return False

        def _sort_tasks(tasks):
            sorted_tasks = []
            for i in range(0, len(tasks)):
                if re.findall(r".*?(\([0-9].*\))", tasks[i].name):
                    clean_name = tasks[i].name.replace(
                        re.findall(r".*?(\([0-9].*\))", tasks[i].name)[0], ""
                    )
                    for task in tasks[i:]:
                        if (
                            clean_name in task.name
                            and task.name not in [t.name for t in sorted_tasks]
                            and re.findall(r".*?(\([0-9].*\))", task.name)
                        ):
                            sorted_tasks.append(task)
                else:
                    sorted_tasks.append(tasks[i])
            return sorted_tasks

        def _modify_task(json_response):
            new_tasks = []
            for res in json_response["response"]:
                if res["objkey"] == "resume":
                    task_name_index = res["objcontent"][0]["rowkeys"].index(
                        "COMMAND"
                    )
                    tasks = json.loads(
                        res["objcontent"][0]["rowvalues"][0][task_name_index]
                    )
            self.experiment_name = tasks["name"]
            for task in tasks["tasks"]:
                arguments = {}
                for j in task["arguments"]:
                    arguments[j.split("=")[0]] = j.split("=")[1]
                task_obj = Task(
                    name=task["name"],
                    operator=task["operator"],
                    type=task["type"],
                    arguments=arguments,
                )
                if "dependencies" in task.keys():
                    for dependency in task["dependencies"]:
                        task_obj.copyDependency(dependency)
                new_tasks.append(task_obj)
            return new_tasks

        def _add_runtimeinfo_task(status_response, tasks):
            task_dict = _extract_info(status_response)
            if task_dict is None:
                raise RuntimeError(
                    "Unable to extract information from JSON response"
                )
            task_list = []
            for task in tasks:
                if task.name in task_dict:
                    task.extra = task_dict[task.name]
                task_list.append(task.name)
            new_tasks = False
            for task in task_dict:
                if task not in task_list:
                    new_tasks = True
                    break
            return tasks, new_tasks

        def _draw(
            tasks,
            status_color_dictionary=None,
            filename="",
        ):
            diamond_commands = ["if", "endif", "else"]
            hexagonal_commands = ["for", "endfor"]
            dot = graphviz.Digraph(comment=self.experiment_name)
            for task in tasks:
                dot.attr(
                    "node",
                    shape="circle",
                    width="1",
                    penwidth="1",
                    style="",
                    fontsize="10pt",
                )
                if len(task.extra.keys()) == 0:
                    dot.attr("node", fillcolor="lightgrey", style="filled")
                if "EXIT STATUS" in task.extra and status_color_dictionary:
                    dot.attr(
                        "node",
                        fillcolor=_find_matches(
                            status_color_dictionary, task.extra["EXIT STATUS"]
                        ),
                        style="filled",
                    )
                dot.attr("edge", penwidth="1")
                if _match_shapes(task.operator, diamond_commands):
                    dot.attr("node", shape="diamond")
                elif _match_shapes(task.operator, hexagonal_commands):
                    dot.attr("node", shape="hexagon")
                dot.node(
                    task.name,
                    _trim_text(task.name)
                    + "\n"
                    + _trim_text(task.type)
                    + "\n"
                    + _trim_text(task.operator),
                )
                dot.attr("edge", style="solid")
                for d in task.dependencies:
                    if "argument" not in d.keys():
                        dot.attr("edge", style="dashed")
                    dot.edge(d["task"], task.name)
            subgraphs = _find_subgraphs(tasks)
            for i in range(0, len(subgraphs) - 1):
                subgraphs[i]["dot"].subgraph(subgraphs[i + 1]["dot"])
            if len(subgraphs) > 0:
                dot.subgraph(subgraphs[0]["dot"])
            notebook_check = self._notebook_check()
            if notebook_check is True:
                # TODO change the image dimensions
                from IPython.display import display, clear_output

                clear_output(wait=True)
                display(dot)
            if notebook_check is False or save is True:
                if not filename:
                    filename = self.experiment_name
                dot.render(filename, view=True)

        self.__param_check(
            params=[
                {"name": "frequency", "value": frequency, "type": int},
                {"name": "iterative", "value": iterative, "type": bool},
                {"name": "filename", "value": filename, "type": str},
                {"name": "display", "value": display, "type": bool},
                {"name": "save", "value": save, "type": bool},
            ]
        )
        status_color_dictionary = {
            "(?i).*RUNNING$": "orange",
            "(?i).*UNSELECTED": "grey",
            "(?i).*UNKNOWN": "lightgrey",
            "(?i).*PENDING": "pink",
            "(?i).*WAITING": "cyan",
            "(?i).*COMPLETED": "palegreen1",
            "(?i).*ERROR": "red",
            "(.*?)_ERROR": "red",
            "(?i).*ABORTED": "red",
            "(?i).*SKIPPED": "yellow",
        }
        if Workflow.client is None:
            raise AttributeError("Workflow.client is None")

        self.client.submit("oph_resume id={0};".format(self.workflow_id))
        status_response = json.loads(self.client.last_response)
        workflow_status = _check_workflow_status(status_response)

        self.client.submit(
            "oph_resume document_type=request;level=3;id={0};".format(
                self.workflow_id
            )
        )
        json_response = json.loads(self.client.last_response)
        tasks = _modify_task(json_response)
        self.runtime_task_graph = _sort_tasks(tasks)

        if iterative is True:
            while True:
                try:
                    self.runtime_task_graph, new_tasks = _add_runtimeinfo_task(
                        status_response, self.runtime_task_graph
                    )
                except Exception as e:
                    print(
                        _get_linenumber(), "Unable to build status graph:", e
                    )
                    print(workflow_status)

                if display is True:
                    _draw(
                        self.runtime_task_graph,
                        status_color_dictionary,
                        filename,
                    )
                else:
                    print(workflow_status)
                if not re.match("(?i).*RUNNING", workflow_status) and (
                    not re.match("(?i).*PENDING", workflow_status)
                ):
                    return workflow_status

                if new_tasks is True:
                    self.client.submit(
                        "oph_resume document_type=request;level=3;"
                        "id={0};".format(self.workflow_id)
                    )
                    json_response = json.loads(self.client.last_response)
                    tasks = _modify_task(json_response)
                    self.runtime_task_graph = _sort_tasks(tasks)

                time.sleep(frequency)

                self.client.submit(
                    "oph_resume id={0};".format(self.workflow_id)
                )
                status_response = json.loads(self.client.last_response)
                workflow_status = _check_workflow_status(status_response)
        else:
            try:
                self.runtime_task_graph, new_tasks = _add_runtimeinfo_task(
                    status_response, self.runtime_task_graph
                )
            except Exception as e:
                print(_get_linenumber(), "Unable to build status graph:", e)
                return workflow_status

            if display is True:
                _draw(self.runtime_task_graph, status_color_dictionary)
            else:
                print(workflow_status)
            return workflow_status

    def __param_check(self, params=[]):
        for param in params:
            if "NoneValue" in param.keys():
                if (
                    not isinstance(param["value"], param["type"])
                    and param["value"] is not None
                ):
                    raise AttributeError(
                        "{0} should be {1}".format(
                            param["name"], param["type"]
                        )
                    )
            else:
                if not isinstance(param["value"], param["type"]):
                    raise AttributeError(
                        "{0} should be {1}".format(
                            param["name"], param["type"]
                        )
                    )

    @staticmethod
    def _notebook_check():
        try:
            shell = get_ipython().__class__.__name__
            if shell == "ZMQInteractiveShell":
                return True
            elif shell == "TerminalInteractiveShell":
                return False
            else:
                return False
        except NameError:
            return False

    def workflow_to_json(self):
        if self.runtime_task_graph:
            new_workflow = {}
            new_workflow["tasks"] = [
                t.__dict__ for t in self.runtime_task_graph
            ]
            return new_workflow
        elif self.experiment_object.__class__.__name__ == "Experiment":
            non_workflow_fields = [
                "client",
                "task_name_counter",
                "workflow_id",
                "runtime_task_graph",
            ]

            new_workflow = {
                k: dict(self.experiment_object.__dict__)[k]
                for k in dict(self.experiment_object.__dict__).keys()
                if k not in non_workflow_fields
            }
            if "tasks" in new_workflow.keys():
                new_workflow["tasks"] = [
                    t.__dict__ for t in new_workflow["tasks"]
                ]
            return new_workflow

    def __repr__(self):
        return json.dumps(self.workflow_to_json())

    def build_provenance(
        self, output_file="", output_format="json", display=True
    ):
        """
        Build the provenance file associated with the workflow, provided that
            it has been completed

        Parameters
        ----------
        output_file: str, optional
            name (without any extension) of the file to be created
        output_format: str, optional
            format of the file to be created, extension to be append to the
            file name
        display: bool
            True for receiving the workflow status as an image or False to
            receive updates only in text

        Example
        -------
        w1.build_provenance("test")
        """

        _dependency_check("prov")
        from prov.model import ProvDocument
        from prov.dot import prov_to_dot

        def prov_doc_entity(doc, entity_id, attributes=None):
            if attributes and doc.get_record(entity_id):
                return doc.entity(entity_id)
            return doc.entity(entity_id, attributes)

        prov_doc = ProvDocument()
        prov_doc.add_namespace("ophidia", "http://ophidia.cmcc.it/")
        prov_doc.add_namespace("prov", "http://www.w3.org/ns/prov#")
        prov_doc.add_namespace(
            "nc", "https://www.unidata.ucar.edu/software/netcdf/"
        )

        # Global dictionaries of operator names
        multiInputsOperators = [
            "oph_mergecubes",
            "oph_mergecubes2",
            "oph_intercube",
            "oph_intercube2",
            "oph_importncs",
            "oph_concatnc",
            "oph_concatnc2",
            "oph_concatesdm",
            "oph_concatesdm2",
        ]  # N input, 1 output
        dataOperators = [
            "oph_aggregate",
            "oph_aggregate2",
            "oph_apply",
            "oph_drilldown",
            "oph_duplicate",
            "oph_merge",
            "oph_permute",
            "oph_reduce",
            "oph_reduce2",
            "oph_rollup",
            "oph_split",
            "oph_subset",
        ]
        fileOperators = [
            "oph_cdo",
            "oph_generic",
        ]
        specialOperators = [
            "oph_delete",
            "oph_explorecube",
            "oph_explorenc",
            "oph_metadata",
            "oph_script",
        ]
        importOperators = [
            "oph_importnc",
            "oph_importnc2",
            "oph_importfits",
            "oph_importesdm",
            "oph_importesdm2",
            "oph_randcube",
            "oph_randcube2",
        ]
        exportOperators = [
            "oph_exportnc",
            "oph_exportnc2",
            "oph_exportesdm",
            "oph_exportesdm2",
        ]
        skippedOperators = [
            "oph_containerschema",
            "oph_createcontainer",
            "oph_cubeelements",
            "oph_cubeio",
            "oph_cubeschema",
            "oph_cubesize",
            "oph_deletecontainer",
            "oph_else",
            "oph_elseif",
            "oph_endfor",
            "oph_endif",
            "oph_folder",
            "oph_for",
            "oph_fs",
            "oph_hierarchy",
            "oph_if",
            "oph_input",
            "oph_instances",
            "oph_list",
            "oph_movecontainer",
            "oph_set",
            "oph_showgrid",
            "oph_wait",
        ]

        data = self.runtime_task_graph

        for task in data:

            if not bool(task.extra):
                continue

            op_name = task.operator

            class_type = None
            if op_name in exportOperators:
                class_type = "export"
            elif op_name in dataOperators:
                class_type = "datacube"
            elif op_name in fileOperators:
                class_type = "file"
            elif op_name in importOperators:
                class_type = "import"
            elif op_name in specialOperators:
                class_type = "special"
            elif op_name in skippedOperators:
                class_type = "skip"
            elif op_name in multiInputsOperators:
                class_type = "multiInput"

            if class_type is not None and class_type != "skip":

                op_id = task.name.replace(" ", "_")
                op_input = task.extra["INPUT"]
                op_output = task.extra["OUTPUT"]
                op_status = task.extra["EXIT STATUS"]
                op_begin = task.extra["BEGIN TIME"]
                op_end = task.extra["END TIME"]
                op_args = task.arguments

                if op_input == op_output:
                    continue

                activity_extra = {
                    "prov:type": "ophidia:operator",
                    "ophidia:status": op_status,
                    "ophidia:arguments": ",".join(op_args),
                }

                if class_type == "multiInput":

                    inputs = op_input.split("|")
                    a = prov_doc.activity(
                        "ophidia:" + op_id, op_begin, op_end, activity_extra
                    )
                    eo = prov_doc_entity(
                        prov_doc,
                        "ophidia:" + op_output,
                        {"prov:type": "ophidia:datacube"},
                    )
                    prov_doc.wasGeneratedBy(eo, a)

                    if "oph_concat" in op_name:
                        if "oph_concatnc" in op_name:
                            ei1 = prov_doc_entity(
                                prov_doc,
                                "nc:" + inputs[0],
                                {"prov:type": "nc:file"},
                            )
                        else:
                            ei1 = prov_doc_entity(
                                prov_doc,
                                "esdm:" + inputs[0],
                                {"prov:type": "esdm:dataset"},
                            )
                        ei2 = prov_doc_entity(
                            prov_doc,
                            "ophidia:" + inputs[1],
                            {"prov:type": "ophidia:datacube"},
                        )
                        prov_doc.wasDerivedFrom(eo, ei1)
                        prov_doc.used(a, ei1)
                        prov_doc.wasDerivedFrom(eo, ei2)
                        prov_doc.used(a, ei2)
                    else:
                        for i in range(0, len(inputs)):
                            if "oph_importncs" in op_name:
                                ei = prov_doc_entity(
                                    prov_doc,
                                    "nc:" + inputs[i],
                                    {"prov:type": "nc:file"},
                                )
                            else:
                                ei = prov_doc_entity(
                                    prov_doc,
                                    "ophidia:" + inputs[i],
                                    {"prov:type": "ophidia:datacube"},
                                )

                            prov_doc.wasDerivedFrom(eo, ei)
                            prov_doc.used(a, ei)

                else:

                    inputs = op_input.split("|")
                    outputs = op_output.split("|")

                    a = prov_doc.activity(
                        "ophidia:" + op_id, op_begin, op_end, activity_extra
                    )

                    len_inputs = len(inputs)
                    len_outputs = len(outputs)
                    length = (
                        len_inputs if len_inputs > len_outputs else len_outputs
                    )

                    ki = -1
                    ko = -1

                    for k in range(length):

                        pki = ki
                        pko = ko

                        ki = k if k < len_inputs else 0
                        ko = k if k < len_outputs else 0

                        if class_type == "special":
                            continue

                        if class_type == "export":
                            if pki < ki:
                                ei = prov_doc_entity(
                                    prov_doc,
                                    "ophidia:" + inputs[ki],
                                    {"prov:type": "ophidia:datacube"},
                                )
                            if pko < ko:
                                if "oph_exportnc" in op_name:
                                    if "esdm://" in outputs[ko]:
                                        eo = prov_doc_entity(
                                            prov_doc,
                                            "esdm:" + outputs[ko],
                                            {"prov:type": "esdm:dataset"},
                                        )
                                    else:
                                        eo = prov_doc_entity(
                                            prov_doc,
                                            "nc:" + outputs[ko],
                                            {"prov:type": "nc:file"},
                                        )
                                else:
                                    eo = prov_doc_entity(
                                        prov_doc,
                                        "esdm:" + outputs[ko],
                                        {"prov:type": "esdm:dataset"},
                                    )

                        if class_type == "datacube":
                            if pki < ki:
                                ei = prov_doc_entity(
                                    prov_doc,
                                    "ophidia:" + inputs[ki],
                                    {"prov:type": "ophidia:datacube"},
                                )
                            if pko < ko:
                                eo = prov_doc_entity(
                                    prov_doc,
                                    "ophidia:" + outputs[ko],
                                    {"prov:type": "ophidia:datacube"},
                                )

                        if class_type == "file":
                            if pki < ki:
                                if "esdm://" in inputs[ki]:
                                    ei = prov_doc_entity(
                                        prov_doc,
                                        "esdm:" + inputs[ki],
                                        {"prov:type": "esdm:dataset"},
                                    )
                                else:
                                    ei = prov_doc_entity(
                                        prov_doc,
                                        "nc:" + inputs[ki],
                                        {"prov:type": "nc:file"},
                                    )
                            if pko < ko:
                                if "esdm://" in outputs[ko]:
                                    eo = prov_doc_entity(
                                        prov_doc,
                                        "esdm:" + outputs[ko],
                                        {"prov:type": "esdm:dataset"},
                                    )
                                else:
                                    eo = prov_doc_entity(
                                        prov_doc,
                                        "nc:" + outputs[ko],
                                        {"prov:type": "nc:file"},
                                    )

                        if class_type == "import":
                            if "randcube" in op_name:
                                ei = None
                                if pko < ko:
                                    eo = prov_doc_entity(
                                        prov_doc,
                                        "ophidia:" + outputs[ko],
                                        {"prov:type": "ophidia:datacube"},
                                    )
                            else:
                                if pki < ki:
                                    if "oph_importnc" in op_name:
                                        if "esdm://" in inputs[ki]:
                                            ei = prov_doc_entity(
                                                prov_doc,
                                                "esdm:" + inputs[ki],
                                                {"prov:type": "esdm:dataset"},
                                            )
                                        else:
                                            ei = prov_doc_entity(
                                                prov_doc,
                                                "nc:" + inputs[ki],
                                                {"prov:type": "nc:file"},
                                            )
                                    elif "oph_importesdm" in op_name:
                                        ei = prov_doc_entity(
                                            prov_doc,
                                            "esdm:" + inputs[ki],
                                            {"prov:type": "esdm:dataset"},
                                        )
                                    else:
                                        ei = prov_doc_entity(
                                            prov_doc,
                                            "fits:" + inputs[ki],
                                            {"prov:type": "fits:file"},
                                        )
                                if pko < ko:
                                    eo = prov_doc_entity(
                                        prov_doc,
                                        "ophidia:" + outputs[ko],
                                        {"prov:type": "ophidia:datacube"},
                                    )

                        if pko < ko:
                            prov_doc.wasGeneratedBy(eo, a)

                        if ei is not None:
                            if pki < ki:
                                prov_doc.used(a, ei)
                                prov_doc.wasDerivedFrom(eo, ei)
                            elif pko < ko:
                                prov_doc.wasDerivedFrom(eo, ei)

        if not output_file:
            output_file = self.experiment_name
        prov_doc.serialize(
            output_file + "." + output_format, format=output_format
        )

        if display:
            figure = prov_to_dot(prov_doc)
            figure.write_png(output_file + ".png")

        prov_doc_output = prov_doc.serialize(format=output_format)
        return prov_doc_output

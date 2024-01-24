class Experiment:
    """
    Creates or loads an ESDM-PAV experiment.

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
        PAV experiment name
    author: str, optional
        PAV experiment author
    abstract: str, optional
        PAV experiment description
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
                raise AttributeError("Unknown experiment argument: {0}".format(k))
            self.active_attributes.append(k)
        self.name = name
        self.author = author
        self.abstract = abstract
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
                if not isinstance(param["value"], param["type"]) and param["value"] is not None:
                    raise AttributeError("{0} should be {1}".format(param["name"], param["type"]))
            else:
                if not isinstance(param["value"], param["type"]):
                    raise AttributeError("{0} should be {1}".format(param["name"], param["type"]))

    def workflow_to_json(self):
        non_experiment_fields = ["task_name_counter"]
        new_experiment = {
            k: dict(self.__dict__)[k]
            for k in dict(self.__dict__).keys()
            if k not in non_experiment_fields
        }
        if "tasks" in new_experiment.keys():
            new_experiment["tasks"] = [t.__dict__ for t in new_experiment["tasks"]]
        return new_experiment

    def __repr__(self):
        import json;
        return json.dumps(self.workflow_to_json())

    def deinit(self):
        """
        Reverse the initialization of the object
        """
        for k in self.active_attributes:
            self.__delattr__(k)

    def addTask(self, task):
        """
        Adds a task to the ESDM-PAV experiment

        Parameters
        ----------
        task : <class 'esdm_pav_client.task.Task'>
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
                if dependency["task"] not in [task.__dict__["name"] for task in self.tasks]:
                    raise AttributeError("dependency not fulfilled")
        self.task_name_counter += 1
        self.tasks.append(task)

    def getTask(self, taskname):
        """
        Retrieve from the ESDM-PAV experiment the
        esdm_pav_client.task.Task object with the given task name

        Parameters
        ----------
        taskname : str
            The name of the task to be found in the experiment

        Returns
        -------
        task : <class 'esdm_pav_client.task.Task'>
            Returns the first task found
        None : Nonetype
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

    def save(self, experimentname):
        """
        Save the ESDM-PAV experiment as a JSON document

        Parameters
        ----------
        experimentname : str
            The path to the PAV document file where the experiment is being
            saved

        Example
        -------
        from esdm_pav_client import experiment
        e1 = experiment(name="sample name", author="sample author",
                        abstract="sample abstract")
        e1.save("sample_experiment")

        Raises
        ------
        AttributeError
            If worfklowname is not a string or it is empty
        """
        import json
        import os

        if not isinstance(experimentname, str):
            raise AttributeError("experimentname must be string")
        if len(experimentname) == 0:
            raise AttributeError("experimentname must contain more than 1 characters")
        data = self.workflow_to_json()
        if not experimentname.endswith(".json"):
            experimentname += ".json"
        with open(os.path.join(os.getcwd(), experimentname), "w") as fp:
            json.dump(data, fp, indent=4)

    def newTask(self, operator, arguments={}, dependencies={}, name=None, **kwargs):
        """
        Adds a new Task in the ESDM-PAV experiment without the need
        of creating a esdm_pav_client.task.Task object

        Attributes
        ----------
        operator : str
            operator name
        arguments : dict, optional
            dict of user-defined operator arguments as key=value pairs
        dependencies : dict, optional
            a dict of dependencies for the task
        name : str, optional
            the name of the task
        type : str, optional
            type of the task
        on_error : str, optional
            behaviour in case of error
        on_exit: str, optional
            behaviour in case of completion
        run : str, optional
            enable actual execution, yes or no

        Returns
        -------
        t : <class 'esdm_pav_client.task.Task'>
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
        try:
            from task import Task
        except ImportError:
            from .task import Task

        self.__param_check(
            [
                {"name": "operator", "value": operator, "type": str},
                {"name": "arguments", "value": arguments, "type": dict},
                {"name": "dependencies", "value": dependencies, "type": dict},
                {"name": "name", "value": name, "type": str, "NoneValue": True},
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
        Embeds an ESDM-PAV experiment into another experiment

        Parameters
        ----------
        experiment : <class 'esdm_pav_client.experiment.experiment'>
            The experiment that will be embeded into our main experiment
        params : dict of keywords
            a dict of keywords that will be used to replace placeholders in
            the tasks
        dependencies : dict, optional
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
        e1 = experiment(name="Experiment 1", author="sample author 1",
                        abstract="sample abstract 1")
        e2 = experiment(name="Experiment 2", author="sample author 2",
                        abstract="sample abstract 2")
        t1 = e2.newTask(operator='oph_reduce', arguments={'operation': 'avg'})
        task_array = e1.newSubexperiment(experiment=e2, params={},
                        dependency={})
        """
        import copy

        try:
            from task import Task
        except ImportError:
            from .task import Task

        def validate_experiment(e1, e2):
            if not isinstance(e2, Experiment) or e1.name == e2.name:
                raise AttributeError("Wrong experiment or same experiments")

        def rename_tasks(e2):
            def _get_flag_id():
                import re

                greatest_id = 1
                for task in self.tasks:
                    if "_{subexperiment_" in task.name:
                        task_id = int(re.findall("_\{subexperiment_(.*)\}", task.name)[0])
                        if task_id >= greatest_id:
                            greatest_id = task_id + 1
                return greatest_id

            flag_id = _get_flag_id()
            for task in e2.tasks:
                task.name = task.name + "_{subexperiment_ID}".replace("ID", str(flag_id))
            return e2

        def check_replace_args(params, task_arguments):
            import re

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
                    if re.findall(r"(\$.*)", task_arguments[k])[0] in params.keys():
                        new_task_arguments[k] = re.sub(
                            r"(\$.*)",
                            params[re.findall(r"(\$.*)", task_arguments[k])[0]],
                            task_arguments[k],
                        )
                    else:
                        new_task_arguments[k] = task_arguments[k]
                else:
                    new_task_arguments[k] = task_arguments[k]

            return new_task_arguments

        self.__param_check(
            [
                {"name": "experiment", "value": experiment, "type": Experiment},
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
            new_arguments = check_replace_args(params, task.reverted_arguments())
            task.arguments = new_arguments
            self.tasks.append(task)
        return copied_experiment.tasks[-1]

    @staticmethod
    def load(file):
        """
        Load a ESDM-PAV experiment from the JSON document

        Parameters
        ----------
        file : str
            The path/name of the PAV document file to be loaded

        Returns
        -------
        experiment : <class 'esdm_pav_client.experiment.Experiment'>
            Returns the experiment object as it was loaded from the file

        Raises
        ------
        IOError
            Raises IOError if the file does not exist
        JSONDecodeError
            Raises JSONDecodeError if the file does not containt a valid JSON
            structure

        Example
        -------
        e1 = Experiment.load("json_file.json")
        """

        def file_check(filename):
            import os
            import json

            if not os.path.isfile(filename):
                raise IOError("File does not exist")
            else:
                try:
                    with open(filename, "r") as f:
                        return json.loads(f.read())
                except json.decoder.JSONDecodeError:
                    raise ValueError("File is not a valid JSON")

        def check_experiment_name(data):
            if "name" not in data.keys():
                raise AttributeError("experiment doesn't have a key")

        def start_experiment(data):
            try:
                from task import Task
            except ImportError:
                from .task import Task

            experiment = Experiment(name=data["name"])
            del data["name"]
            attrs = {k: data[k] for k in data if k != "name" and k != "tasks"}
            experiment.__dict__.update(attrs)
            for d in data["tasks"]:
                new_task = Task(
                    operator=d["operator"],
                    name=d["name"],
                    arguments={a.split("=")[0]: a.split("=", 1)[1] for a in d["arguments"]},
                )
                new_task.__dict__.update(
                    {k: d[k] for k in d if k != "name" and k != "operator" and k != "arguments"}
                )
                experiment.addTask(new_task)
            return experiment

        data = file_check(file)
        check_experiment_name(data)
        experiment = start_experiment(data)
        return experiment

    def check(self, filename="sample.dot", visual=True):
        """
        Check the ESDM-PAV experiment definition validity and display the
        graph of the experiment structure

        Parameters
        ----------
        filename  : str, optional
            The name of the file that will contain the diagram

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
        import graphviz

        def _trim_text(text):
            return text[:7] + "..." if len(text) > 10 else text

        def _find_subgraphs(tasks):
            list_of_operators = [t.operator for t in tasks]
            subgraphs_list = [
                {"start_index": start_index, "operator": "if"}
                for start_index in [i for i, t in enumerate(list_of_operators) if t == "if"]
            ]
            subgraphs_list += [
                {"start_index": start_index, "operator": "for"}
                for start_index in [i for i, t in enumerate(list_of_operators) if t == "for"]
            ]
            subgraphs_list = sorted(subgraphs_list, key=lambda i: i["start_index"])
            closing_indexes = sorted(
                [i for i, t in enumerate(list_of_operators) if t == "endfor" or t == "endif"]
            )[::-1]
            for i in range(0, len(subgraphs_list)):
                subgraphs_list[i]["end_index"] = closing_indexes[i]

            cluster_counter = 0
            for subgraph in subgraphs_list:
                new_dot = graphviz.Digraph(name="cluster_{0}".format(str(cluster_counter)))
                for i in range(subgraph["start_index"], subgraph["end_index"] + 1):
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

        def _check_experiment_validity():
            import json
            
            import PyOphidia.client as _client
            pyophidia_client = _client.Client(
                local_mode=True,
            )

            experiment_validity = pyophidia_client.wisvalid(
                json.dumps(self.workflow_to_json())
            )
            if experiment_validity[1] == "experiment is valid":
                return True
            else:
                return False

        experiment_validity = _check_experiment_validity()
        self.__param_check(
            [
                {"name": "filename", "value": filename, "type": str},
                {"name": "visual", "value": visual, "type": bool},
            ]
        )
        if visual is False:
            return experiment_validity
        diamond_commands = ["if", "endif", "else"]
        hexagonal_commands = ["for", "endfor"]
        dot = graphviz.Digraph(comment=self.name)
        for task in self.tasks:
            dot.attr("node", shape="circle", width="1", penwidth="1", fontsize="10pt")
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
        else:
            dot.render(filename, view=True)

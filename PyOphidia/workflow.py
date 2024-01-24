class Workflow:
    """
    Submits, cancels and monitors a ESDM-PAV experiment execution (a workflow)

    Construction::
    w1 = Workflow(experiment=e1)

    Parameters
    ----------
    experiment: int or <class 'esdm_pav_client.experiment.Experiment'>
        Id of a running experiment or Experiment object

    Raises
    ------
    ValueError
        Raises ValueError if the provided parameter is not int or an Experiment
        object
    """

    pyophidia_client = None
    username = "oph-test"
    password = "abcd"
    server = "127.0.0.1"
    port = "11732"
    token = ''
    read_env = False
    project = None
    experiment_name = None

    def __init__(self, experiment):
        try:
            from experiment import Experiment
        except ImportError:
            from .experiment import Experiment
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
            print(k)
            self.__delattr__(k)

    def cancel(self):
        """
        Cancel the running PAV experiment

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
        if self.workflow_id is None:
            raise AttributeError("Cancel requires workflow_id")
        self.__runtime_connect()
        self.pyophidia_client.submit(
            query="oph_cancel id={0};exec_mode=async;".format(self.workflow_id)
        )

    def submit(self, *args, server="127.0.0.1", port="11732", checkpoint="all"):
        """
        Submit the PAV experiment on the ESDM-PAV runtime

        Parameters
        ----------
        server : str, optional
            ESDM-PAV runtime DNS/IP address
        port : str, optional
            ESDM-PAV runtime port
        args : list
            list of arguments to be substituted in the workflow
        checkpoint : str, optional
            name of the checkpoint which the execution has to start from


        Raises
        ------
        AttributeError
            Raises AttributeError in case of failure to connect to the PAV
            runtime


        Example
        -------
        w1.submit(server="127.0.0.1", port="11732", "test")
        """

        self.server = server
        self.port = port
        self.__runtime_connect()

        exec_mode = self.experiment_object.exec_mode
        self.experiment_object.exec_mode = "async"

        if checkpoint == "all":

            import json

            if self.workflow_id is not None:
                raise AttributeError("You can't submit a workflow that was already" "submitted")
            dict_workflow = json.dumps(self.workflow_to_json())
            str_workflow = str(dict_workflow)
            self.pyophidia_client.wsubmit(str_workflow, *args)

        else:

            query = "oph_resume document_type=request;execute=yes;"
            query += "id=" + self.workflow_id + ";"
            query += "checkpoint=" + checkpoint + ";"
            self.pyophidia_client.submit(query)

        if self.pyophidia_client.last_jobid is None:
            raise AttributeError("Something went wrong during the submission: " + str(self.pyophidia_client.last_error) if self.pyophidia_client.last_error is not None else "")
        self.workflow_id = self.pyophidia_client.last_jobid.split("?")[1].split("#")[0]
        self.experiment_object.exec_mode = exec_mode
        return self.workflow_id

    def monitor(self, frequency=10, iterative=True, visual_mode=True):
        """
        Monitors the progress of the PAV experiment execution

        Parameters
        ----------
        frequency : int
            The frequency in seconds to receive the updates
        iterative: bool
            True for receiving updates periodically, based on the frequency, or
            False to receive updates only once
        visual_mode: bool
            True for receiving the workflow status as an image or False to
            receive updates only in text

        Returns
        -------
        workflow_status : <class 'str'>
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
         w1.monitor(frequency=10, iterative=True, visual_mode=True)
        """
        import graphviz
        import json
        import time
        import re

        def _trim_text(text):
            return text[:7] + "..." if len(text) > 10 else text

        def _find_matches(d, item):
            for k in d:
                if re.match(k, item):
                    return d[k]

        def _check_workflow_validity():
            import json

            self.__runtime_connect()
            workflow_validity = self.pyophidia_client.wisvalid(json.dumps(self.workflow_to_json()))
            if not workflow_validity[1] == "Workflow is valid":
                raise AttributeError("Workflow is not valid")

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
                [
                    i
                    for i, t in enumerate(list_of_operators)
                    if re.match("(?i).*endfor", t) or re.match("(?i).*endif", t)
                ]
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

        def _check_workflow_status(json_response):
            for res in json_response["response"]:
                if res["objkey"] == "workflow_status":
                    return res["objcontent"][0]["message"]

        def _extract_info(json_response):
            task_dict = {}
            for res in json_response["response"]:
                if res["objkey"] == "workflow_list":
                    task_name_index = res["objcontent"][0]["rowkeys"].index("TASK NAME")
                    status_index = res["objcontent"][0]["rowkeys"].index("EXIT STATUS")
                    output_index = res["objcontent"][0]["rowkeys"].index("OUTPUT")
                    btime_index = res["objcontent"][0]["rowkeys"].index("BEGIN TIME")
                    etime_index = res["objcontent"][0]["rowkeys"].index("END TIME")
                    for task in res["objcontent"][0]["rowvalues"]:
                        task_dict[task[int(task_name_index)]] = (task[int(status_index)], task[int(output_index)], task[int(btime_index)], task[int(etime_index)])
            return task_dict

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
            try:
                from task import Task
            except ImportError:
                from .task import Task

            new_tasks = []
            for res in json_response["response"]:
                if res["objkey"] == "resume":
                    task_name_index = res["objcontent"][0]["rowkeys"].index("COMMAND")
                    tasks = json.loads(res["objcontent"][0]["rowvalues"][0][task_name_index])
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

        def _draw(
            tasks,
            json_response,
            status_color_dictionary=None,
        ):
            task_dict = _extract_info(json_response)
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
                if len(task_dict.keys()) == 0:
                    dot.attr("node", fillcolor="red", style="filled")
                if task.name in task_dict and status_color_dictionary:
                    dot.attr(
                        "node",
                        fillcolor=_find_matches(status_color_dictionary, task_dict[task.name][0]),
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
            else:
                dot.render("sample", view=True)

        self.__param_check(
            params=[
                {"name": "frequency", "value": frequency, "type": int},
                {"name": "iterative", "value": iterative, "type": bool},
                {"name": "visual_mode", "value": visual_mode, "type": bool},
            ]
        )
        status_color_dictionary = {
            "(?i).*RUNNING$": "orange",
            "(?i).*UNSELECTED": "grey",
            "(?i).*UNKNOWN": "grey",
            "(?i).*PENDING": "pink",
            "(?i).*WAITING": "cyan",
            "(?i).*COMPLETED": "palegreen1",
            "(?i).*ERROR": "red",
            "(.*?)_ERROR": "red",
            "(?i).*ABORTED": "red",
            "(?i).*SKIPPED": "yellow",
        }
        self.__runtime_connect()
        self.pyophidia_client.submit("oph_resume id={0};".format(self.workflow_id))
        status_response = json.loads(self.pyophidia_client.last_response)
        self.pyophidia_client.submit(
            "oph_resume document_type=request;level=3;id={0};".format(self.workflow_id)
        )
        json_response = json.loads(self.pyophidia_client.last_response)
        tasks = _modify_task(json_response)
        sorted_tasks = _sort_tasks(tasks)
        workflow_status = _check_workflow_status(status_response)
        if iterative is True:
            while True:
                if visual_mode is True:
                    _draw(sorted_tasks, status_response, status_color_dictionary)
                else:
                    print(workflow_status)
                if not re.match("(?i).*RUNNING", workflow_status) and (
                    not re.match("(?i).*PENDING", workflow_status)
                ):
                    return workflow_status
                time.sleep(frequency)
                self.pyophidia_client.submit("oph_resume id={0};".format(self.workflow_id))
                status_response = json.loads(self.pyophidia_client.last_response)
                workflow_status = _check_workflow_status(status_response)
        else:
            if visual_mode is True:
                _draw(sorted_tasks, status_response, status_color_dictionary)
                return workflow_status
            else:
                return workflow_status

    def __param_check(self, params=[]):
        for param in params:
            if "NoneValue" in param.keys():
                if not isinstance(param["value"], param["type"]) and param["value"] is not None:
                    raise AttributeError("{0} should be {1}".format(param["name"], param["type"]))
            else:
                if not isinstance(param["value"], param["type"]):
                    raise AttributeError("{0} should be {1}".format(param["name"], param["type"]))

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
        non_workflow_fields = [
            "pyophidia_client",
            "task_name_counter",
            "workflow_id",
        ]

        new_workflow = {
            k: dict(self.experiment_object.__dict__)[k]
            for k in dict(self.experiment_object.__dict__).keys()
            if k not in non_workflow_fields
        }
        if "tasks" in new_workflow.keys():
            new_workflow["tasks"] = [t.__dict__ for t in new_workflow["tasks"]]
        return new_workflow

    def __repr__(self):
        return self.workflow_to_json()

    def __runtime_connect(self):
        import PyOphidia.client as _client

        self.__param_check(
            [
                {"name": "username", "value": self.username, "type": str},
                {"name": "server", "value": self.server, "type": str},
                {"name": "port", "value": self.port, "type": str},
                {"name": "password", "value": self.password, "type": str},
            ]
        )
        if self.pyophidia_client is None:
            self.pyophidia_client = _client.Client(
                username=self.username,
                password=self.password,
                server=self.server,
                port=self.port,
                token=self.token,
                read_env=self.read_env,
                project=self.project,
                api_mode=False,
            )
            if self.pyophidia_client.last_return_value != 0:
                raise AttributeError("failed to connect to the runtime")
            else:
                self.pyophidia_client.resume_session()

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import os
import json
import time
import re
from inspect import currentframe
from prov.model import ProvDocument
from prov.dot import prov_to_dot

sys.path.append(os.path.dirname(__file__))

def _get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno

class Workflow:
    """
    Submits, cancels and monitors a workflow experiment execution (a workflow)

    Construction::
    w1 = Workflow(experiment=e1)

    Parameters
    ----------
    experiment: int or <class 'PyOphidia.experiment.Experiment'>
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
            self.__delattr__(k)
            
    @classmethod
    def setclient(
        cls,
        client,
    ):
        """
        Instantiate the Client, common for all Workflow objects, for submitting requests
        
        
        Parameters
        ----------
        client : <class 'PyOphidia.client.Client'>
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
            raise AttributeError("Cancel requires workflow_id or Workflow.client is None")
        self.client.submit(
            query="oph_cancel id={0};exec_mode=async;".format(self.workflow_id)
        )

    def submit(self, *args, checkpoint="all"):
        """
        Submit the experiment on the Ophidia Server

        Parameters
        ----------
        args : list
            list of arguments to be substituted in the workflow
        checkpoint : str, optional
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
                raise AttributeError("You can't submit a workflow that was already" "submitted")
            dict_workflow = json.dumps(self.workflow_to_json())
            str_workflow = str(dict_workflow)
            self.client.wsubmit(str_workflow, *args)

        else:

            query = "oph_resume document_type=request;execute=yes;"
            query += "id=" + self.workflow_id + ";"
            query += "checkpoint=" + checkpoint + ";"
            self.client.submit(query)

        if self.client.last_jobid is None:
            raise AttributeError("Something went wrong during the submission: " + str(self.client.last_error) if self.client.last_error is not None else "")
        self.workflow_id = self.client.last_jobid.split("?")[1].split("#")[0]
        self.experiment_object.exec_mode = exec_mode
        return self.workflow_id

    def monitor(self, frequency=10, iterative=True, display=True):
        """
        Monitor the progress of the workflow execution

        Parameters
        ----------
        frequency : int
            The frequency in seconds to receive the updates
        iterative: bool
            True for receiving updates periodically, based on the frequency, or
            False to receive updates only once
        display: bool
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
         w1.monitor(frequency=10, iterative=True, display=True)
        """

        import graphviz

        def _trim_text(text):
            return text[:7] + "..." if len(text) > 10 else text

        def _find_matches(d, item):
            for k in d:
                if re.match(k, item):
                    return d[k]

        def _check_workflow_validity():

            self.__runtime_connect()
            workflow_validity = self.client.wisvalid(json.dumps(self.workflow_to_json()))
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
                print(res)
                if res["objkey"] == "workflow_list":
                    exec_keys = ["TASK NAME", "EXIT STATUS", "INPUT", "OUTPUT", "BEGIN TIME", "END TIME"]
                    index = []
                    if all(idx in res["objcontent"][0]["rowkeys"] for idx in exec_keys):
                        for k in exec_keys:
                            index.append(int(res["objcontent"][0]["rowkeys"].index(k)))
                        for task in res["objcontent"][0]["rowvalues"]:
                            task_dict[task[index[0]]] = dict((exec_keys[i], task[index[i]]) for i in range(1,len(exec_keys)))
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

        def _add_runtimeinfo_task(status_response, tasks):
            try:
                from task import Task
            except ImportError:
                from .task import Task

            task_dict = _extract_info(status_response)
            if task_dict is None:
                raise RuntimeError("Unable to extract information from JSON response")
            for task in tasks:
                if task.name in task_dict:
                    task.extra = task_dict[task.name]
            return tasks

        def _draw(
            tasks,
            status_color_dictionary=None,
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
                if 'EXIT STATUS' in task.extra and status_color_dictionary:
                    dot.attr(
                        "node",
                        fillcolor=_find_matches(status_color_dictionary, task.extra['EXIT STATUS']),
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
                {"name": "display", "value": display, "type": bool},
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
        self.client.submit(
            "oph_resume document_type=request;level=3;id={0};".format(self.workflow_id)
        )
        json_response = json.loads(self.client.last_response)
        tasks = _modify_task(json_response)
        self.runtime_task_graph = _sort_tasks(tasks)
        workflow_status = _check_workflow_status(status_response)

        if iterative is True:
            while True:
                try:
                    self.runtime_task_graph = _add_runtimeinfo_task(status_response, self.runtime_task_graph)
                except Exception as e:
                    print(_get_linenumber(), "Unable to build status graph:", e)
                    print(workflow_status)

                if display is True:
                    _draw(self.runtime_task_graph, status_color_dictionary)
                else:
                    print(workflow_status)
                if not re.match("(?i).*RUNNING", workflow_status) and (
                    not re.match("(?i).*PENDING", workflow_status)
                ):
                    return workflow_status
                time.sleep(frequency)
                self.client.submit("oph_resume id={0};".format(self.workflow_id))
                status_response = json.loads(self.client.last_response)
                workflow_status = _check_workflow_status(status_response)
        else:
            try:
                self.runtime_task_graph = _add_runtimeinfo_task(status_response, self.runtime_task_graph)
            except Exception as e:
                print(_get_linenumber(), "Unable to build status graph:", e)
                return workflow_status

            if display is True:
                _draw(self.runtime_task_graph, status_color_dictionary)
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
        if self.runtime_task_graph:
            new_workflow = {}
            new_workflow["tasks"] = [t.__dict__ for t in self.runtime_task_graph]
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
                new_workflow["tasks"] = [t.__dict__ for t in new_workflow["tasks"]]
            return new_workflow

    def __repr__(self):
        return json.dumps(self.workflow_to_json())
    
    def build_provenance(self, output_file, output_format="json", display=True):
        """
        Build the provenance file associated with the workflow, provided that it has been completed

        Parameters
        ----------
        output_file : str
            name (without any extension) of the file to be created
        output_format : str, optional
            format of the file to be created, extension to be append to the name
        display: bool
            True for receiving the workflow status as an image or False to
            receive updates only in text

        Example
        -------
        w1.build_provenance("test")
        """

        prov_doc = ProvDocument()
        prov_doc.add_namespace('ophidia', 'http://ophidia.cmcc.it/')
        prov_doc.add_namespace('prov', 'http://www.w3.org/ns/prov#')
        prov_doc.add_namespace('nc', 'https://www.unidata.ucar.edu/software/netcdf/')
        
        # Global dictionaries of operator names
        multiInputsOperators = ["oph_mergecubes", "oph_mergecubes2","oph_intercube", "oph_intercube2", "oph_importncs","oph_concatnc","oph_concatnc2"] # N input, 1 output
        dataOperators = ["oph_aggregate", "oph_aggregate2", "oph_apply", "oph_drilldown", "oph_duplicate","oph_merge", "oph_permute", "oph_reduce", "oph_reduce2", "oph_rollup", "oph_subset"]
        specialOperators = ["oph_script", "oph_metadata", "oph_delete"]
        importOperators = ["oph_importnc", "oph_importnc2", "oph_importfits", "oph_randcube", "oph_randcube2"]
        exportOperators = ["oph_exportnc", "oph_exportnc2", "oph_explorecube"]
        skippedOperators = ["oph_createcontainer", "for", "endfor"]

        data = self.runtime_task_graph

        for task in data:

            op_name = task.operator

            class_type = None
            if op_name in exportOperators:
                class_type = "export"
            elif op_name in dataOperators:
                class_type = "datacube"
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

                activity_extra = {'prov:type': 'ophidia:operator','ophidia:status':op_status,'ophidia:arguments':','.join(op_args)}

                if class_type == "multiInput":
                    
                    inputs = op_input.split("|")
                    a = prov_doc.activity('ophidia:'+op_id, op_begin, op_end, activity_extra)
                    eo = prov_doc.entity('ophidia:'+op_output, {'prov:type': 'ophidia:datacube'})
                    prov_doc.wasGeneratedBy(eo, a)
                    
                    if "oph_concatnc" in op_name:
                        ei1 = prov_doc.entity('nc:'+inputs[0], {'prov:type': 'nc:file'})
                        ei2 = prov_doc.entity('ophidia:'+inputs[1], {'prov:type': 'ophidia:datacube'})
                        prov_doc.wasDerivedFrom(eo, ei1)
                        prov_doc.used(a,ei1)
                        prov_doc.wasDerivedFrom(eo, ei2)
                        prov_doc.used(a,ei2)
                    else:
                        for i in range(0,len(inputs)):
                            if "oph_importncs" in op_name:    
                                ei = prov_doc.entity('nc:'+inputs[i], {'prov:type': 'nc:file'})
                            else:
                                ei = prov_doc.entity('ophidia:'+inputs[i], {'prov:type': 'ophidia:datacube'})

                            prov_doc.wasDerivedFrom(eo, ei)
                            prov_doc.used(a,ei)
                        
                else:
                    
                    inputs = op_input.split("|")
                    outputs = op_output.split("|")
                    
                    a = prov_doc.activity('ophidia:'+op_id, op_begin, op_end, activity_extra)
                    
                    for k in range(len(inputs)):
                        
                        if class_type == "special":
                            continue
                        
                        if class_type == "export":
                            ei = prov_doc.entity('ophidia:'+inputs[k], {'prov:type': 'ophidia:datacube'})
                            eo = prov_doc.entity('nc:'+outputs[k], {'prov:type': 'nc:file'})
                        
                        if class_type == "datacube":
                            ei = prov_doc.entity('ophidia:'+inputs[k], {'prov:type': 'ophidia:datacube'})
                            eo = prov_doc.entity('ophidia:'+outputs[k], {'prov:type': 'ophidia:datacube'})
                        
                        if class_type == "import":
                            if "randcube" in op_name:
                                ei = None
                                eo = prov_doc.entity('ophidia:'+outputs[k], {'prov:type': 'ophidia:datacube'})
                            else:
                                ei = prov_doc.entity('nc:'+inputs[k], {'prov:type': 'nc:file'})
                                eo = prov_doc.entity('ophidia:'+outputs[k], {'prov:type': 'ophidia:datacube'})
                                
                        prov_doc.wasGeneratedBy(eo, a)
                        
                        if ei is not None:
                            prov_doc.wasDerivedFrom(eo, ei)
                            prov_doc.used(a,ei)
        
        prov_doc.serialize(output_file + "." + output_format, format = output_format)
        
        if display:
            figure = prov_to_dot(prov_doc)
            figure.write_png(output_file + '.png')

        prov_doc_output = prov_doc.serialize(format = output_format)
        return prov_doc_output

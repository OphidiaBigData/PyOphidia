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

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import sys
import os
from inspect import currentframe

sys.path.append(os.path.dirname(__file__))

def _get_linenumber():
    cf = currentframe()
    return __file__, cf.f_back.f_lineno

class Task:
    """
    Creates a Task object that can be embedded in a workflow experiment
    workflow

    Construction::
    t1 = Task(name="Sample task", operator="oph_reduce",
                arguments={'operation': 'avg'})

    Parameters
    ----------
    operator : str
        operator name
    arguments : dict, optional
        list of user-defined operator arguments as key=value pairs
    name : str, optional
        unique task name
    type : str, optional
        type of the task
    on_error : str, optional
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
        self.arguments = ["{0}={1}".format(k, arguments[k]) for k in arguments.keys()]
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
        task : <class 'PyOphidia.task.Task'>
            task the current one depends on
        argument : str, optional
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
        dependency : dict
            Copy a dependency to a task
        """
        self.dependencies.append(dependency)

    def reverted_arguments(self):
        """
        Changes the format of the arguments

        Returns
        -------
        arguments : dict
            returns the arguments with the newest format
        """
        arguments = {}
        for arg in self.arguments:
            arguments[arg.split("=")[0]] = arg.split("=")[1]
        return arguments

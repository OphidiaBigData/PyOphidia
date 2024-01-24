class Task:
    """
    Creates a Task object that can be embedded in a ESDM-PAV experiment
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
        task : <class 'esdm_pav_client.task.Task'>
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

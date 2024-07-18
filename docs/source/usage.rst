Usage
=====

Client
------

Attributes
^^^^^^^^^^

- *username*: Ophidia username
- *password*: Ophidia password
- *server*: Ophidia server address
- *port*: Ophidia server port (default is 11732)
- *session*: ID of the current session
- *base_src_path*: Server-side instance base source path
- *cwd*: Current Working Directory
- *cdd*: Current Data Directory
- *cube*: Last produced cube PID
- *host_partition*: Name of host partition being used
- *exec_mode*: Execution mode, 'sync' for synchronous mode (default), 'async' for asynchronous mode
- *ncores*: Number of cores for each operation (default is 1)
- *last_request*: Last submitted query
- *last_response*: Last response received from the server (JSON string)
- *last_jobid*: Job ID associated to the last request
- *last_return_value*: Last return value associated to response
- *last_error*: Last error value associated to response
- *last_exec_time*: Last execution time value associated to response
- *project*: Project to be used for the resource manager (if required)

Methods
^^^^^^^

- *submit(query, display) -> self*: Submit a query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;' to the Ophidia server according to all login parameters of the Client and its state.
- *get_progress(id) -> dict*: Get progress of a workflow, either by specifying the id or from the last submitted one.
- *deserialize_response() -> dict*: Return the last_response JSON string attribute as a Python dictionary.
- *get_base_path(display) -> self*: Get base path for data from the Ophidia server.
- *resume_session(display) -> self*: Resume the last session the user was connected to.
- *resume_cwd(display) -> self*: Resume the last cwd (current working directory) the user was located into.
- *resume_cdd(display) -> self*: Resume the last cdd (current working data directory) the user was located into.
- *resume_cube(display) -> self*: Resume the last cube produced by the user.
- *wsubmit(workflow, \*params) -> self*: Submit an entire workflow passing a JSON string or the path of a JSON file and an optional series of parameters that will replace $1, $2 etc. in the workflow. The workflow will be validated against the Ophidia Workflow JSON Schema.
- *wisvalid(workflow) -> bool*: Return True if the workflow (a JSON string or a Python dict) is valid against the Ophidia Workflow JSON Schema or False and the related validation/error message.
- *pretty_print(response, response_i) -> self*: Prints the last_response JSON string attribute as a formatted response.

*To display the command output set "display=True"* 

Import the class
^^^^^^^^^^^^^^^^

Import *client* module from *PyOphidia* package:

.. code-block:: python

   from PyOphidia import client

Instantiate a Client
^^^^^^^^^^^^^^^^^^^

Create a new *Client()* using the login parameters *username*, *password*, *host* and *port*.
It will also try to resume the last session the user was connected to, as well as the last working directory and the last produced cube.

.. code-block:: python

   ophclient = client.Client(username="oph-user",password="oph-passwd",server="127.0.0.1",port="11732")

In case of authentication token is used:

.. code-block:: python

   ophclient = client.Client(token="token",server="127.0.0.1",port="11732")

If *OPH_USER*, *OPH_PASSWD* (or *OPH_TOKEN*), *OPH_SERVER_HOST* and *OPH_SERVER_PORT* variables have been set in the environment (see the documentation_ for more details), a client can be also created reading directly the values from the environment without the need to specify any parameter. 

.. code-block:: python

   ophclient = client.Client(read_env=True)

Submit a request
^^^^^^^^^^^^^^^^

Execute the request *oph_list level=2*:

.. code-block:: python

   ophclient.submit("oph_list level=2", display=True)

Cube
----

Attributes
^^^^^^^^^^

Instance attributes:

- *pid*: Cube PID
- *creation_date*: Creation date of the cube
- *measure*: Name of the variable imported into the cube
- *measure_type*: Measure data type
- *level*: Number of operations between the original imported cube and the actual cube
- *nfragments*: Total number of fragments
- *source_file*: Parent of the actual cube
- *hostxcube*: Number of hosts on which the cube is stored
- *fragxdb*: Number of fragments for each database
- *rowsxfrag*: Number of rows for each fragment
- *elementsxrow*: Number of elements for each row
- *compressed*: If the cube is compressed or not
- *size*: Size of the cube
- *nelements*: Total number of elements
- *dim_info*: List of dict with information on each cube dimension

Class attributes:

- *client*: instance of class Client through which it is possible to submit all requests

Import the class
^^^^^^^^^^^^^^^^

Import *cube* module from *PyOphidia* package:

.. code-block:: python

   from PyOphidia import cube

Set a Client for the Cube
^^^^^^^^^^^^^^^^^^^^^^^^^

Instantiate a new Client common to all Cube instances:

.. code-block:: python

   cube.Cube.setclient(username="oph-user",password="oph-passwd",server="127.0.0.1",port="11732")

Create a new container
^^^^^^^^^^^^^^^^^^^^^^

Create a new container to contain our cubes called *test*, with 3 *double* dimensions (*lat*, *lon* and *time*):

.. code-block:: python

   cube.Cube.createcontainer(container='test',dim='lat|lon|time',dim_type='double|double|double',hierarchy='oph_base|oph_base|oph_time')

Import a new Cube
^^^^^^^^^^^^^^^^^

Import the variable *T2M* from the NetCDF file */path/to/file.nc* into a new cube inside the *test* container. Use *lat* and *lon* as explicit dimensions and *time* as implicit dimension expressed in days:

.. code-block:: python

   mycube = cube.Cube(container='test',exp_dim='lat|lon',imp_dim='time',measure='T2M',src_path='/path/to/file.nc',exp_concept_level='c|c',imp_concept_level='d')

Create a Cube object with an existing cube identifier
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Instantiate a new Cube using the PID of an existing cube:

.. code-block:: python

   mycube2 = cube.Cube(pid='http://127.0.0.1/1/2')

Show the structure and info of a Cube
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To shows metadata information about a data cube, its size and the dimensions related to it:

.. code-block:: python

   mycube2.info()

*For the operators such as "cubeschema", "cubesize", "cubeelements", "explore", "hierarchy", "info", "list", "loggingbk", "operators", "search", "showgrid", "man", "metadata", "primitives", "provenance", "search", "showgrid", "tasks" and other operators that provide verbose output, the display parameter by default is "True". For the rest of operators, to display the result, "dispay=True" should be set.*

Subset a Cube
^^^^^^^^^^^^^

To perform a subsetting operation along dimensions of a data cube (dimension values are used as input filters):

.. code-block:: python

   mycube3 = mycube2.subset(subset_dims='lat|lon',subset_filter='1:10|20:30',subset_type='coord')

Explore a Cube
^^^^^^^^^^^^^^

To explore a data cube filtering the data along its dimensions:

.. code-block:: python

   mycube2.explore(subset_dims='lat|lon',subset_filter='1:10|20:30',subset_type='coord')

Export a Cube to NetCDF file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To export data into a single NetCDF file:

.. code-block:: python

   mycube3.exportnc2(output_path='/home/user')

Export to a Python array
^^^^^^^^^^^^^^^^^^^^^^^^

To export data in a python-friendly format:

.. code-block:: python

   data = mycube3.export_array(show_time='yes')

Export a datacube to a Xarray dataset
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To export a datacube into a Xarray dataset (if supported):

.. code-block:: python

   data = mycube3.to_dataset()

Export a datacube to a Pandas dataframe
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To export a datacube into a Pandas dataframe (if supported):

.. code-block:: python

   data = mycube3.to_dataframe()

Run a Python script with Ophidia
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To run a Python script through Ophidia load or define the Python function in the script where PyOphidia is used (it works only with Python 3), e.g.:

.. code-block:: python

	def myScript(arg1):
		import subprocess
		return subprocess.call('ls -la ' + arg1, shell=True)

	cube.Cube.script(python_code=True,script=myScript,args="/home/ophidia",display=True)

Experiment
----------

Experiment attributes
^^^^^^^^^^^^^^^^^^^^^
- *exec_mode*: Execution mode, 'sync' for synchronous mode (default), 'async' for asynchronous mode
- *on_error*: Error mode, behavior in case of error
- *on_exit*: Exit mode, behaviour in case of completion
- *run*: Run mode, enable actual execution, 'yes' (default) or 'no'
- *nthreads*: Number of threads for data processing operation (default is 1)
- *ncores*: Number of cores for each operation (default is 1)
- *host_partition*: Name of host partition being used

Experiment methods
^^^^^^^^^^^^^^^^^^
Instance methods:

- *addTask(task)*: add a task to the workflow experiment.
- *getTask(taskname) -> Task*: retrieve the Task object from the workflow experiment with the given task name
- *save(experimentname)*: save the experiment as a JSON document
- *newTask(operator, arguments, dependencies, name, ...) -> Task*: add a new Task in the experiment without the need of creating a Task object
- *newSubexperiment(self, experiment, params, dependency) -> Task*: embed an experiment into another experiment
- *isvalid() -> bool*: check the workflow experiment definition validity
- *check(filename, display) -> bool*: check the experiment definition validity and display the graph of the experiment structure

Class methods:

- *load(file) -> Experiment*: load an experiment from the JSON document
- *validate(file) -> bool*: check the workflow experiment definition validity

Import Experiment
^^^^^^^^^^^^^^^^^
Import *Experiment* module from *PyOphidia* package:

.. code-block:: python

   from PyOphidia import Experiment

Create an experiment
^^^^^^^^^^^^^^^^^^^^
Create a simple experiment consisting of a single task (an Ophidia operator):

.. code-block:: python

	e1 = Experiment(name="Sample experiment", author="sample author",
		          abstract='Sample workflow')
	t1 = e1.newTask(name="Sample task", type="ophidia", operator="oph_list", 
		          on_error="skip", arguments={"level": "2"})

Task dependency management
^^^^^^^^^^^^^^^^^^^^^^^^^^
Dependency can be specified to enforce an order in the execution of the tasks. Starting from the previous example, a dependent task is added (e.g., an Ophidia operator):

.. code-block:: python

	t2 = e1.newTask(name="Sample task 2", type="ophidia", operator='oph_createcontainer', 
	                arguments={'container': "test", 'dim': 'lat|lon|time'},
	                dependencies={t1: None}) 

Dynamic replacement of argument values in tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Arguments value can be dynamically replaced in an experiment upon submission time. Considering the previous example, the container argument value can be made dynamic:

.. code-block:: python

	t2 = e1.newTask(name="Sample task 2", type="ophidia", operator='oph_createcontainer', 
	                arguments={'container': "$1", 'dim': 'lat|lon|time'},
	                dependencies={t1: None})

Implement a loop in the experiment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
A loop starts with the for operator and ends with endfor operator. The parallel argument allows the activation of the parallel execution mode. All the tasks with a dependency on the Start Loop task are performed within the loop:

.. code-block:: python

	t1 = e1.newTask(name="Start loop", type="control", operator="for", 
		          arguments={"key": "index", "values": "1|2", "parallel": "yes"})
	t2 = e1.newTask(name="Import", type="ophidia", operator="oph_importnc", 
		          arguments={"measure": "tasmax", "imp_dim": "time", "input": "tasmax_@{index}.nc"}, 
		          dependencies={"t1": ""})
	t3 = e1.newTask(name="End loop", type="control", operator="endfor", 
		          dependencies={"t2": "cube"})

Implement a selection block in the experiment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The flow control constructs ("if", "elseif", "else" and "endif") can be used to declare a selection statement:

.. code-block:: python

	t1 = e1.newTask(name="If block", type="control", operator='if', 
		          arguments={'condition': '$1'})
	t2 = e1.newTask(name="Import data", type="ophidia", operator='oph_importnc',
		          arguments={'measure': 'tasmax', 'imp_dim': 'time', 'input': 'tasmax.nc'},
		          dependencies={t1:''})
	t3 = e1.newTask(name="Endif block", type="control", operator='endif', arguments={},
		          dependencies={t2:''})

Error management of experiments 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Different behaviours can be specified for the experiment in case of an error during its execution via the 'on_error' argument. If set to "abort", an error in a task will cause the entire workflow to end; in case of "skip" only the failed task is skipped; with "continue" the failed task and all its dependencies are skipped; while with "repeat" the task execution will be repeated. 

.. code-block:: python

	e1 = Experiment(name="Sample experiment", author="sample author",
		          abstract='Sample workflow', on_error="abort")

Save an experiment
^^^^^^^^^^^^^^^^^^
Save the experiment as JSON document

.. code-block:: python

	e1.save("example.json")

Validate an experiment
^^^^^^^^^^^^^^^^^^^^^^
Validate the experiment document before the submission

.. code-block:: python

	e1.check()

Workflow
--------

Workflow attributes
^^^^^^^^^^^^^^^^^^^
- *client*: instance of class Client through which it is possible to submit all requests
- *experiment_name*: name of the experiment associated with the workflow
- *runtime_task_graph* : last response received from the server (JSON string)

Workflow methods
^^^^^^^^^^^^^^^^
Instance methods:

- *submit(args, checkpoint) -> int*: submit the workflow
- *cancel()*: cancel the running workflow
- *monitor(frequency, iterative, display):*: monitor the progress of the workflow execution
- *build_provenance(output_file, output_format, display) -> str*: build the provenance file associated with the workflow

Class methods:

- *setclient(cls, client)*: associate an instance of Client to any instance of Workflow

Import Workflow
^^^^^^^^^^^^^^^
Import *Workflow* module from *PyOphidia* package:

.. code-block:: python

   from PyOphidia import Workflow

Submit an experiment for execution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Submit the experiment created for execution to Ophidia Server

.. code-block:: python

	w1 = Workflow(Experiment.load("example.json"))
	w1.submit("2")

Monitor a running workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^
Monitor a workflow running on the Ophidia platform. The visual mode argument shows a graphical view of the experiment execution status

.. code-block:: python

	w1.monitor(visual_mode=True)

Cancel a workflow execution
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Cancel the executuon of a workflow.

.. code-block:: python

	w1.cancel()

Load an experiment
^^^^^^^^^^^^^^^^^^
Load an experiment from the JSON document

	e1 = Experiment.load("example.json")

Additional information on the methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Docstrings are available for the Workflow, Experiment and Task classes. To get additional information run:

.. code-block:: python

	from PyOphidia import Workflow, Experiment, Task
	help(Workflow)
	help(Experiment)
	help(Task)

Run an experiment with the CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To submit the execution of an experiment document to Ophidia Server:

.. code-block:: python

	$ wclient -w example.json 2

To submit an experiment and monitor its execution to Ophidia Server:

.. code-block:: python

	$ wclient -w example.json 2 -m

To cancel a running workflow:

.. code-block:: python

	$ wclient -c -i <workflow_id>

A full experiment example
^^^^^^^^^^^^^^^^^^^^^^^^^
The following code show a full experiment composed of CDO tasks, the commands to save the related JSON file and for its submission

.. code-block:: python

	from PyOphidia import Workflow, Experiment, Task
	 
	e1 = Experiment(name="CDO-based experiment example",
		      author="ESiWACE2",
		      abstract="Sample experiment with CDO")
	t1 = e1.newTask(name="Regrid",
		      type="cdo",
		      operator='-remapbil,r90x45',
		      arguments={'input': '/path/to/infile.nc', 
	                     'output': '/path/to/outfile.nc'})
	t2 = e1.newTask(name="Max",
		      type="cdo",
		      operator='-timmax',
		      arguments={'output': '/path/to/outfile_max.nc'},
		      dependencies={t1:'input'})
	t3 = e1.newTask(name="Min",
		      type="cdo",
		      operator='-timmin',
		      arguments={'output': '/path/to/outfile_min.nc'},
		      dependencies={t1:'input'})
	t4 = e1.newTask(name="Avg",
		      type="cdo",
		      operator='-timavg',
		      arguments={'output': '/path/to/outfile_avg.nc'},
		      dependencies={t1:'input'})

	e1.save("example.json")
	e1.check()

	w1 = Workflow(e1)
	w1.submit()

.. _documentation: https://ophidia.cmcc.it/documentation/users/terminal/term_advanced.html#oph-terminal-environment


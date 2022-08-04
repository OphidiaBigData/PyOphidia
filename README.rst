PyOphidia: Python bindings for Ophidia
======================================

*PyOphidia* is a GPLv3_-licensed Python package for interacting with the Ophidia_ framework.

It is an alternative to Oph_Term, the Ophidia no-GUI interpreter component, and a convenient way to submit SOAP HTTPS requests to an Ophidia server or to develop your own application using Python. 

It runs on Python 2.7, 3.7, 3.8, 3.9 and 3.10 it is pure-Python code and has some dependencies on Xarray, Pandas and Numpy. It requires a running Ophidia instance for client-server interactions. The latest PyOphidia version (v1.10) is compatible with Ophidia v1.7.

It provides 2 main modules:

- client.py: generic *low level* class to submit any type of requests (simple tasks and workflows), using SSL and SOAP with the client ophsubmit.py;
- cube.py: *high level* cube-oriented class to interact directly with cubes, with several methods wrapping the operators.

Installation
------------
To install *PyOphidia* package run the following command:

.. code-block:: bash 

   pip install pyophidia

Install with conda
------------------

To install *PyOphidia* with conda run the following command:

.. code-block:: bash 

   conda install -c conda-forge pyophidia 

Installation from sources
-------------------------
To install the latest developement version run the following commands:

.. code-block:: bash 

   git clone https://github.com/OphidiaBigData/PyOphidia
   cd PyOphidia
   python setup.py install

   
Examples
--------

Import PyOphidia
^^^^^^^^^^^^^^^^
Import *client* module from *PyOphidia* package:

.. code-block:: python

   from PyOphidia import client

Instantiate a client
^^^^^^^^^^^^^^^^^^^^
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


Client attributes
^^^^^^^^^^^^^^^^^
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
- *last_response_status*: Status of last response received from the server (string)
- *last_jobid*: Job ID associated to the last request
- *last_return_value*: Last return value associated to response
- *last_error*: Last error value associated to response
- *last_exec_time*: Last execution time value associated to response
- *project*: Project to be used for the resource manager (if required)

Client methods
^^^^^^^^^^^^^^
- *submit(query, display) -> self*: Submit a query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;' to the Ophidia server according to all login parameters of the Client and its state.
- *get_progress(id) -> dict* : Get progress of a workflow, either by specifying the id or from the last submitted one.
- *deserialize_response() -> dict*: Return the last_response JSON string attribute as a Python dictionary.
- *get_base_path(display) -> self* : Get base path for data from the Ophidia server.
- *resume_session(display) -> self*: Resume the last session the user was connected to.
- *resume_cwd(display) -> self*: Resume the last cwd (current working directory) the user was located into.
- *resume_cdd(display) -> self*: Resume the last cdd (current working data directory) the user was located into.
- *resume_cube(display) -> self*: Resume the last cube produced by the user.
- *wsubmit(workflow, \*params) -> self*: Submit an entire workflow passing a JSON string or the path of a JSON file and an optional series of parameters that will replace $1, $2 etc. in the workflow. The workflow will be validated against the Ophidia Workflow JSON Schema.
- *wisvalid(workflow) -> bool*: Return True if the workflow (a JSON string or a Python dict) is valid against the Ophidia Workflow JSON Schema or False and the related validation/error message.
- *pretty_print(response, response_i) -> self*: Prints the last_response JSON string attribute as a formatted response.

*To display the command output set "display=True"* 

Submit a request
^^^^^^^^^^^^^^^^
Execute the request *oph_list level=2*:

.. code-block:: python

   ophclient.submit("oph_list level=2", display=True)

Set a Client for the Cube class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Instantiate a new Client common to all Cube instances:

.. code-block:: python

   from PyOphidia import cube
   cube.Cube.setclient(username="oph-user",password="oph-passwd",server="127.0.0.1",port="11732")

Cube attributes
^^^^^^^^^^^^^^^
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
 
Create a new container
^^^^^^^^^^^^^^^^^^^^^^
Create a new container to contain our cubes called *test*, with 3 *double* dimensions (*lat*, *lon* and *time*):

.. code-block:: python

   cube.Cube.createcontainer(container='test',dim='lat|lon|time',dim_type='double|double|double',hierarchy='oph_base|oph_base|oph_time')

Import a new cube
^^^^^^^^^^^^^^^^^
Import the variable *T2M* from the NetCDF file */path/to/file.nc* into a new cube inside the *test* container. Use *lat* and *lon* as explicit dimensions and *time* as implicit dimension expressed in days:

.. code-block:: python

   mycube = cube.Cube(container='test',exp_dim='lat|lon',imp_dim='time',measure='T2M',src_path='/path/to/file.nc',exp_concept_level='c|c',imp_concept_level='d')

Create a Cube object from an existing cube identifier
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Instantiate a new Cube using the PID of an existing cube:

.. code-block:: python

   mycube2 = cube.Cube(pid='http://127.0.0.1/1/2')

Show a Cube structure and info
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To shows metadata information about a data cube, its size and the dimensions related to it:

.. code-block:: python

   mycube2.info()

*For the operators such as "cubeschema", "cubesize", "cubeelements", "explore", "hierarchy", "info", "list", "loggingbk", "operators", "search", "showgrid", "man", "metadata", "primitives", "provenance", "search", "showgrid", "tasks" and other operators that provide verbose output, the display parameter by default is "True". For the rest of operators, to display the result, "dispay=True" should be set.*

Subset a Cube
^^^^^^^^^^^^^
To perform a subsetting operation along dimensions of a data cube (dimension values are used as input filters):

.. code-block:: python

   mycube3 = mycube2.subset(subset_dims='lat|lon',subset_filter='1:10|20:30',subset_type='coord')

Explore Cube
^^^^^^^^^^^^
To explore a data cube filtering the data along its dimensions:

.. code-block:: python

   mycube2.explore(subset_dims='lat|lon',subset_filter='1:10|20:30',subset_type='coord')

Export to NetCDF file
^^^^^^^^^^^^^^^^^^^^^
To export data into a single NetCDF file:

.. code-block:: python

   mycube3.exportnc2(output_path='/home/user')

Export to Python array
^^^^^^^^^^^^^^^^^^^^^^
To exports data in a python-friendly format:

.. code-block:: python

   data = mycube3.export_array(show_time='yes')

Run a Python script with Ophidia
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To run a Python script through Ophidia load or define the Python function in the script where PyOphidia is used (works only with Python 3), e.g.:

.. code-block:: python

	def myScript(arg1):
		import subprocess
		return subprocess.call('ls -la ' + arg1, shell=True)

	cube.Cube.script(python_code=True,script=myScript,args="/home/ophidia",display=True)


.. _GPLv3: http://www.gnu.org/licenses/gpl-3.0.txt
.. _Ophidia: http://ophidia.cmcc.it
.. _documentation: http://ophidia.cmcc.it/documentation/users/terminal/term_advanced.html#oph-terminal-environment

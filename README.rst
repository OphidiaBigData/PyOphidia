PyOphidia: Python bindings for Ophidia
======================================

*PyOphidia* is a GPLv3_-licensed Python package for interacting to the Ophidia_ platform.

It is an alternative to Oph_Term, the no-GUI interpreter component bundled with Ophidia, and a convenient way to submit SOAP HTTPS requests to an Ophidia server or to develop your own client using Python.

It runs on Python 2.6, 2.7, and 3.4, has no dependencies and is pure-Python.

It provides 2 main modules:

- client.py: generic *low level* class to submit any type of requests (simple tasks and workflows), using SSL and SOAP with the client ophsubmit.py;
- cube.py: *high level* cube-oriented class to interact directly with cubes, with several methods wrapping some of the most useful operators.


Examples
--------

Import PyOphidia
^^^^^^^^^^^^^^^^
From the *PyOphidia* package import the *client* module:

.. code-block:: python

   from PyOphidia import client

Instantiate a client
^^^^^^^^^^^^^^^^^^^^
Create a new *Client()* using the login parameters *username*,*password*,*host* and *port*.
It will also try to resume the last session the user was connected to, as well as the last working directory and the last produced cube.

.. code-block:: python

   ophclient = client.Client("oph-user","oph-passwd","127.0.0.1","11732")

Client attributes
^^^^^^^^^^^^^^^^^
- *username*: Ophidia username
- *password*: Ophidia password
- *server*: Ophidia server address
- *port*: Ophidia server port (default is 11732)
- *session*: ID of the current session
- *cwd*: Current Working Directory
- *cube*: Last produced cube PID
- *exec_mode*: Execution mode, 'sync' for synchronous mode (default),'async' for asynchronous mode
- *ncores*: Number of cores for each operation (default is 1)
- *last_request*: Last submitted query
- *last_response*: Last response received from the server (JSON string)
- *last_jobid*: Job ID associated to the last request

Client methods
^^^^^^^^^^^^^^
- *submit(query) -> self*: Submit a query like 'operator=myoperator;param1=value1;' or 'myoperator param1=value1;' to the Ophidia server according to all login parameters of the Client and its state.
- *deserialize_response() -> dict*: Return the last_response JSON string attribute as a Python dictionary.
- *resume_session() -> self*: Resume the last session the user was connected to.
- *resume_cwd() -> self*: Resume the last cwd (current working directory) the user was located into.
- *resume_cube() -> self*: Resume the last cube produced by the user.
- *wsubmit(workflow,\*params) -> self*: Submit an entire workflow passing a JSON string or the path of a JSON file and an optional series of parameters that will replace $1, $2 etc. in the workflow. The workflow will be validated against the Ophidia Workflow JSON Schema.
- *wisvalid(workflow) -> bool*: Return True if the workflow (a JSON string or a Python dict) is valid against the Ophidia Workflow JSON Schema or False.

Submit a request
^^^^^^^^^^^^^^^^
Execute the request *oph_list level=2*:

.. code-block:: python

   ophclient.submit("oph_list level=2")

View the result
^^^^^^^^^^^^^^^
View the *JobID* of the last request and the returned JSON response:

.. code-block:: python

   print("Last JobID: " + ophclient.last_jobid)
   print("Last response: " + ophclient.last_response)

Set a Client for the Cube class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Instantiate a new Client common to all Cube instances:

.. code-block:: python

   from PyOphidia import cube
   cube.Cube.setclient('oph-user','oph-password','127.0.0.1','11732')

Create a new container
^^^^^^^^^^^^^^^^^^^^^^
Create a new container to contain our cubes called *test*, with 3 *double* dimensions (*lat*,*lon* and *time*):

.. code-block:: python

   cube.Cube.createcontainer(container='test',dim='lat|lon|time',dim_type='double|double|double',hierarchy='oph_base|oph_base|oph_time')

Import a new cube
^^^^^^^^^^^^^^^^^
Import the variable *T2M* from the NetCDF file */path/to/file.nc* into a new cube inside the *test* container. Use *lat* and *lon* as explicit dimensions and *time* as implicit dimension expressed in days. Use the host partition *testpartition* and distribute the cube across 1 host, 1 DBMS instance, 2 databases and 16 fragments (8 fragments per database):

.. code-block:: python

   mycube = cube.Cube(container='test',exp_dim='lat|lon',host_partition='testpartition',imp_dim='time',measure='T2M',src_path='/path/to/file.nc',exp_concept_level='c|c',imp_concept_level='d',ndb=2,ndbms=1,nfrag=8,nhost=1)

Create a Cube object with an existing cube
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Instantiate a new Cube using the PID of an existing cube:

.. code-block:: python

   mycube2 = cube.Cube(pid='http://127.0.0.1/1/2')

Pretty print information on a Cube
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Print in a structured way the main information regarding a Cube object:

.. code-block:: python

   print(mycube2)

.. _GPLv3: http://www.gnu.org/licenses/gpl-3.0.txt
.. _Ophidia: http://ophidia.cmcc.it

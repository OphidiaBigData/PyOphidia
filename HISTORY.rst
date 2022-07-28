
v1.10.0 - 2022-07-28
--------------------

Added:
~~~~~~

- to_dataset and to_dataframe methods `#40 <https://github.com/OphidiaBigData/PyOphidia/pull/40>`_
- Method for wait operator
- Methods for intercube2 operator `#39 <https://github.com/OphidiaBigData/PyOphidia/issues/39>`_
- Methods for importncs operator `#38 <https://github.com/OphidiaBigData/PyOphidia/issues/38>`_

Changed:
~~~~~~~~

- Update workflow supported keys
- Direct ouput option to no when running workflows instead of single tasks
- Cube methods interfaces to comply with Ophidia operators in v1.7.0
- client requests with new field 'command' 

Fixed:
~~~~~~

- Subset methods to not use 'time_filter' when 'subset_type' is index 
- Bug in server port parameter reading from the env 

Removed:
~~~~~~~~

- Automatic setting of host partition within the workflows submitted with wsubmit() 


v1.9.1 - 2021-08-03
-------------------

Added:
~~~~~~

- Argument 'save' for saving the JSON response in most cube methods

Changed:
~~~~~~~~

- Default values of 'disable' and 'enable' argument in 'service' method in cube class
- Default value of 'missingvalue' argument in cube class to '-'

Fixed:
~~~~~~

- Slow parsing of large XML response documents in ophsubmit.py `#34 <https://github.com/OphidiaBigData/PyOphidia/issues/34>`_


Removed:
~~~~~~~~

- Deprecated 'subset2' method from cube `#35 <https://github.com/OphidiaBigData/PyOphidia/pull/35>`_


v1.9.0 - 2021-07-21
-------------------

Added:
~~~~~~

- New attribute 'last_response_status' in client class
- Option to specify 'project' value for interacting with the job scheduler `#33 <https://github.com/OphidiaBigData/PyOphidia/pull/33>`_
- New methods 'last_workflowid' and 'last_markerid' in client class  `#26 <https://github.com/OphidiaBigData/PyOphidia/pull/26>`_
- New parameter 'cubes' to OPH_INTERCUBE

Changed:
~~~~~~~~

- Return value to JSON response for multiple metadata and info methods in cube class when display is set to False 
- Removed workflow json print from wisvalid method
- Cube methods interfaces to comply with Ophidia operators in v1.6.0
- Improve export_array method in cube class `#28 <https://github.com/OphidiaBigData/PyOphidia/pull/28>`_
- sectlient method in cube class with new argument 'api_mode' `#27 <https://github.com/OphidiaBigData/PyOphidia/pull/27>`_
- script method with new parameter 'space' `#25 <https://github.com/OphidiaBigData/PyOphidia/pull/25>`_
- b2drop method in cube class to support also 'get' action `#24 <https://github.com/OphidiaBigData/PyOphidia/pull/24>`_
- script method in cube class to support the execution of python code through the operator `#23 <https://github.com/OphidiaBigData/PyOphidia/pull/23>`_  

Fixed:
~~~~~~

- Input arguments usage in concatnc and concatnc2 methods in cube class 
- randcube2 method in cube class to use the proper operator


v1.8.1 - 2019-04-16
-------------------

Fixed:
~~~~~~

- Bug `#22 <https://github.com/OphidiaBigData/PyOphidia/issues/22>`_ related to authZ/authN token read from env.
- Bug `#21 <https://github.com/OphidiaBigData/PyOphidia/issues/21>`_ related to error detection in massive operators


v1.8.0 - 2019-01-24
-------------------

Added:
~~~~~~

- Methods for concatnc operators

Changed:
~~~~~~~~

- Cube methods interfaces to comply with Ophidia operators in v1.5.0
- ophsubmit main function to provide a more descriptive message in case of error in operator execution `#20 <https://github.com/OphidiaBigData/PyOphidia/pull/20>`_


Removed:
~~~~~~~~

- 'dbxdbms' and 'dbmsxhost' attributes from Cube module


v1.7.0 - 2018-07-27
-------------------

Added:
~~~~~~

- Features to retrive last CDD from server `#18 <https://github.com/OphidiaBigData/PyOphidia/pull/18>`_
- Interfaces of 2 new methods: b2drop (class method) and to_b2drop 
 
Changed:
~~~~~~~~

- 'info' method in Cube class to avoid calling cubeelements operator 
- Cube methods interfaces to comply with Ophidia operators in v1.4.0
- Interfaces of several operators to allow multi-thread execution `#19 <https://github.com/OphidiaBigData/PyOphidia/pull/19>`_

v1.6.0 - 2018-06-18
-------------------

Added:
~~~~~~

- New method for importnc2 in cube module `#15 <https://github.com/OphidiaBigData/PyOphidia/pull/15/>`_
- Support to manage reserved or user-defined host partition `#14 <https://github.com/OphidiaBigData/PyOphidia/pull/14>`_
- Support to parse comments and print validation errors in 'wsubmit' method `#13 <https://github.com/OphidiaBigData/PyOphidia/pull/13>`_
- New method for containerschema operator in cube module `#10 <https://github.com/OphidiaBigData/PyOphidia/pull/10>`_
 
Changed:
~~~~~~~~

- Connection functions to also get parameters from environment variables `#17 <https://github.com/OphidiaBigData/PyOphidia/pull/17>`_ 
- Reduce2 method in cube module for multiple threads `#15 <https://github.com/OphidiaBigData/PyOphidia/pull/15>`_
- Client module to read variables from extra fields in JSON response `#12 <https://github.com/OphidiaBigData/PyOphidia/pull/12>`_
- Metadata method in cube module to filter on variables `#11 <https://github.com/OphidiaBigData/PyOphidia/pull/11>`_
- Cubeschema method in cube module for dimension management `#9 <https://github.com/OphidiaBigData/PyOphidia/pull/9>`_

Fixed:
~~~~~~

- 'export_metadata' default value for export methods in cube module
- Export array function in cube module to work also with adimensional cubes `#16 <https://github.com/OphidiaBigData/PyOphidia/pull/16>`_

v1.5.0 - 2018-02-16
-------------------

Added:
~~~~~~

- Support for Authentication, Authorization and Accounting as a Service (token-based access) `#7 <https://github.com/OphidiaBigData/PyOphidia/pull/7>`_
- Method to monitor a workflow progress rate `#6 <https://github.com/OphidiaBigData/PyOphidia/pull/6>`_
- Support to retrieve base_src_path from Ophidia server
 
Changed:
~~~~~~~~

- Cube methods interfaces to comply with Ophidia operators in v1.2.0

Fixed:
~~~~~~

- Bugs related to non-ASCII and special (HTML) chars in json request submission


v1.4.0 - 2017-08-23
-------------------

Added:
~~~~~~

- Support for Current Data Directory in client
- Support for Ophidia file system operator
- last_error and last_return_value attributes in client

Changed:
~~~~~~~~

- Cube methods interfaces to comply with Ophidia operators in v1.1.0
- Client class to optionally catch framework-level errors
- Cube class constructor to allow instantiation of empty cube objects

Fixed:
~~~~~~

- Bug `#3 <https://github.com/OphidiaBigData/PyOphidia/issues/3>`_

v1.3.0 - 2017-05-08
-------------------

Added:
~~~~~~

- Method to export data as python arrays in cube module
- Pretty print support in most methods
- Pretty print function in client module
- Methods for all missing operators in cube module

Changed:
~~~~~~~~

- Code indentation style (PEP8)
- Improved inline documentation
- Disabled info method execution for each cube object instantiation

Fixed:
~~~~~~

- Import of local dependencies in cube and client modules
- Bug in cwd attribute resetting it only when session changes
- Bug in query parameter in apply method of cube module
- Submit function to correctly parse massive operations
- Bug `#1 <https://github.com/OphidiaBigData/PyOphidia/issues/1>`_

v1.2.1 - 2015-08-25
-------------------

- Bug fixing

v1.2.0 - 2015-08-12
-------------------

- Added Cube class

v1.1.0 - 2015-07-20
-------------------

- Bug fixing

v1.0.0 - 2015-06-05
-------------------

- Initial public release



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



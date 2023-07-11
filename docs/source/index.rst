Welcome to PyOphidia documentation!
===================================

.. figure:: https://ophidia.cmcc.it/wp-content/themes/ophidia/images/oph_logo_symbol_big.png
   :width: 30.0%
   :align: center
   :target: https://ophidia.cmcc.it

**PyOphidia** is a GPLv3_-licensed Python package for interacting with the Ophidia_ Framework.

It aims at providing a user-friendly and programmatic interface for large-scale data analytics and a convenient way to submit SOAP HTTPS requests to an Ophidia server or to develop your own application using Python. 

PyOphidia provides features for handling scientific data in the form of datacubes, managing workflow execution, enabling parallel processing on HPC/Cloud systems and supporting integration with well-known modules from the Python scientific ecosystem.

It runs on Python 2.7, 3.7, 3.8, 3.9 and 3.10 it is pure-Python code and has some dependencies on Xarray, Pandas and Numpy. It requires a running Ophidia instance for client-server interactions. The latest PyOphidia version (v1.10) is compatible with Ophidia v1.7.

It provides 2 main modules:

- client.py: generic *low level* class to submit any type of requests (simple tasks and workflows), using SSL and SOAP with the client ophsubmit.py
- cube.py: *high level* cube-oriented class to interact directly with cubes, with several methods wrapping the operators

In particular, they implement two main classes:

- Client class: it supports the submissions of Ophidia commands and workflows as well as the management of sessions from Python code
- Cube class: it builds on the client class and provides the datacube type abstraction and the methods to manipulate, process and get information on cubes objects

While the cube module provides a user-friendly interface, the client module allows a finer specification of the operators.

Check out the :doc:`usage` section for further information, including
how to :ref:`installation` the project.

.. note::

   This project is under active development.

Contents
--------

.. toctree::

   installation
   usage
   examples

.. _GPLv3: https://www.gnu.org/licenses/gpl-3.0.txt
.. _Ophidia: https://ophidia.cmcc.it


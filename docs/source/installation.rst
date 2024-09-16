Installation
============

.. _installation:

Dependencies
------------

Most of *PyOphidia* features are provided without installing any additional Python library, anyway the graphical support (e.g., associated with the class *Workflow*),the CLI, provenance and CWL supports need of additional libraries:

-   [graphviz](https://graphviz.readthedocs.io/en/stable/): an interface to facilitates the creation and rendering of graph descriptions in the DOT language of Graphviz
-   [click](https://click.palletsprojects.com): a package for creating beautiful command line interfaces in a composable way
-   [pydot](https://github.com/pydot/pydot): an interface for Graphviz's Dot
-   [prov](https://prov.readthedocs.io/en/latest/): a library for W3C Provenance Data Model supporting PROV-O (RDF), PROV-XML, PROV-JSON import/export
-   [xarray](https://docs.xarray.dev/en/stable/index.html): a library to handle multi-dimensional arrays in a simple and efficient way
-   [numpy](https://numpy.org/): a package for scientific computing
-   [pandas](https://pandas.pydata.org/): a data analysis and manipulation tool
-   [cwltool](https://cwltool.readthedocs.io/en/latest/): a tool to provide validation and execution of CWL files

Install with pip
----------------

To install *PyOphidia* package run the following command:

.. code-block:: console 

   $ pip install pyophidia

Install with conda
------------------

To install *PyOphidia* with conda run the following command:

.. code-block:: console 

   $ conda install -c conda-forge pyophidia 

Install from sources
--------------------

To install the latest developement version run the following commands:

.. code-block:: console 

   $ git clone https://github.com/OphidiaBigData/PyOphidia
   $ cd PyOphidia
   $ python setup.py install

CWL support
-----------
This tool translates a workflow description written using CWL specification_ into Ophidia workflow specification.
Before using the tool run the following commands:

.. code-block:: console

   $ pip install cwltool
   $ pip install cwlref-runner

To configure the tool, append the reference to folder PyOphidia/utils to PATH, by running the following commands from the main folder of PyOphidia:

.. code-block:: console

   $ cd PyOphidia/utils
   $ export PATH=$PATH:$PWD

.. _specification: http://www.commonwl.org/specification


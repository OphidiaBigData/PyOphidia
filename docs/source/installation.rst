Installation
============

.. _installation:

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


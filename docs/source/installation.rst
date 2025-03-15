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

Install from Pypi
-----------------

To install the base *PyOphidia* package run the following command:

.. code-block:: bash

   pip install pyophidia

The base installation includes most of the PyOphidia capabilities with a few exceptions. The following features can be optionally enabled and the related dependencies can be automatically installed:

- Conversion of the native Ophidia data structure into well-known structures (i.e., Pandas dataframe, Xarray dataset). *Numpy*, *Pandas* and *Xarray* libraries are installed

.. code-block:: bash

   pip install pyophidia[convert]

- Visual representation of Ophidia workflows defined in the *Experiment* module and executed with the *Workflow* one. *Graphviz* and *IPython* libraries are installed

.. code-block:: bash

   pip install pyophidia[display]

- Generation of provenance documents with the W3C PROV standard from *Workflow* module. *Prov* and *Pydot* libraries are installed

.. code-block:: bash

   pip install pyophidia[prov]

- Support for Ophidia workflows in CWL standard. *CWL* and *cwlref-runner* are installed

.. code-block:: bash

   pip install pyophidia[cwl]

Multiple features can be enabled together by specifying the different options, e.g., :code:`pip install pyophidia[convert,display,prov,cwl]`.

Install with conda
------------------

To install *PyOphidia* with conda run the following command:

.. code-block:: bash

   conda install -c conda-forge pyophidia

Install from sources
--------------------

To install the latest developement version run the following command:

.. code-block:: bash

   pip install git+https://github.com/OphidiaBigData/PyOphidia.git

Optional features can be enabled similarly to what explained above wit the following command:

.. code-block:: bash

   pip install git+https://github.com/OphidiaBigData/PyOphidia.git#egg=pyophidia[convert,display,prov,cwl]

.. _specification: http://www.commonwl.org/specification


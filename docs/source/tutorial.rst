Tutorial
========

This section provides a practical walkthrough using the PyOphidia Python module applied to some real climate data. It provides step-by-step instructions to execute the tutorial and the listing of the tutorial notebook. The full training course is available at this `page <https://oercommons.org/courseware/lesson/86887/overview>`_.

Setup the environment
---------------------

Requirements for the demo and hands-on parts: Docker, git command line and a web browser

To run this tutorial first retrieve the Ophidia training image from DockerHub:

.. code-block:: console

  docker pull ophidiabigdata/ophidia-training:latest

This image includes the full Ophidia software stack, the PyOphidia libary, a Jupyter Notebook server and a set of scientific Python modules. Find additional information on the image at `https://hub.docker.com/r/ophidiabigdata/ophidia-training <https://hub.docker.com/r/ophidiabigdata/ophidia-training>`_

Download the tutorial material:

.. code-block:: console

  git clone https://github.com/ESiWACE/hpda-vis-training.git

The tutorial requires some NetCDF files from the CMIP5 archive. In particular a couple of files from the ouput of the CESM model provided by CMCC will be downloaded with the following script from ESGF CMIP5 data nodes. CMIP5 data can be accessed from the ESGF Data Portal CMIP5 project page: `https://esgf-node.llnl.gov/search/cmip5/ <https://esgf-node.llnl.gov/search/cmip5/>`_. Please note that CMIP5 data come with the following Terms of Use: `https://pcmdi.llnl.gov/mips/cmip5/terms-of-use.html <https://pcmdi.llnl.gov/mips/cmip5/terms-of-use.html>`_


To download the data run:

.. code-block:: console

  cd hpda-vis-training/Training2022/Session1
  ./get_data.sh

You should now see two CMIP5 NetCDF files under the git repository folder.

Start the environment
---------------------

From the same folder start the container, binding the tutorial material repo path ($PWD):

.. code-block:: console

  sudo docker run --rm -it -v $PWD:/home/ophidia/notebooks ophidiabigdata/ophidia-training:latest

Now copy the URL showed in the log message (e.g., *http://172.17.0.2:8888/*) in your browser to open the Jupyter Notebook UI. Type ‘ophidia’ as password when prompted. In case the IP address in not reachable, try with *http://localhost:8888/*. The notebooks will be available under the *notebooks* folder in the Jupyter Notebook UI.

Run the tutorial notebooks
--------------------------

The tutorial is available on YouTube at this link: `http://www.youtube.com/watch?v=aPhwuBy1UxM <http://www.youtube.com/watch?v=aPhwuBy1UxM>`_

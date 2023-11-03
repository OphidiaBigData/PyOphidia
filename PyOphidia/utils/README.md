# CWL support

This tool translates a workflow description written using [CWL specification](https://www.commonwl.org/specification/) into Ophidia workflow specification.

Requirements
------------

Before using the tool run the following commands:

``` {.sourceCode .bash}
pip install git+https://github.com/OphidiaBigData/esdm-pav-client
pip install cwltool
pip install cwlref-runner
```

Install from source
-------------------

To configure the tool run the following commands from the main folder of PyOphidia:

``` {.sourceCode .bash}
cd PyOphidia/utils
export PATH=$PATH:$PWD/src
```

Usage
-----

The following example shows how a CWL-compliant workflow "oph_wf.cwl" can be submitted to Ophidia platform; the list "args" will se passed to CWT tool to set the workflow parameters. Internally, the workflow is translated into an Ophidia-compliant workflow.

``` {.sourceCode .bash}
cd examples
run.py oph_wf.cwl --args "--inputcontainer container"
```

The following example shows how the same CWL-compliant workflow can simply be translated into an Ophidia-compliant workflow, without submitting it. The output JSON file is saved into the folder "examples".

``` {.sourceCode .bash}
cd examples
./oph_wf.cwl --inputcontainer container
```


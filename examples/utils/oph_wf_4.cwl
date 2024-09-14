#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow
label: Example of workflow

inputs:
  inputexperiment:
    type: File?
  inputfile: string
  inputmeasure: string
  partition: string
  nthreads: int

outputs:
  outputexperiment:
    type: File
    outputSource: Delete/experiment

steps:
  Download:
    run: tasks/generic.cwl
    in:
      experiment: inputexperiment
      name:
        default: "Download file"
      command:
        default: "curl -k -s -o target_file.nc"
      args: inputfile
      description:
        default: "Download file"
    out: [experiment]
  Import:
    run: tasks/importnc2.cwl
    in:
      experiment: Download/experiment
      name:
        default: "Import cube"
      src_path:
        default: "target_file.nc"
      measure: inputmeasure
      imp_dim:
        default: "time"
      imp_concept_level:
        default: "d"
      hierarchy:
        default: "oph_base|oph_base|oph_time"
      nthreads: nthreads
      description:
        default: "Import cube"
      host_partition: partition
    out: [experiment]
  Apply:
    run: tasks/apply.cwl
    in:
      experiment: Import/experiment
      name:
        default: "Apply predicate"
      query:
        default: "oph_predicate2(measure,'x-293.15','>0','1','0')"
      measure_type:
        default: "auto"
      nthreads: nthreads
      description: 
        default: "Apply predicate"
    out: [experiment]
  Reduce:
    run: tasks/reduce2.cwl
    in:
      experiment: Apply/experiment
      name:
        default: "Reduce cube"
      operation:
        default: "sum"
      dim:
        default: "time"
      concept_level:
        default: "y"
      nthreads: nthreads
      description: 
        default: "Reduce cube"
    out: [experiment]
  Subset:
    run: tasks/subset.cwl
    in:
      experiment: Reduce/experiment
      name:
        default: "Subset cube"
      subset_dims:
        default: "time"
      subset_filter:
        default: "1"
      description: 
        default: "Subset cube"
    out: [experiment]
  Export:
    run: tasks/exportnc2.cwl
    in:
      experiment: Subset/experiment
      name:
        default: "Export cube"
      description: 
        default: "Export cube"
    out: [experiment]
  Delete:
    run: tasks/deletecontainer.cwl
    in:
      experiment: Export/experiment
      name:
        default: "Delete cubes"
      container:
        default: "target_file.nc"
      force:
        default: "yes"
      description: 
        default: "Delete cubes"
      on_error:
        default: "skip"
    out: [experiment]

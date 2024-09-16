#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow
label: Example of workflow

inputs:
  inputexperiment:
    type: File?
  inputfile: string
  inputmeasure: string

outputs:
  outputexperiment:
    type: File
    outputSource: Reduce/experiment

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
      description: 
        default: "Import cube"
    out: [experiment]
  Reduce:
    run: tasks/reduce.cwl
    in:
      experiment: Import/experiment
      name:
        default: "Reduce cube"
      operation:
        default: avg
      description: 
        default: "Reduce cube"
    out: [experiment]

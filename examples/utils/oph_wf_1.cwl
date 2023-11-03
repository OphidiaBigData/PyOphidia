#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: Workflow
label: Example of workflow

inputs:
  inputexperiment:
    type: File?
  inputcontainer: string

outputs:
  outputexperiment:
    type: File
    outputSource: Reduce/experiment

steps:
  CreateContainer:
    run: tasks/createcontainer.cwl
    in:
      experiment: inputexperiment
      name: 
        default: "Create container"
      container: inputcontainer
      dim:
        default: "time|lat|lon"
      dim_type:
        default: "double|double|double"
      description: 
        default: "Container creation"
    out: [experiment]
  RandCube:
    run: tasks/randcube.cwl
    in:
      experiment: CreateContainer/experiment
      container: inputcontainer
      name:
        default: "Random cube"
      measure:
        default: "prova"
      measure_type:
        default: "float"
      exp_ndim:
        default: 2
      dim:
        default: "time|lat|lon"
      dim_size:
        default: "10|10|10"
      nfrag:
        default: 10
      ntuple:
        default: 10
      description: 
        default: "Random cube"
    out: [experiment]
  Reduce:
    run: tasks/reduce.cwl
    in:
      experiment: RandCube/experiment
      name:
        default: "Reduce cube"
      operation:
        default: avg
    out: [experiment]

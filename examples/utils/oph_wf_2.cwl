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
    outputSource: Intercomparison/experiment

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
  RandCube1:
    run: tasks/randcube.cwl
    in:
      experiment: CreateContainer/experiment
      container: inputcontainer
      name:
        default: "First random cube"
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
        default: "First random cube"
    out: [experiment]
  RandCube2:
    run: tasks/randcube.cwl
    in:
      experiment: CreateContainer/experiment
      container: inputcontainer
      name:
        default: "Second random cube"
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
        default: "Second random cube"
    out: [experiment]
  Intercomparison:
    run: tasks/intercube.cwl
    in:
      experiment1: RandCube1/experiment
      experiment2: RandCube2/experiment
      name:
        default: "Intercomparison"
      description: 
        default: "Intercomparison"
    out: [experiment]

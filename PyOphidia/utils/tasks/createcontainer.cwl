#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_createcontainer
baseCommand: [cwl2oph.py, oph_createcontainer]
inputs:
  experiment:
    type: File?
    inputBinding:
      prefix: --experiment
      separate: true
  name:
    type: string
    inputBinding:
      prefix: --name
      separate: true
  container:
    type: string
    inputBinding:
      prefix: --container
      separate: true
  dim:
    type: string
    inputBinding:
      prefix: --dim
      separate: true
  dim_type:
    type: string?
    inputBinding:
      prefix: --dim_type
      separate: true
  hierarchy:
    type: string?
    inputBinding:
      prefix: --hierarchy
      separate: true
  description:
    type: string?
    inputBinding:
      prefix: --description
      separate: true
  on_error:
    type: string?
    inputBinding:
      prefix: --on_error
      separate: true
  dependencies:
    type: string?
    inputBinding:
      prefix: --dependencies
      separate: true
outputs:
  experiment: 
    type: stdout


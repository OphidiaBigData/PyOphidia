#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_set
baseCommand: [cwl2oph, oph_set]
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
  cube:
    type: string?
    inputBinding:
      prefix: --cube
      separate: true
  key:
    type: string
    inputBinding:
      prefix: --key
      separate: true
  keys:
    type: string?
    inputBinding:
      prefix: --keys
      separate: true
  value:
    type: string?
    inputBinding:
      prefix: --value
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


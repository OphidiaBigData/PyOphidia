#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_if
baseCommand: [cwl2oph, oph_if]
inputs:
  experiment:
    type: File
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
  condition:
    type: string?
    inputBinding:
      prefix: --condition
      separate: true
  forward:
    type: string?
    inputBinding:
      prefix: --forward
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


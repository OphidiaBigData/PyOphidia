#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_intercube
baseCommand: [cwl2oph.py, oph_intercube]
inputs:
  experiment1:
    type: File
    inputBinding:
      prefix: --experiment1
      separate: true
  experiment2:
    type: File
    inputBinding:
      prefix: --experiment2
      separate: true
  name:
    type: string
    inputBinding:
      prefix: --name
      separate: true
  operation:
    type: string?
    inputBinding:
      prefix: --operation
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


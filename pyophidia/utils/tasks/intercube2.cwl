#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_intercube2
baseCommand: [cwl2oph, oph_intercube2]
inputs:
  experiment:
    type: File[]
    inputBinding:
      prefix: --experiment1
      separate: true
  name:
    type: string
    inputBinding:
      prefix: --name
      separate: true
  cubes:
    type: string?
    inputBinding:
      prefix: --cubes
      separate: true
  operation:
    type: string?
    inputBinding:
      prefix: --operation
      separate: true
  measure:
    type: string?
    inputBinding:
      prefix: --measure
      separate: true
  ncores:
    type: int?
    inputBinding:
      prefix: --ncores
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


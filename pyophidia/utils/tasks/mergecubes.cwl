#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_mergecubes
baseCommand: [cwl2oph, oph_mergecubes]
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
  cubes:
    type: string?
    inputBinding:
      prefix: --cube
      separate: true
  container:
    type: string?
    inputBinding:
      prefix: --container
      separate: true
  mode:
    type: string?
    inputBinding:
      prefix: --mode
      separate: true
  hold_values:
    type: string?
    inputBinding:
      prefix: --hold_values
      separate: true
  number:
    type: int?
    inputBinding:
      prefix: --number
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


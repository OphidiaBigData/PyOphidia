#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_reduce
baseCommand: [cwl2oph.py, oph_reduce]
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
  operation:
    type: string
    inputBinding:
      prefix: --operation
      separate: true
  group_size:
    type: string?
    inputBinding:
      prefix: --group_size
      separate: true
  ncores:
    type: int?
    inputBinding:
      prefix: --ncores
      separate: true
  nthreads:
    type: int?
    inputBinding:
      prefix: --nthreads
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


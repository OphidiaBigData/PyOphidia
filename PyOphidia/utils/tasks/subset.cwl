#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_subset
baseCommand: [cwl2oph.py, oph_subset]
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
  subset_dims:
    type: string?
    inputBinding:
      prefix: --subset_dims
      separate: true
  subset_filter:
    type: string?
    inputBinding:
      prefix: --subset_filter
      separate: true
  subset_type:
    type: string?
    inputBinding:
      prefix: --subset_type
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


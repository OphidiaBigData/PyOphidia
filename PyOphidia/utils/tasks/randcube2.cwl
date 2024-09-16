#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_randcube
baseCommand: [cwl2oph.py, oph_randcube]
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
  measure:
    type: string
    inputBinding:
      prefix: --measure
      separate: true
  measure_type:
    type: string
    inputBinding:
      prefix: --measure_type
      separate: true
  exp_ndim:
    type: int
    inputBinding:
      prefix: --exp_ndim
      separate: true
  dim:
    type: string
    inputBinding:
      prefix: --dim
      separate: true
  concept_level:
    type: string?
    inputBinding:
      prefix: --concept_level
      separate: true
  dim_size:
    type: string
    inputBinding:
      prefix: --dim_size
      separate: true
  nfrag:
    type: int
    inputBinding:
      prefix: --nfrag
      separate: true
  ntuple:
    type: int
    inputBinding:
      prefix: --ntuple
      separate: true
  nhost:
    type: int?
    inputBinding:
      prefix: --nhost
      separate: true
  host_partition:
    type: string?
    inputBinding:
      prefix: --host_partition
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


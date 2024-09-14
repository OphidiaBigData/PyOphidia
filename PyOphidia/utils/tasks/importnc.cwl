#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_importnc
baseCommand: [cwl2oph.py, oph_importnc]
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
    type: string?
    inputBinding:
      prefix: --container
      separate: true
  src_path:
    type: string
    inputBinding:
      prefix: --src_path
      separate: true
  measure:
    type: string
    inputBinding:
      prefix: --measure
      separate: true
  import_metadata:
    type: string?
    inputBinding:
      prefix: --import_metadata
      separate: true
  exp_dim:
    type: string?
    inputBinding:
      prefix: --exp_dim
      separate: true
  exp_concept_level:
    type: string?
    inputBinding:
      prefix: --exp_concept_level
      separate: true
  imp_dim:
    type: string?
    inputBinding:
      prefix: --imp_dim
      separate: true
  imp_concept_level:
    type: string?
    inputBinding:
      prefix: --imp_concept_level
      separate: true
  hierarchy:
    type: string?
    inputBinding:
      prefix: --hierarchy
      separate: true
  nfrag:
    type: int?
    inputBinding:
      prefix: --nfrag
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
  ioserver:
    type: string?
    inputBinding:
      prefix: --ioserver
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


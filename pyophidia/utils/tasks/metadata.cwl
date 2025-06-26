#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_rollup
baseCommand: [cwl2oph, oph_rollup]
inputs:
  experiment:
    type: File[]
    inputBinding:
      prefix: --experiment
      separate: true
      itemSeparator: ","
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
  mode:
    type: string?
    inputBinding:
      prefix: --mode
      separate: true
  metadata_key:
    type: string?
    inputBinding:
      prefix: --metadata_key
      separate: true
  metadata_type:
    type: string?
    inputBinding:
      prefix: --metadata_type
      separate: true
  metadata_value:
    type: string?
    inputBinding:
      prefix: --metadata_value
      separate: true
  variable:
    type: string?
    inputBinding:
      prefix: --variable
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


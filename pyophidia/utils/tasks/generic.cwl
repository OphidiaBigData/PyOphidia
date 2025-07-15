#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_generic
baseCommand: [cwl2oph, oph_generic]
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
  command:
    type: string?
    inputBinding:
      prefix: --command
      separate: true
  args:
    type: string?
    inputBinding:
      prefix: --args
      separate: true
  space:
    type: string?
    inputBinding:
      prefix: --space
      separate: true
  input:
    type: string?
    inputBinding:
      prefix: --input
      separate: true
  output:
    type: string?
    inputBinding:
      prefix: --output
      separate: true
  output_path:
    type: string?
    inputBinding:
      prefix: --output_path
      separate: true
  output_name:
    type: string?
    inputBinding:
      prefix: --output_name
      separate: true
  force:
    type: string?
    inputBinding:
      prefix: --force
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


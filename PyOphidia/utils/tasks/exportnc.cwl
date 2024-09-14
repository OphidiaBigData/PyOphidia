#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_exportnc
baseCommand: [cwl2oph.py, oph_exportnc]
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
  export_metadata:
    type: string?
    inputBinding:
      prefix: --export_metadata
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


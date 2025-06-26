#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_for
baseCommand: [cwl2oph, oph_for]
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
  cube:
    type: string?
    inputBinding:
      prefix: --cube
      separate: true
  key:
    type: string
    inputBinding:
      prefix: --key
      separate: true
  values:
    type: string?
    inputBinding:
      prefix: --values
      separate: true
  counter:
    type: string?
    inputBinding:
      prefix: --counter
      separate: true
  input:
    type: string?
    inputBinding:
      prefix: --input
      separate: true
  parallel:
    type: string?
    inputBinding:
      prefix: --force
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


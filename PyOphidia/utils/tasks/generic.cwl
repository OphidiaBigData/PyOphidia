#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
label: oph_generic
baseCommand: [cwl2oph.py, oph_generic]
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
  command:
    type: string?
    inputBinding:
      prefix: --command
      separate: true
  script:
    type: string?
    inputBinding:
      prefix: --script
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


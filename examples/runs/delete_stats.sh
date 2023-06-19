#!/bin/bash

# Specify the path to the 'graphs' directory
graphs_dir="graphs"

# Find and delete all 'stats.pickle' files in subdirectories of 'graphs'
find "$graphs_dir" -type f -name 'stats.pickle' -delete
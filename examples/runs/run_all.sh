#!/bin/bash

# Specify the path to the 'graphs' directory
directory="graphs"

# Loop over each directory in 'graphs'
for dir in "graphs"/*; do
    if [ -d "$dir" ]; then  # Check if it's a directory
        dir_name=$(basename "$dir")
        echo "Processing directory: $dir_name"
        mpirun -n 1 python run_graph.py "$dir_name"
        mpirun -n 2 python run_graph.py "$dir_name"
        mpirun -n 3 python run_graph.py "$dir_name"
        mpirun -n 5 python run_graph.py "$dir_name"
        mpirun -n 7 python run_graph.py "$dir_name"
        mpirun -n 9 python run_graph.py "$dir_name"
    fi
done
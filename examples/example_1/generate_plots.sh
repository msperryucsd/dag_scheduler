#!/bin/bash
for f in $(find . -maxdepth 1 -type f); do
    if [[ "$f" == *"output"* ]];then
        if [[ "$f" != *"png"* ]];then
            echo "generating $f"
            gprof2dot -f pstats $f | dot -Tpng -o $f.png
        fi
    fi
    # echo "generating $f"
done
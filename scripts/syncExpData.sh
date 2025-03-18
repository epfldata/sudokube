#!/bin/bash

dest="${1:-kube1}"
echo "Downloading experiment data from $dest"


rsync -avz --exclude-from='.gitignore' --progress $dest:/var/data/sudokube/sudokube/expdata/ expdata
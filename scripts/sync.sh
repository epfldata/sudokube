#!/bin/bash

dest="${1:-kube}"
echo $dest


rsync -avz --exclude-from='.gitignore' --progress --delete src/ $dest:/var/data/sudokube/sudokube/src
rsync -avz --exclude-from='.gitignore' --progress --delete example-data/ $dest:/var/data/sudokube/sudokube/example-data
rsync -avz --exclude-from='.gitignore' --progress build.sbt $dest:/var/data/sudokube/sudokube/build.sbt
rsync -avz --exclude-from='.gitignore' --progress project/ $dest:/var/data/sudokube/sudokube/project/
rsync -avz --exclude-from='.gitignore' --progress backend_CBackend.h $dest:/var/data/sudokube/sudokube/backend_CBackend.h
rsync -avz --exclude-from='.gitignore' --progress Makefile $dest:/var/data/sudokube/sudokube/Makefile

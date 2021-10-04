rsync -avz --exclude-from='.gitignore' --progress src/ kube:/var/data/sudokube/sudokube/src/
rsync -avz --exclude-from='.gitignore' --progress build.sbt kube:/var/data/sudokube/sudokube/build.sbt
rsync -avz --exclude-from='.gitignore' --progress project/ kube:/var/data/sudokube/sudokube/project/
rsync -avz --exclude-from='.gitignore' --progress backend_CBackend.h kube:/var/data/sudokube/sudokube/backend_CBackend.h
#rsync -avz --exclude-from='.gitignore' --progress makefile kube:/var/data/sudokube/sudokube/makefile
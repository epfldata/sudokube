echo "Splitting NYC data into 1000 files for parallel processing"
split -a 3 -n l/1000 all all.part -d --additional-suffix=.tsv


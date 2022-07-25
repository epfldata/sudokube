echo "Unique values for column $2 of NYC table"
cut -f$2 ../$1 | sort | uniq > $1.$2.uniq

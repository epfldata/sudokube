echo "Unique values for column $2 for $1 table"
cut -d\| -f$2 ../$1.tbl | sort | uniq > $1.$2.uniq

sf=$1
cur=`pwd`
echo "Splitting Lineorder into several files for parallel processing at $cur"
split -a 3 -n l/$((sf*10)) lineorder.tbl lineorder. -d --additional-suffix=.tbl


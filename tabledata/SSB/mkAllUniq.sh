cur=`pwd`
echo "Getting distict values of columns at $cur"
seq 1 8 | xargs -n1 -P8 ../../cutuniq.sh customer
seq 1 17 | xargs -n1 -P17 ../../cutuniq.sh lineorder
seq 1 9 | xargs -n1 -P9 ../../cutuniq.sh part
seq 1 7 | xargs -n1  -P7 ../../cutuniq.sh supplier

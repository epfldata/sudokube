host=`kubectl describe pod sudokube-0-0 | grep "^Node:" |  grep -o "icc.*.ch"`
echo "Host = $host"
sed -i.bu "8s|icc.*ch|$host|" ~/.ssh/config

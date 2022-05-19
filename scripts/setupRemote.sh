#!/usr/bin/env bash

nickname=$1
hostname="iccluster$2"

case "$nickname" in
"kube1")
  line=8
  ;;
"kube2")
  line=15
  ;;
*)
  echo "First argument must be kube1 or kube2"
  exit
esac

sed -i.bu "${line}s/iccluster[0-9]*/$hostname/" ~/.ssh/config
sedstr="sed -i.bu  '${line}s/iccluster[0-9]*/${hostname}/' ~/.ssh/config"
echo $sedstr
ssh-copy-id $nickname
ssh-copy-id -i ~/.ssh/server_rsa.pub $nickname
scp ./Docker/init.sh $nickname:.

ssh $nickname "mkdir -p  /root/.ssh /var/data/sudokube/sudokube"

ssh datadell "$sedstr"
ssh datadell "scp ~/.rclone.conf $nickname:."
ssh datadell "scp ~/sudokube/sudokube/.jvmopts $nickname:/var/data/sudokube/sudokube/"
ssh datadell "scp ~/.ssh/id* $nickname:/root/.ssh"

ssh $nickname 'chmod +x ./init.sh && ./init.sh'
scripts/sync.sh $nickname
ssh $nickname 'cd /var/data/sudokube/sudokube && make'

ssh datadell "~/sudokube/sudokube/scripts/uniqsyncto $nickname SSB/sf100"
ssh datadell "~/sudokube/sudokube/scripts/uniqsyncto $nickname nyc"

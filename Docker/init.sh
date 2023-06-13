apt update
apt install -y vim \
htop \
bash \
tar \
wget \
byobu \
ca-certificates \
curl \
rsync \
make \
cmake  \
build-essential \
openjdk-8-jdk \
unzip \
rclone \
parallel \
git


wget https://github.com/sbt/sbt/releases/download/v1.2.7/sbt-1.2.7.zip
unzip sbt-1.2.7.zip && mv sbt /opt/sbt && rm sbt-1.2.7.zip
echo "export PATH=/var/data/sudokube/sudokube/scripts:/opt/sbt/bin:$PATH" >> /root/.profile
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /root/.profile

echo "Host *.iccluster.epfl.ch
      	User root
      	LogLevel ERROR
      	StrictHostKeyChecking no
      	UserKnownHostsFile=/dev/null
" >> /root/.ssh/config

#copy rclone conf
mkdir -p /var/data/sudokube/sudokube
cd /var/data/sudokube/sudokube
mkdir tabledata expdata cubedata
mount /dev/sdb tabledata
mount /dev/sdc cubedata
rclone copy  --include "**/*.uniq"  skRW:12327-755e92f906ff1f273f45a3a76785476f/tabledata tabledata   -P --transfers=16 --multi-thread-streams=16
rclone copy    skRW:12327-755e92f906ff1f273f45a3a76785476f/colstoreMay23 cubedata -P --transfers=16 --multi-thread-streams=16


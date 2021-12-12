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
unzip 


wget https://github.com/sbt/sbt/releases/download/v1.2.7/sbt-1.2.7.zip
unzip sbt-1.2.7.zip && mv sbt /opt/sbt && rm sbt-1.2.7.zip
echo "export PATH=/opt/sbt/bin:$PATH" >> /root/.profile

mkdir -p /var/data/sudokube/sudokube
cd /var/data/sudokube/sudokube
mkdir tabledata expdata cubedata

#sync
#make shared lib !



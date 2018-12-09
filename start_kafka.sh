#! /bin/bash
# This is the script to help you deploy kafka+samza cluster on EMR
home="/home/hadoop"
join_arr() {
  local IFS="$1"; shift; echo "$*";
}
echo "Enter the full path of pem file (ex:/home/hadoop/yourpem.pem):"
read pemfile
if [ ! -f "$pemfile" ]
then
	echo "pem file not found. Please put your pem file under /home/hadoop/ folder."
	exit
else
	chmod 400 "$pemfile"
fi
iplist=$(hdfs dfsadmin -report | grep ^Name | cut -f2 -d: | cut -f2 -d' ')
array=($iplist)
count=1
for private_ip in "${array[@]}"
do
    echo -e "\033[34mdeploy kafka for slave node:$private_ip\e[0m"
    scp -o stricthostkeychecking=no -i "$pemfile" "$home"/hello-samza/bin/grid hadoop@"$private_ip":/home/hadoop/
	# install kafka on slave node
	ssh -o stricthostkeychecking=no -i "$pemfile" hadoop@"$private_ip" 'mkdir -p hello-samza/bin;mv grid hello-samza/bin/;cd hello-samza;bin/grid install kafka'
	# configure kafka
	ssh -o stricthostkeychecking=no -i "$pemfile" hadoop@"$private_ip" 'sed -i ''/zookeeper.connect=/d'' /home/hadoop/hello-samza/deploy/kafka/config/server.properties;hdfs_path=$(grep -o "hdfs:[^<]*" /etc/hadoop/conf/core-site.xml);master_ip=$(expr "$hdfs_path" : "hdfs://\([^:]*\):");echo zookeeper.connect="$master_ip":2181 >> /home/hadoop/hello-samza/deploy/kafka/config/server.properties;echo delete.topic.enable=true >> /home/hadoop/hello-samza/deploy/kafka/config/server.properties;'
	ssh -o stricthostkeychecking=no -i "$pemfile" hadoop@"$private_ip" "sed -i ''/broker.id=/d'' /home/hadoop/hello-samza/deploy/kafka/config/server.properties;echo broker.id=$count >> /home/hadoop/hello-samza/deploy/kafka/config/server.properties"
	# start kafka
	ssh -o stricthostkeychecking=no -i "$pemfile" hadoop@"$private_ip" 'cd /home/hadoop/hello-samza;bin/grid start kafka&'
	# iptables
	ssh -o stricthostkeychecking=no -i "$pemfile" hadoop@"$private_ip" 'sudo service iptables save;sudo service iptables stop;sudo chkconfig iptables off'
	count=$((count+1))
done

ownDns=`ifconfig eth0 | grep inet | grep -v inet6 | awk '{print $2}' | cut -d ':' -f2`
ipArray=($iplist)
ipArray+=($ownDns)
brokerArray=( "${ipArray[@]/%/:9092}" )
brokerList=`join_arr , "${brokerArray[@]}"`
echo -e "\033[92mThe IP of the master is: $ownDns\e[0m"
echo -e "\033[92mThe IP list of Samza brokers in the cluster is given below for your reference. Copy it for pasting into .properties file and for submitting Task 1!\e[0m"
echo -e "\033[92m$brokerList\e[0m"

# Here is a scenario how to deploy custom connector example
# to Kafka "fast-data-dev" Landoop (Lenses) cluster running by docker

# 1) First assembly a fat jar for all files
if sbt assembly
then

# 2) Then mount `target` folder to docker filesystem
mkdir -p ~/kafka-lab/connectors
cp ./target/scala-2.12/kafka-lab-assembly-0.1.jar ~/kafka-lab/connectors

docker run --name fast-data-dev -it --rm -p 2181:2181 -p 3030:3030 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 9092:9092 -e ADV_HOST=127.0.0.1 -e RUNTESTS=0 -v ~/kafka-lab/connectors:/connectors/GitHub landoop/fast-data-dev

fi

# 3) Now you can open Lagom (Lenses) Kafka console at http://127.0.0.1:3030/#/
# Open "CONNECTORS" app, press "+" and create a `GitHubSourceConnector` connector.
# Now you can view your configured topic in a "TOPICS" app.
# Connectors app is here: at http://127.0.0.1:3030/kafka-connect-ui/#/cluster/fast-data-dev
# Topics app is here: http://127.0.0.1:3030/kafka-topics-ui/#/
# Logs are here: http://127.0.0.1:3030/logs/
# pdm

Start rabbitmq:
docker run -d --hostname dimm --name rabbitmq -e RABBITMQ_DEFAULT_USER=name -e RABBITMQ_DEFAULT_PASS=pass -p 8080:15672 -p 5672:5672 rabbitmq:management


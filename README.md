## pdm

Parallel data mover is a general tool for moving a huge collections of files between filesystems using distributed set of worker nodes with help of RabbitMQ messaging.

This version is significantly improved lustre-data-mover (https://github.com/sdsc/lustre-data-mover) written in golang.

In general it's a good idea to copy data in several passes. Migrating a large filesystem involves HPC cluster downtime, which can be minimised by copying most of the data from live filesystem and then finalising the copy during a downtime, when no changes are made by users.

One copy pass can contain either of 3 actions:

* Copying the data. The files on source different from ones on target (including the case when the last don't exist) are copied. Also involves creating folders on target.

* Delete. Needed for the final pass, when we want to delete files on target which were deleted by users from source after the last migration happened.

* Dirs mtime sync. After all changes to files are made on target filesystem, this pass will synchronize the target folders mtimes with ones on source to create an identical copy of the filesystem. (not yet implemented in this version)

_(source: the filesystem with users' data; target: the filesystem where data is being migrated to)_

#Building:

Install Go environment.

Run:
```
go get https://github.com/sdsc/pdm
cd $GOPATH/src/github.com/sdsc/pdm
go get ./...
go build
```

This will create pdm binary runnable in your OS. To create a binary for another OS use cross-compilation, f.e.: ```GOOS=linux GOARCH=amd64 go build``` would build a binary for 64-bit linux.

# Configuring:

To configure workers, copy config.toml.example file to either user's $HOME/.pdm or the folder where the script executes as config.toml and modify as needed.

In command mode the tool only needs the RabbitMQ server connect string in either a PDM_RABBITMQ environmental variable or as a --rabbitmq parameter.

# Running:

Let's assume we have two filesystems defined in the config file: "panda" and "badger", mounted at /mnt/panda and /mnt/badger. We want to copy /mnt/panda/user_xyz folder to /mnt/badger. The badger filesystem should have "write" attribute set to true in the config file.

To start regular file copy workers on a node, edit the config file appropriately and run ```./pdm worker```.

To send the initial copy command, set the RabbitMQ connect string for the client (see Configuring section) and run ```./pdm copy panda badger /user_xyz```. This will send initial RabbitMQ message to the queue and worker(s) will start copying the files.

# Monitoring:

To monitor the progress, install a Prometheus service (https://prometheus.io/). Start a pdm montoring daemon by running ```./pdm monitor``` - this will start a prometheus target on port 8082. Direct your prometheus service to this target and you will get the statistics collected in prometheus instance.

A nice tool to visualize the prometheus data is Grafana (https://grafana.com/). The example of dashboard is provided in this repo.

The logging library (https://github.com/sirupsen/logrus) can be configured to send logs to a variety of log collection tools. To send logs to elasticsearch, set up the server and set the elastic_url config parameter to its endpoint. Then use Kibana (https://www.elastic.co/products/kibana) to view the logs.
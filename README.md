Elasticsearch-HBase-River
==========================

This is a fork of the Elasticsearch-HBase-River. It was tested using elastic
search 337805d396326de8a2153b04a7d6e8e422677439 (0.93-SNAPSHOT).
https://github.com/mallocator/Elasticsearch-HBase-River
This one is slightly different in that it uses replication feature already in 
HBase. http://hbase.apache.org/replication.html. The plugin updates HBase's
zookeeper cluster to direct HBase to send WAL edits to the elastic search plugin. It is currently not in production and doesn't support deletes, and updates.

# Building

To build the plugin you need to have maven installed. With that in mind simply check out the project and run "mvn package" in the project directory. The plugin should then be available under target/release as a .zip file.

# Installation
For the elastic search directory run
./bin/plugin -i elasticsearch-river-hbase -u file:///[path to plugin zip file]
The plugin needs to be installed on all nodes of the ES cluster.

for more info on plugins check out http://www.elasticsearch.org/guide/reference/modules/plugins.html

# Usage

Check out the examples directory. It contains an import.sh script, which is 
used to initialize the hbase river with all necessary config data. It also
contains example_hbase_setup. These commands when run from the hbase shell will
tell hbase to start replication.

More info on how to use rivers can be found here: http://www.elasticsearch.org/guide/reference/river/

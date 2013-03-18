Elasticsearch-HBase-River
==========================

An import river similar to the elasticsearch mysql river

# Building

To build the plugin you need to have maven installed. With that in mind simply check out the project and run "mvn package" in the project directory. The plugin should then be available under target/release as a .zip file.

# Installation

Just copy the .zip file on the elasticsearch server should be using the plugin and run the "plugin" script coming with elasticsearch in the bin folder.

An Exmaple how one would call the plugin script:

	/my/elasticsearch/bin/plugin install river-hbase -url file:///path/to/plugin/river-hbase.zip

The plugin needs to be installed on all nodes of the ES cluster.

for more info on plugins check out http://www.elasticsearch.org/guide/reference/modules/plugins.html

# Usage

Check out the import.sh script, which is used to initialize the hbase river with all necessary config data.

More info on how to use rivers can be found here: http://www.elasticsearch.org/guide/reference/river/
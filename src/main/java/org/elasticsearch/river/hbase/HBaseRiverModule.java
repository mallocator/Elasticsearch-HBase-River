package org.elasticsearch.river.hbase;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 * Does the initial configuration of the Module, when it is called by ElasticSearch. Binds the HBase river as an eager
 * singleton river.
 * 
 * @author Ravi Gairola
 */
public class HBaseRiverModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(HBaseRiver.class).asEagerSingleton();
	}
}

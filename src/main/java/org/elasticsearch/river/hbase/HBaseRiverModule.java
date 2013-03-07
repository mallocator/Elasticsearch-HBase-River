package org.elasticsearch.river.hbase;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class HBaseRiverModule  extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(HBaseRiver.class).asEagerSingleton();
	}
}

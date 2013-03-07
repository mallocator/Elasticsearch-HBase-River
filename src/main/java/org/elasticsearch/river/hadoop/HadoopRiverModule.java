package org.elasticsearch.river.hadoop;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class HadoopRiverModule  extends AbstractModule {

	@Override
	protected void configure() {
		bind(River.class).to(HadoopRiver.class).asEagerSingleton();
	}
}

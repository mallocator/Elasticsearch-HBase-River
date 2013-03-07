package org.elasticsearch.plugin.river.hbase;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.hbase.HBaseRiverModule;

public class HBaseRiverPlugin  extends AbstractPlugin {

	@Inject
	public HBaseRiverPlugin() {}

	public String name() {
		return "river-hbase";
	}

	public String description() {
		return "River HBase Plugin";
	}

	public void onModule(final RiversModule module) {
		module.registerRiver("hbase", HBaseRiverModule.class);
	}
}

package org.elasticsearch.plugin.river.hadoop;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.hadoop.HadoopRiverModule;

public class HadoopRiverPlugin  extends AbstractPlugin {

	@Inject
	public HadoopRiverPlugin() {}

	public String name() {
		return "river-hadoop";
	}

	public String description() {
		return "River MySQL Plugin";
	}

	public void onModule(final RiversModule module) {
		module.registerRiver("hadoop", HadoopRiverModule.class);
	}
}

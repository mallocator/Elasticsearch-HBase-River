package org.elasticsearch.river.hadoop;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class HadoopRiver extends AbstractRiverComponent implements River {

	@Inject
	public HadoopRiver(RiverName riverName, RiverSettings settings) {
		super(riverName, settings);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
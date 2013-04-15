package org.elasticsearch.river.hbase;

import org.elasticsearch.common.logging.ESLogger;

import com.stumbleupon.async.Callback;

/**
 * A small helper class that will log any responses from HBase, in case there are any.
 * 
 * @author Ravi Gairola
 */
public class HBaseCallbackLogger implements Callback<Object, Object> {
	private final ESLogger	logger;
	private final String	realm;

	public HBaseCallbackLogger(final ESLogger logger, final String realm) {
		this.logger = logger;
		this.realm = realm;
	}

	@Override
	public Object call(final Object arg) throws Exception {
		if (arg instanceof Throwable) {
			this.logger.error("An async error has been caught from HBase within {}:", (Throwable) arg, this.realm);
		}
		else {
			this.logger.trace("Got response from HBase within {}: {}", this.realm, arg);
		}
		return arg;
	}
}

package org.aasharblog.nifi4trading.FixEngineToNiFi;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

public class NiFiDispatcher {
	private SiteToSiteClient client;
	private SiteToSiteClientConfig clientConfig;

	public NiFiDispatcher(SiteToSiteClientConfig clientConfig) {
		this.clientConfig = clientConfig;
	}

	public void start() throws Exception {
		this.client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
	}

	public void send(String message) throws Exception {
		final Transaction transaction = client.createTransaction(TransferDirection.SEND);
		if (transaction == null) {
			throw new IllegalStateException("Unable to create a NiFi Transaction to send data");
		}

		transaction.send(message.getBytes(), getAttributes());
		transaction.confirm();
		transaction.complete();
	}

	private Map<String,String> getAttributes() {
		return new HashMap<String,String>();
	}
	
	public void stop() throws IOException {
		client.close();
	}
}

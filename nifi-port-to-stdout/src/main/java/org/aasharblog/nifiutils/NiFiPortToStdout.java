package org.aasharblog.nifiutils;

import java.io.IOException;
import java.io.InputStream;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NiFiPortToStdout {
    final static Logger logger = LoggerFactory.getLogger(NiFiPortToStdout.class);

    private final SiteToSiteClientConfig clientConfig;

    public NiFiPortToStdout(final SiteToSiteClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public void start(String nifiPortName) {
        System.out.println("Listening");

        while(true) {
        	SiteToSiteClient client = null;

        	try {
	        	client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
                while(true) {
                	Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
            		DataPacket dataPacket = transaction.receive();
	        		
		            if (dataPacket == null) {
		                transaction.confirm();
		                transaction.complete();
		            }
		            else {
			            final InputStream inStream = dataPacket.getData();
			            final byte[] data = new byte[(int) dataPacket.getSize()];
			            StreamUtils.fillBuffer(inStream, data);
			
			            System.out.println(new String(data));
			            dataPacket = transaction.receive();
			
			            transaction.confirm();
			            transaction.complete();
		            }
                }
        	} catch (Exception ex) {
        		logger.error("Error in reading data from NiFi Port", ex);
	        } finally {
	            try {
	                client.close();
	            } catch (final IOException ioe) {
	        		logger.error("Failed to close client", ioe);
	                ioe.printStackTrace();
	            }
        	}
        	
            // no data available. Wait a bit and try again
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
            }
        }
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Syntax <exec> <url> <NiFi Portname>");
			return;
		}
		SiteToSiteClientConfig config = new SiteToSiteClient.Builder()
			 .url(args[0])
			 .portName(args[1])
			 .requestBatchCount(1)
			 .buildConfig();

		new NiFiPortToStdout(config).start(args[1]);
	}
}

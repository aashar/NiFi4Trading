package org.aasharblog.nifi4trading.FixEngineToNiFi;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.FieldConvertError;
import quickfix.FileStoreFactory;
import quickfix.LogFactory;
import quickfix.ScreenLogFactory;
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;

public class Engine implements MessageListener {
	private final SessionSettings settings;
    private final static String NIFI_PORT_URL = "NiFiPortURL";
    private final static String NIFI_PORT_NAME = "NiFiPortName";

    private final SocketAcceptor acceptor;
    NiFiDispatcher niFiDispatcher;

    private List<MessageListener> listeners = new ArrayList<MessageListener>();

    public void addListener(MessageListener toAdd) {
        listeners.add(toAdd);
    }

    public Engine(InputStream configFileStream) throws ConfigError, FieldConvertError {
        settings = new SessionSettings(configFileStream);

        Application application = new Application(settings);
        FileStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory(settings);
        acceptor = new SocketAcceptor(application, storeFactory, settings,
                logFactory, new DefaultMessageFactory());

		SiteToSiteClientConfig sendToNiFiPortConfig = new SiteToSiteClient.Builder()
				 .url(settings.getString(NIFI_PORT_URL))
				 .portName(settings.getString(NIFI_PORT_NAME))
				 .requestBatchCount(1)
				 .buildConfig();

		application.addListener(this);

		niFiDispatcher = new NiFiDispatcher(sendToNiFiPortConfig);
	}
	
	private void run() throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        
		niFiDispatcher.start();
        acceptor.start();

        String value = "";
        while (value != "#quit") {
            System.out.println("type #quit to quit");
            value = in.readLine();
        }

        niFiDispatcher.stop();
        acceptor.stop();
	}

	public static void main(String[] args) {
		
        try {
            InputStream configFileStream = null;
            if (args.length == 0) {
                configFileStream = Engine.class.getResourceAsStream("dcrec.cfg");
            } else if (args.length == 1) {
                configFileStream = new FileInputStream(args[0]);
            }
            if (configFileStream == null) {
                System.out.println("usage: " + Engine.class.getName() + " [configFile].");
                return;
            }
            
            new Engine(configFileStream).run();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
 	}

	@Override
	public void messageReceived(String string, Map<String, String> attr) throws Exception {
		niFiDispatcher.send(string, attr);
	}
}

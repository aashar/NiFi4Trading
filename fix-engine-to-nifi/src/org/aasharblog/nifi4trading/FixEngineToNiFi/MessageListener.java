package org.aasharblog.nifi4trading.FixEngineToNiFi;

import java.util.Map;

import quickfix.FieldNotFound;
import quickfix.InvalidMessage;
import quickfix.SessionNotFound;

public interface MessageListener {
	void messageReceived(String string, Map<String, String> attr)
			throws SessionNotFound, FieldNotFound, InvalidMessage, Exception;
}

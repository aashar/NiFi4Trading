package org.aasharblog.nifi4trading.FixEngineToNiFi;

import quickfix.FieldNotFound;
import quickfix.InvalidMessage;
import quickfix.SessionNotFound;

public interface MessageListener {
	void messageReceived(String string) throws SessionNotFound, FieldNotFound, InvalidMessage, Exception;
}

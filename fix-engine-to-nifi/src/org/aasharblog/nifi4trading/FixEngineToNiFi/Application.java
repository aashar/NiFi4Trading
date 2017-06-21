package org.aasharblog.nifi4trading.FixEngineToNiFi;

import java.util.ArrayList;
import java.util.List;

import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.InvalidMessage;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.UnsupportedMessageType;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

public class Application implements quickfix.Application, MessageListener {
    public void fromAdmin(Message message, SessionID sessionId) throws FieldNotFound,
            IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    }

    public void fromApp(Message message, SessionID sessionId) throws FieldNotFound,
            IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
    	for (MessageListener listener : listeners) {
    		try {
				listener.messageReceived(message.toString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }

    public void onCreate(SessionID sessionId) {
    }

    public void onLogon(SessionID sessionId) {
        System.out.println("Logon - " + sessionId);
    }

    public void onLogout(SessionID sessionId) {
        System.out.println("Logout - " + sessionId);
    }

    public void toAdmin(Message message, SessionID sessionId) {
        // empty
    }

    public void toApp(Message message, SessionID sessionId) throws DoNotSend {
        // empty
    }
    
    public void sendMessage(Message message) throws SessionNotFound, FieldNotFound {
    	Session.sendToTarget(message,
    			message.getHeader().getString(SenderCompID.FIELD),
    			message.getHeader().getString(TargetCompID.FIELD));
    }

	@Override
	public void messageReceived(String messageString) throws InvalidMessage, SessionNotFound, FieldNotFound {
		Message message = new Message(messageString);
		sendMessage(message);
	}

    private List<MessageListener> listeners = new ArrayList<MessageListener>();

    public void addListener(MessageListener toAdd) {
        listeners.add(toAdd);
    }
}

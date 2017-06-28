package org.aasharblog.nifi4trading.FixEngineToNiFi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.DoNotSend;
import quickfix.FieldConvertError;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.InvalidMessage;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SessionSettings;
import quickfix.UnsupportedMessageType;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

public class Application implements quickfix.Application, MessageListener {
    DataDictionary dd = null;
    List<Integer> fieldsList = null;

	public Application(SessionSettings settings) throws ConfigError, FieldConvertError {
		List<String> fieldsListString
			= Arrays.asList(settings.getString("NiFiAttributes")
					.trim()
					.split("\\s*,\\s*"));

		fieldsList = fieldsListString
						.stream()
						.map(Integer::parseInt)
						.collect(Collectors.toList());
	}

    public void fromAdmin(Message message, SessionID sessionId) throws FieldNotFound,
            IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    	// TODO: Send non-app messages
    }

    public void fromApp(Message message, SessionID sessionId) throws FieldNotFound,
            IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {

    	Map<String, String> attr = new HashMap<String, String>();
    	attr.put("Type", "AppMsg"); // Differentiates from the non-app messages

        DataDictionary dd = Session.lookupSession(sessionId).getDataDictionary();
	    for (Integer fieldNo : fieldsList) {
    		try {
	            if (dd.isField(fieldNo)) {
	            	attr.put(dd.getFieldName(fieldNo),
	        			dd.isHeaderField(fieldNo) ? message.getHeader().getString(fieldNo)
	    					: dd.isTrailerField(fieldNo) ? message.getTrailer().getString(fieldNo)
							: message.getString(fieldNo)
	    			);
	            }
	            else {
	            	// Unknown field
	            }
        	} catch (FieldNotFound exc) {
				// Ignore?
			}
	    }	

/*    	attr.put("SenderCompID", message.getHeader().getString(SenderCompID.FIELD));
    	attr.put("TargetCompID", message.getHeader().getString(TargetCompID.FIELD));
    	attr.put("MsgSeqNum", message.getHeader().getString(MsgSeqNum.FIELD));
    	if (message.isSetField(PossDupFlag.FIELD))
    		attr.put("PossDupFlag", message.getHeader().getString(PossDupFlag.FIELD));
*/
    	for (MessageListener listener : listeners) {
    		try {
				listener.messageReceived(message.toString(), attr);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }

    public void onCreate(SessionID sessionId) {
    	// TODO: Send non-app messages
    }

    public void onLogon(SessionID sessionId) {
    	// TODO: Send non-app messages
        System.out.println("Logon - " + sessionId);
    }

    public void onLogout(SessionID sessionId) {
    	// TODO: Send non-app messages
        System.out.println("Logout - " + sessionId);
    }

    public void toAdmin(Message message, SessionID sessionId) { }

    public void toApp(Message message, SessionID sessionId) throws DoNotSend { }
    
    public void sendMessage(Message message) throws SessionNotFound, FieldNotFound {
    	Session.sendToTarget(message,
    			message.getHeader().getString(SenderCompID.FIELD),
    			message.getHeader().getString(TargetCompID.FIELD));
    }

	@Override
	public void messageReceived(String messageString, Map<String, String> attr)
			throws InvalidMessage, SessionNotFound, FieldNotFound {
		// TODO: Support multiple sessions
		Message message = new Message(messageString);
		sendMessage(message);
	}

    private List<MessageListener> listeners = new ArrayList<MessageListener>();

    public void addListener(MessageListener toAdd) {
        listeners.add(toAdd);
    }
}

package org.aasharblog.nifi4trading.fix_utils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.FieldNotFound;
import quickfix.FixVersions;
import quickfix.Message;
import quickfix.SessionID;
import quickfix.field.*;

public class OrderFiller {
    private double defaultPrice;
    private DataDictionary dd = null;

    private final HashSet<String> validOrderTypes = new HashSet<String>();

    public OrderFiller(DataDictionary dd, double defaultPrice, String validOrderTypesList)
			throws ConfigError {
		this.dd = dd;
		this.defaultPrice = defaultPrice;
		initializeValidOrderTypes(validOrderTypesList);
    }

    public OrderFiller(String dataDictionaryFile, double defaultPrice, String validOrderTypesList)
			throws ConfigError {
		dd = new DataDictionary(dataDictionaryFile);
		this.defaultPrice = defaultPrice;
		initializeValidOrderTypes(validOrderTypesList);
    }

    public Message parseFIXString(String messageString) throws Exception {
    	Message fixMessage = new Message();

        fixMessage.fromString(messageString, dd, false);
        
        if (!validateOrder(fixMessage)) {
        	throw new Exception("Invalid Message");
        }

    	return fixMessage;
    }

    private void initializeValidOrderTypes(String validOrderTypesList) throws ConfigError {
        if (validOrderTypesList.trim().length()!=0) {
            List<String> orderTypes = Arrays
                    .asList(validOrderTypesList.trim().split("\\s*,\\s*"));
            validOrderTypes.addAll(orderTypes);
        } else {
            validOrderTypes.add(OrdType.LIMIT + "");
        }
    }

    private boolean validateOrder(Message order) throws Exception {
        if (!order.getHeader().getString(MsgType.FIELD).equals("D")) {
        	return false;
        }
        
        OrdType ordType = new OrdType(order.getChar(OrdType.FIELD));
        if (!validOrderTypes.contains(Character.toString(ordType.getValue()))) {
        	return false;
        }

    	return true;
    }

    public List<Message> processFix40Order(Message fixMessage) throws FieldNotFound {
    	List<Message> returnMessages = new LinkedList<Message>();

    	OrderQty orderQty = new OrderQty(fixMessage.getDouble(OrderQty.FIELD));
	    ClOrdID clOrdID = new ClOrdID(fixMessage.getString(ClOrdID.FIELD));
	    Symbol symbol = new Symbol(fixMessage.getString(Symbol.FIELD));
	    Side side = new Side(fixMessage.getChar(Side.FIELD));
	    Price price = getPrice(fixMessage);

	    quickfix.fix40.ExecutionReport ack = new quickfix.fix40.ExecutionReport(genOrderID(), genExecID(),
	            new ExecTransType(ExecTransType.NEW), new OrdStatus(OrdStatus.NEW), symbol, side,
	            orderQty, new LastShares(0), new LastPx(0), new CumQty(0), new AvgPx(0));
	
	    ack.set(clOrdID);
        setSessionID(ack, quickfix.MessageUtils.getReverseSessionID(fixMessage));
	    returnMessages.add(ack);
	
	    if (isOrderExecutable(fixMessage, price)) {
	        quickfix.fix40.ExecutionReport fill = new quickfix.fix40.ExecutionReport(genOrderID(), genExecID(),
	                new ExecTransType(ExecTransType.NEW), new OrdStatus(OrdStatus.FILLED), symbol, side,
	                orderQty, new LastShares(orderQty.getValue()), new LastPx(price.getValue()),
	                new CumQty(orderQty.getValue()), new AvgPx(price.getValue()));

	        fill.set(clOrdID);
	        setSessionID(fill, quickfix.MessageUtils.getReverseSessionID(fixMessage));

		    returnMessages.add(fill);
	    }
	    
	    return returnMessages;
    }

    public List<Message> processFix41Order(Message fixMessage) throws FieldNotFound {
    	List<Message> returnMessages = new LinkedList<Message>();

    	OrderQty orderQty = new OrderQty(fixMessage.getDouble(OrderQty.FIELD));
	    ClOrdID clOrdID = new ClOrdID(fixMessage.getString(ClOrdID.FIELD));
	    Symbol symbol = new Symbol(fixMessage.getString(Symbol.FIELD));
	    Side side = new Side(fixMessage.getChar(Side.FIELD));
	    Price price = getPrice(fixMessage);

		quickfix.fix41.ExecutionReport ack = new quickfix.fix41.ExecutionReport(genOrderID(), genExecID(),
		        new ExecTransType(ExecTransType.NEW), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.NEW),
                symbol, side, orderQty, new LastShares(0), new LastPx(0), new LeavesQty(0), new CumQty(0), new AvgPx(0));
		
		ack.set(clOrdID);
        setSessionID(ack, quickfix.MessageUtils.getReverseSessionID(fixMessage));
	    returnMessages.add(ack);
		
		if (isOrderExecutable(fixMessage, price)) {
		    quickfix.fix41.ExecutionReport executionReport = new quickfix.fix41.ExecutionReport(genOrderID(),
		            genExecID(), new ExecTransType(ExecTransType.NEW), new ExecType(ExecType.FILL), new OrdStatus(
		                    OrdStatus.FILLED), symbol, side, orderQty, new LastShares(orderQty
		                    .getValue()), new LastPx(price.getValue()), new LeavesQty(0), new CumQty(orderQty
		                    .getValue()), new AvgPx(price.getValue()));
		
		    executionReport.set(clOrdID);
	        setSessionID(executionReport, quickfix.MessageUtils.getReverseSessionID(fixMessage));
		
		    returnMessages.add(executionReport);
		}
	    
	    return returnMessages;
    }

    public List<Message> processFix42Order(Message fixMessage) throws FieldNotFound {
    	List<Message> returnMessages = new LinkedList<Message>();

    	OrderQty orderQty = new OrderQty(fixMessage.getDouble(OrderQty.FIELD));
	    ClOrdID clOrdID = new ClOrdID(fixMessage.getString(ClOrdID.FIELD));
	    Symbol symbol = new Symbol(fixMessage.getString(Symbol.FIELD));
	    Side side = new Side(fixMessage.getChar(Side.FIELD));
	    Price price = getPrice(fixMessage);

		quickfix.fix42.ExecutionReport ack = new quickfix.fix42.ExecutionReport(genOrderID(), genExecID(),
		        new ExecTransType(ExecTransType.NEW), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.NEW),
		        symbol, side, new LeavesQty(0), new CumQty(0), new AvgPx(0));
		
		ack.set(clOrdID);
        setSessionID(ack, quickfix.MessageUtils.getReverseSessionID(fixMessage));
	    returnMessages.add(ack);
		
		if (isOrderExecutable(fixMessage, price)) {
		    quickfix.fix42.ExecutionReport executionReport = new quickfix.fix42.ExecutionReport(genOrderID(),
	            genExecID(), new ExecTransType(ExecTransType.NEW), new ExecType(ExecType.FILL),
	            new OrdStatus(OrdStatus.FILLED), symbol, side, new LeavesQty(0), new CumQty(orderQty.getValue()),
	            new AvgPx(price.getValue()));
		
		    executionReport.set(clOrdID);
		    executionReport.set(orderQty);
		    executionReport.set(new LastShares(orderQty.getValue()));
		    executionReport.set(new LastPx(price.getValue()));
	        setSessionID(executionReport, quickfix.MessageUtils.getReverseSessionID(fixMessage));
		
		    returnMessages.add(executionReport);
		}
	    
	    return returnMessages;
    }
    
    public List<Message> processFix43Order(Message fixMessage) throws FieldNotFound {
    	List<Message> returnMessages = new LinkedList<Message>();

    	OrderQty orderQty = new OrderQty(fixMessage.getDouble(OrderQty.FIELD));
	    ClOrdID clOrdID = new ClOrdID(fixMessage.getString(ClOrdID.FIELD));
	    Symbol symbol = new Symbol(fixMessage.getString(Symbol.FIELD));
	    Side side = new Side(fixMessage.getChar(Side.FIELD));
	    Price price = getPrice(fixMessage);
	    
		quickfix.fix43.ExecutionReport ack = new quickfix.fix43.ExecutionReport(
	            genOrderID(), genExecID(), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.NEW),
	            side, new LeavesQty(orderQty.getValue()), new CumQty(0), new AvgPx(0));

		ack.set(clOrdID);
		ack.set(symbol);
        setSessionID(ack, quickfix.MessageUtils.getReverseSessionID(fixMessage));
	    returnMessages.add(ack);

		if (isOrderExecutable(fixMessage, price)) {
		    quickfix.fix43.ExecutionReport executionReport = new quickfix.fix43.ExecutionReport(genOrderID(),
		            genExecID(), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.FILLED), side,
		            new LeavesQty(0), new CumQty(orderQty.getValue()), new AvgPx(price.getValue()));

		    executionReport.set(clOrdID);
		    executionReport.set(symbol);
		    executionReport.set(orderQty);
		    executionReport.set(new LastQty(orderQty.getValue()));
		    executionReport.set(new LastPx(price.getValue()));
	        setSessionID(executionReport, quickfix.MessageUtils.getReverseSessionID(fixMessage));

		    returnMessages.add(executionReport);
		}
	    
	    return returnMessages;
    }

    public List<Message> processFix44Order(Message fixMessage) throws FieldNotFound {
    	List<Message> returnMessages = new LinkedList<Message>();

    	OrderQty orderQty = new OrderQty(fixMessage.getDouble(OrderQty.FIELD));
	    ClOrdID clOrdID = new ClOrdID(fixMessage.getString(ClOrdID.FIELD));
	    Symbol symbol = new Symbol(fixMessage.getString(Symbol.FIELD));
	    Side side = new Side(fixMessage.getChar(Side.FIELD));
	    Price price = getPrice(fixMessage);
	    
		quickfix.fix44.ExecutionReport ack = new quickfix.fix44.ExecutionReport(
	            genOrderID(), genExecID(), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.NEW),
	            side, new LeavesQty(orderQty.getValue()), new CumQty(0), new AvgPx(0));

		ack.set(clOrdID);
		ack.set(symbol);
        setSessionID(ack, quickfix.MessageUtils.getReverseSessionID(fixMessage));
	    returnMessages.add(ack);

		if (isOrderExecutable(fixMessage, price)) {
		    quickfix.fix44.ExecutionReport executionReport = new quickfix.fix44.ExecutionReport(genOrderID(),
	            genExecID(), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.FILLED), side,
	            new LeavesQty(0), new CumQty(orderQty.getValue()), new AvgPx(price.getValue()));

		    executionReport.set(clOrdID);
		    executionReport.set(symbol);
		    executionReport.set(orderQty);
		    executionReport.set(new LastQty(orderQty.getValue()));
		    executionReport.set(new LastPx(price.getValue()));
	        setSessionID(executionReport, quickfix.MessageUtils.getReverseSessionID(fixMessage));

		    returnMessages.add(executionReport);
		}

		return returnMessages;
    }
	    
    public List<Message> processFix50Order(Message fixMessage) throws FieldNotFound {
    	List<Message> returnMessages = new LinkedList<Message>();

    	OrderQty orderQty = new OrderQty(fixMessage.getDouble(OrderQty.FIELD));
	    ClOrdID clOrdID = new ClOrdID(fixMessage.getString(ClOrdID.FIELD));
	    Symbol symbol = new Symbol(fixMessage.getString(Symbol.FIELD));
	    Side side = new Side(fixMessage.getChar(Side.FIELD));
	    Price price = getPrice(fixMessage);
	    
	    quickfix.fix50.ExecutionReport ack = new quickfix.fix50.ExecutionReport(
	            genOrderID(), genExecID(), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.NEW),
	            side, new LeavesQty(orderQty.getValue()), new CumQty(0));
	
		ack.set(clOrdID);
		ack.set(symbol);
        setSessionID(ack, quickfix.MessageUtils.getReverseSessionID(fixMessage));
	    returnMessages.add(ack);
	
	    if (isOrderExecutable(fixMessage, price)) {
	        quickfix.fix50.ExecutionReport executionReport = new quickfix.fix50.ExecutionReport(
	                genOrderID(), genExecID(), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.FILLED),
	                side, new LeavesQty(0), new CumQty(orderQty.getValue()));
	
		    executionReport.set(clOrdID);
		    executionReport.set(symbol);
		    executionReport.set(orderQty);
		    executionReport.set(new LastQty(orderQty.getValue()));
		    executionReport.set(new LastPx(price.getValue()));
	        executionReport.set(new AvgPx(price.getValue()));
	        setSessionID(executionReport, quickfix.MessageUtils.getReverseSessionID(fixMessage));
	        
		    returnMessages.add(executionReport);
	    }

		return returnMessages;
    }

    public List<Message> processMessage(Message fixMessage) throws Exception {
	    switch (fixMessage.getHeader().getString(BeginString.FIELD)) {
		    case FixVersions.BEGINSTRING_FIX40 :
        		return processFix40Order(fixMessage);
		    case FixVersions.BEGINSTRING_FIX41 :
        		return processFix41Order(fixMessage);
		    case FixVersions.BEGINSTRING_FIX42 :
        		return processFix42Order(fixMessage);
		    case FixVersions.BEGINSTRING_FIX43 :
        		return processFix43Order(fixMessage);
		    case FixVersions.BEGINSTRING_FIX44 :
        		return processFix44Order(fixMessage);
		    case FixVersions.BEGINSTRING_FIXT11:
        		return processFix50Order(fixMessage);
    		default:
        		throw new Exception("Message version not implemented");
    	}
	}

	private boolean isOrderExecutable(Message order, Price price) throws FieldNotFound {
		if (order.getChar(OrdType.FIELD) == OrdType.LIMIT) {
		    BigDecimal limitPrice = new BigDecimal(order.getString(Price.FIELD));
		    char side = order.getChar(Side.FIELD);
		    BigDecimal thePrice = new BigDecimal(""+ price.getValue());
		
		    return (side == Side.BUY && thePrice.compareTo(limitPrice) <= 0)
		            || ((side == Side.SELL || side == Side.SELL_SHORT) && thePrice.compareTo(limitPrice) >= 0);
		}
		return true;
	}
	
	private Price getPrice(Message message) throws FieldNotFound {
		Price price;
		if (message.getChar(OrdType.FIELD) == OrdType.LIMIT) {
		    price = new Price(message.getDouble(Price.FIELD));
		} else {
		    price = new Price(defaultPrice);  // TO-DO use appropriate price
		}
		return price;
	}
	
	private void setSessionID(Message fixMessage, SessionID session) {
        fixMessage.getHeader().setString(SenderCompID.FIELD, session.getSenderCompID());
        fixMessage.getHeader().setString(TargetCompID.FIELD, session.getTargetCompID());
	}
	
	private OrderID genOrderID() {
		return new OrderID(Integer.valueOf(++m_orderID).toString());
	}
	
	private ExecID genExecID() {
		return new ExecID(Integer.valueOf(++m_execID).toString());
	}
	
	private int m_orderID = 0;
	private int m_execID = 0;
}

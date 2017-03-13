package org.aasharblog.nifi4trading.qfixsamples.matching;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import quickfix.field.MsgType;
import quickfix.field.OrdStatus;

public class OrderMatcher {
	// Implement message type V and W for market data
    private HashMap<String, Market> markets = new HashMap<String, Market>();
    private IdGenerator generator = new IdGenerator();

    public ArrayList<ExecReport> processOrder(OrderRequest order, boolean performMatch) throws Exception {
		ArrayList<ExecReport> orderUpdates = new ArrayList<ExecReport>();

    	if (order.getMsgType().equals(MsgType.ORDER_CANCEL_REQUEST)) {
	        try {
	    		order.validateCancelOrder();
	        } catch (Exception e) {
	            orderUpdates.add(
	        		order.updateOrder(OrdStatus.REJECTED, e.getMessage(), generator.genExecutionID())
	    		);
	
	            return orderUpdates;
	        }

    		orderUpdates.add(
				processCancelOrder(order)
			);

            return orderUpdates;
    	}
    	else if (order.getMsgType().equals(MsgType.ORDER_SINGLE)) {
	        try {
	        	order.validateNewOrder();
	        } catch (Exception e) {
	            orderUpdates.add(
	        		order.updateOrder(OrdStatus.REJECTED, e.getMessage(), generator.genExecutionID())
	    		);
	
	            return orderUpdates;
	        }

	        getMarket(order.getSymbol())
	        	.insert(order);

        	orderUpdates.add(
    			order.updateOrder(OrdStatus.NEW, "", generator.genExecutionID())
			);

        	if (performMatch) {
	            ArrayList<OrderRequest> orders = match(order.getSymbol());

	            while (orders.size() > 0) {
	            	boolean isFilled = orders.get(0).isFilled();
	            	orderUpdates.add(
            			orders.remove(0)
            				.updateOrder(isFilled ? OrdStatus.FILLED : OrdStatus.PARTIALLY_FILLED, "", generator.genExecutionID())
        			);
	            }
        	}

	        return orderUpdates;
    	}
    	else
    		throw new Exception("Order type not implemented" + order.getMsgType());
    }

    public ArrayList<ExecReport> match() {
    	ArrayList<ExecReport> orderUpdates = new ArrayList<ExecReport>();

    	for (String symbol : getMarkets().keySet()) {
        	ArrayList<OrderRequest> symbolOrders = match(symbol);
    		
            while (symbolOrders.size() > 0) {
            	boolean isFilled = symbolOrders.get(0).isFilled();
            	orderUpdates.add(
        			symbolOrders
        				.remove(0)
        					.updateOrder(isFilled ? OrdStatus.FILLED : OrdStatus.PARTIALLY_FILLED, "", generator.genExecutionID())
    			);
            }
    	}

    	return orderUpdates;
    }

    private ArrayList<OrderRequest> match(String symbol) {
        ArrayList<OrderRequest> orders = new ArrayList<OrderRequest>();
        getMarket(symbol).match(symbol, orders);
        return orders;
    }

    private ExecReport processCancelOrder(OrderRequest order) {
        OrderRequest origOrder =
    		getMarket(order.getSymbol())
    			.find(order.getSide(), order.getOrigClOrderId());

        ExecReport execReport = origOrder.cancel(generator.genExecutionID());

    	getMarket(origOrder.getSymbol())
    		.erase(origOrder);

    	return execReport;
    }

    public String toString() {
    	StringBuffer buf = new StringBuffer();
    	
        for (Iterator<String> iter = getMarkets().keySet().iterator(); iter.hasNext();) {
            String symbol = iter.next();
            buf.append("MARKET: " + symbol + "\n");
            display(symbol);
        }
        
        return buf.toString();
    }

    public String toString(String symbol) {
    	StringBuffer buf = new StringBuffer();
        getMarket(symbol).display();
        return buf.toString();
    }

    public void display() {
        for (Iterator<String> iter = getMarkets().keySet().iterator(); iter.hasNext();) {
            String symbol = iter.next();
            System.out.println("MARKET: " + symbol);
            display(symbol);
        }
    }

    public void display(String symbol) {
        getMarket(symbol).display();
    }

    Market getMarket(String symbol) {
        Market m = getMarkets().get(symbol);
        if (m == null) {
            m = new Market();
            getMarkets().put(symbol, m);
        }
        return m;
    }

	public HashMap<String, Market> getMarkets() {
		return markets;
	}

	public void setMarkets(HashMap<String, Market> markets) {
		this.markets = markets;
	}
}
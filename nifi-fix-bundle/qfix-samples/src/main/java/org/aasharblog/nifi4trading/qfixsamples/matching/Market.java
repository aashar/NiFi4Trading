package org.aasharblog.nifi4trading.qfixsamples.matching;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import quickfix.field.OrdType;
import quickfix.field.Side;

public class Market {

    private List<OrderRequest> bidOrders = new ArrayList<OrderRequest>();
    private List<OrderRequest> askOrders = new ArrayList<OrderRequest>();

    public boolean match(String symbol, List<OrderRequest> orders) {
        while (true) {
            if (getBidOrders().size() == 0 || getAskOrders().size() == 0) {
                return orders.size() != 0;
            }
            OrderRequest bidOrder = getBidOrders().get(0);
            OrderRequest askOrder = getAskOrders().get(0);
            if (bidOrder.getType() == OrdType.MARKET || askOrder.getType() == OrdType.MARKET
                    || (bidOrder.getPrice() >= askOrder.getPrice())) {
                match(bidOrder, askOrder);
                if (!orders.contains(bidOrder)) {
                    orders.add(0, bidOrder);
                }
                if (!orders.contains(askOrder)) {
                    orders.add(0, askOrder);
                }

                if (bidOrder.isClosed()) {
                    getBidOrders().remove(bidOrder);
                }
                if (askOrder.isClosed()) {
                    getAskOrders().remove(askOrder);
                }
            } else
                return orders.size() != 0;
        }
    }

    private void match(OrderRequest bid, OrderRequest ask) {
        double price = ask.getType() == OrdType.LIMIT ? ask.getPrice() : bid.getPrice();
        long quantity = 0;

        if (bid.getOpenQuantity() >= ask.getOpenQuantity())
            quantity = ask.getOpenQuantity();
        else
            quantity = bid.getOpenQuantity();

        bid.execute(price, quantity);
        ask.execute(price, quantity);
    }

    public void insert(OrderRequest order) {
        if (order.getSide() == Side.BUY) {
            insert(order, true, getBidOrders());
        } else {
            insert(order, false, getAskOrders());
        }
    }

    private void insert(OrderRequest order, boolean descending, List<OrderRequest> orders) {
    	Comparator<OrderRequest> c = new Comparator<OrderRequest>()
        {
            public int compare(OrderRequest ls, OrderRequest rs)
            {
            	if (descending) {
            		if (ls.getPrice() == rs.getPrice()) {
                		if (ls.getEntryTime() < rs.getEntryTime())
                			return 1;
                		else if (ls.getEntryTime() == rs.getEntryTime())
                			return 0;
                		else
                			return -1;
            		}
            		else {
                		if (ls.getPrice() < rs.getPrice())
                			return 1;
                		else
                			return -1;
            		}
            	}
            	else {
            		if (ls.getPrice() == rs.getPrice()) {
                		if (ls.getEntryTime() > rs.getEntryTime())
                			return 1;
                		else if (ls.getEntryTime() == rs.getEntryTime())
                			return 0;
                		else
                			return -1;
            		}
            		else {
                		if (ls.getPrice() > rs.getPrice())
                			return 1;
                		else
                			return -1;
            		}
            	}
            }
        };

        if (orders.size() == 0) {
            orders.add(order);
        } else if (order.getType() == OrdType.MARKET) {
            orders.add(0, order);
        } else {
        	int ind = Collections.binarySearch(orders, order, c);
        	orders.add(ind <= 0 ?  -1 * ind - 1 : ind, order);

/*            for (int i = 0; i < orders.size(); i++) {
                OrderRequest o = orders.get(i);
                if ((descending ? order.getPrice() > o.getPrice() : order.getPrice() < o.getPrice())
                        && order.getEntryTime() < o.getEntryTime()) {
                    orders.add(i, order);
                }
            }
            orders.add(order); */
        }
    }

    public void erase(OrderRequest order) {
        if (order.getSide() == Side.BUY) {
            getBidOrders().remove(find(getBidOrders(), order.getClientOrderId()));
        } else {
            getAskOrders().remove(find(getAskOrders(), order.getClientOrderId()));
        }
    }

    public OrderRequest find(char side, String id) {
        OrderRequest order = null;
        if (side == Side.BUY) {
            order = find(getBidOrders(), id);
        } else {
            order = find(getAskOrders(), id);
        }
        return order;
    }

    private OrderRequest find(List<OrderRequest> orders, String clientOrderId) {
        for (int i = 0; i < orders.size(); i++) {
            OrderRequest o = orders.get(i);
            if (o.getClientOrderId().equals(clientOrderId)) {
                return o;
            }
        }
        return null;
    }
    
    public String toString() {
    	StringBuffer buf = new StringBuffer();

    	buf.append(toString(getBidOrders(), "BIDS") + "\n");
    	buf.append(toString(getAskOrders(), "ASKS"));
        
        return buf.toString();
    }
    
    public String toString(List<OrderRequest> orders, String title) {  // Appropriate name?
    	StringBuffer buf = new StringBuffer();

    	DecimalFormat priceFormat = new DecimalFormat("#.00");
        DecimalFormat qtyFormat = new DecimalFormat("######");
        System.out.println(title + ":\n----");
        for (int i = 0; i < orders.size(); i++) {
            OrderRequest order = orders.get(i);
            buf.append("  $" + priceFormat.format(order.getPrice()) + " "
                    + qtyFormat.format(order.getOpenQuantity()) + " " + order.getOwner() + " "
                    + new Date(order.getEntryTime()) + "\n");
        }
        buf.deleteCharAt(buf.length()-1);

        return buf.toString();
    }

    public void display() {
        displaySide(getBidOrders(), "BIDS");
        displaySide(getAskOrders(), "ASKS");
    }

    private void displaySide(List<OrderRequest> orders, String title) {
        DecimalFormat priceFormat = new DecimalFormat("#.00");
        DecimalFormat qtyFormat = new DecimalFormat("######");
        System.out.println(title + ":\n----");
        for (int i = 0; i < orders.size(); i++) {
            OrderRequest order = orders.get(i);
            System.out.println("  $" + priceFormat.format(order.getPrice()) + " "
                    + qtyFormat.format(order.getOpenQuantity()) + " " + order.getOwner() + " "
                    + new Date(order.getEntryTime()));
        }
    }

	public List<OrderRequest> getBidOrders() {
		return bidOrders;
	}

	public void setBidOrders(List<OrderRequest> bidOrders) {
		this.bidOrders = bidOrders;
	}

	public List<OrderRequest> getAskOrders() {
		return askOrders;
	}

	public void setAskOrders(List<OrderRequest> askOrders) {
		this.askOrders = askOrders;
	}
}
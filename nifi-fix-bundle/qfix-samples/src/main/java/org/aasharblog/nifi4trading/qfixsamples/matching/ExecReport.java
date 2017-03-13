package org.aasharblog.nifi4trading.qfixsamples.matching;

import quickfix.field.ExecTransType;
import quickfix.field.ExecType;
import quickfix.field.OrdStatus;
import quickfix.field.Side;
import java.text.DecimalFormat;

public class ExecReport {
    private long entryTime;
    private String orderId;
    private String execId;
    private String clientOrderId;
    private String symbol;

	private String owner;
    private String target;
    private char side;
    private long openQuantity;
    private long executedQuantity;
    private double avgExecutedPrice;
    private double lastExecutedPrice;
    private long lastExecutedQuantity;
    private long orderQuantity;

    private char execTransType;
    private char execType;
    private char ordStatus;
    
    private String text;

	public ExecReport(OrderRequest order, char status, String text, String execId) {
	    this.setEntryTime(System.currentTimeMillis());
	    this.setOrderId(order.getClientOrderId());
	    this.setExecId(execId);
	    this.execTransType = ExecTransType.NEW;
	    this.execType = status;
	    this.ordStatus = status;
	    this.symbol = order.getSymbol();
	    this.side = order.getSide();
	    this.openQuantity = order.getOpenQuantity();
	    this.executedQuantity = order.getExecutedQuantity();
	    this.avgExecutedPrice = order.getAvgExecutedPrice();

	    this.setClientOrderId(order.getClientOrderId());
	    this.orderQuantity = order.getQuantity();

	    this.owner = order.getTarget();  // Flip sender and target
	    this.target = order.getOwner();
	    
	    this.text = text;

	    if (status == OrdStatus.FILLED || status == OrdStatus.PARTIALLY_FILLED) {
		    this.lastExecutedPrice = order.getLastExecutedPrice();
		    this.lastExecutedQuantity = order.getLastExecutedQuantity();
	    }
	}

	public long getEntryTime() { return entryTime; }
	public void setEntryTime(long entryTime) { this.entryTime = entryTime; }

	public String getOrderId() { return orderId; }
	public void setOrderId(String orderId) { this.orderId = orderId; }

	public String getExecId() { return execId; }
	public void setExecId(String execId) { this.execId = execId; }

	public String getClientOrderId() { return clientOrderId; }
	public void setClientOrderId(String clientOrderId) { this.clientOrderId = clientOrderId; }

	public String getSymbol() { return symbol; }
	public void setSymbol(String symbol) { this.symbol = symbol; }

	public String getOwner() { return owner; }
	public void setOwner(String owner) { this.owner = owner; }

	public String getTarget() { return target; }
	public void setTarget(String target) { this.target = target; }

	public char getSide() { return side; }
	public void setSide(char side) { this.side = side; }

	public long getOpenQuantity() { return openQuantity; }
	public void setOpenQuantity(long openQuantity) { this.openQuantity = openQuantity; }

	public long getExecutedQuantity() { return executedQuantity; }
	public void setExecutedQuantity(long executedQuantity) { this.executedQuantity = executedQuantity; }

	public double getAvgExecutedPrice() { return avgExecutedPrice; }
	public void setAvgExecutedPrice(double avgExecutedPrice) { this.avgExecutedPrice = avgExecutedPrice; }

	public double getLastExecutedPrice() { return lastExecutedPrice; }
	public void setLastExecutedPrice(double lastExecutedPrice) { this.lastExecutedPrice = lastExecutedPrice; }

	public long getLastExecutedQuantity() { return lastExecutedQuantity; }
	public void setLastExecutedQuantity(long lastExecutedQuantity) { this.lastExecutedQuantity = lastExecutedQuantity; }

	public long getOrderQuantity() { return orderQuantity; }
	public void setOrderQuantity(long orderQuantity) { this.orderQuantity = orderQuantity; }

	public char getExecTransType() { return execTransType; }
	public void setExecTransType(char execTransType) { this.execTransType = execTransType; }

	public char getExecType() { return execType; }
	public void setExecType(char execType) { this.execType = execType; }

	public char getOrdStatus() { return ordStatus; }
	public void setOrdStatus(char ordStatus) { this.ordStatus = ordStatus; }

	public String getText() { return text; }
	public void setText(String text) { this.text = text; }

	public String toString() {
		DecimalFormat priceFormat = new DecimalFormat("#.00");
		DecimalFormat qtyFormat = new DecimalFormat("######");

		StringBuffer retBuf = new StringBuffer();
		if (this.execType == ExecType.NEW) {
			retBuf.append("Accepted " + clientOrderId + ", " + (side == Side.BUY ? "BUY " : "SELL ") + qtyFormat.format(orderQuantity) + " " + symbol);
		}
		else if (execType == ExecType.PARTIAL_FILL) {
			retBuf.append("Filled " + clientOrderId + ", " + (side == Side.BUY ? "BUY " : "SELL ") + qtyFormat.format(lastExecutedQuantity) +
					" of " + qtyFormat.format(orderQuantity) + " " + symbol + " @$" + priceFormat.format(lastExecutedPrice));
		}
		else if (execType == ExecType.FILL) {
			retBuf.append("Filled " + clientOrderId + ", " + (side == Side.BUY ? "BUY " : "SELL ") + qtyFormat.format(lastExecutedQuantity) + " " +
					symbol + " @$" + priceFormat.format(lastExecutedPrice));
		}
		else if (execType == ExecType.CANCELED) {
			retBuf.append("Canceled " + clientOrderId + ", " + (side == Side.BUY ? "BUY " : "SELL ") + symbol);
		}
		else {
			retBuf.append("ExecType:" + execType +  ", " + clientOrderId + "\", " + (side == Side.BUY ? ", BUY " : ", SELL ") + ", Last Quantity:" +
					qtyFormat.format(lastExecutedQuantity) + " of " + qtyFormat.format(orderQuantity) + " " +
					symbol + " @$" + priceFormat.format(lastExecutedPrice));
		}

		retBuf.append(" from " + owner);

		return retBuf.toString();
	}
}

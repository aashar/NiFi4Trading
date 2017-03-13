package org.aasharblog.nifi4trading.qfixsamples.matching;

import java.text.DecimalFormat;

import com.fasterxml.jackson.annotation.JsonProperty;

import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.Side;

public class OrderRequest {
    private String msgType;
    private long entryTime;
    private String clientOrderId;
    private String OrigClOrderId;
    private String symbol;
    private String owner;
    private String target;
    private char side;
    private char type;
    private double price;
    private long quantity;
    private long openQuantity;
    private long executedQuantity;
    private double avgExecutedPrice;
    private double lastExecutedPrice;
    private long lastExecutedQuantity;
    
    public OrderRequest() {
        setEntryTime(System.currentTimeMillis());
    }

    public OrderRequest(String clientId, String symbol, String owner, String target, char side, char type,
            double price, long quantity) {
        this();
        this.clientOrderId = clientId;
        this.symbol = symbol;
        this.owner = owner;
        this.target = target;
        this.side = side;
        this.type = type;
        this.price = price;
        this.quantity = quantity;
        setOpenQuantity(quantity);
    }

    public void validateNewOrder() throws Exception {
    	if (getOwner().isEmpty()) throw new Exception("Missing order owner");
    	if (getTarget().isEmpty()) throw new Exception("Missing order target");
    	if (getClientOrderId().isEmpty()) throw new Exception("Missing order id");
    	if (getSymbol().isEmpty()) throw new Exception("Missing symbol");
    	if (getSide() != '1' && getSide() != '2') throw new Exception("Invalid side");
    	if (getType() == OrdType.LIMIT && getPrice() == 0) throw new Exception("Missing Price on a limit order");
    	if (getQuantity() == 0) throw new Exception("Missing quantity");
    }

    public void validateCancelOrder() throws Exception {
		if (getClientOrderId().isEmpty()) { throw new Exception("Missing client order id"); }
		if (getSymbol().isEmpty()) { throw new Exception("Missing symbol"); }
    }

    public void execute(double price, long quantity) {
        avgExecutedPrice = ((quantity * price) + (avgExecutedPrice * executedQuantity))
                / (quantity + executedQuantity);

        setOpenQuantity(getOpenQuantity() - quantity);
        executedQuantity += quantity;
        lastExecutedPrice = price;
        lastExecutedQuantity = quantity;
    }

    public ExecReport cancel(String execId) {
    	setOpenQuantity(0);
    	return updateOrder(OrdStatus.CANCELED, "", execId);
	}

    public ExecReport updateOrder(char status, String text, String execId) {
        return new ExecReport(this, status, text, execId);
    }

    public String toString() {
		DecimalFormat priceFormat = new DecimalFormat("#.00");
		DecimalFormat qtyFormat = new DecimalFormat("######");
		return (side == Side.BUY ? "BUY" : "SELL")+" "+quantity+ " " + symbol +
				(price != 0 ? " @$"+priceFormat.format(price) : "") +
				" ("+qtyFormat.format(getOpenQuantity())+") from "+owner;
    }

    @JsonProperty("MsgType")
	public String getMsgType() { return msgType; }
	public void setMsgType(String msgType) { this.msgType = msgType; }

	@JsonProperty("AvgPx")
    public double getAvgExecutedPrice() { return avgExecutedPrice; }

	@JsonProperty("OrigClOrdID")
	public String getOrigClOrderId() { return OrigClOrderId; }
	public void setOrigClOrderId(String origClOrderId) { OrigClOrderId = origClOrderId; }

	@JsonProperty("ClOrdID")
    public String getClientOrderId() { return clientOrderId; }

    @JsonProperty("CumQty")
    public long getExecutedQuantity() { return executedQuantity; }

    @JsonProperty("LastShares")
    public long getLastExecutedQuantity() { return lastExecutedQuantity; }

    @JsonProperty("LeavesQty")
    public long getOpenQuantity() { return openQuantity; }
	public void setOpenQuantity(long openQuantity) { this.openQuantity = openQuantity; }

    @JsonProperty("SenderCompID")
    public String getOwner() { return owner; }

    @JsonProperty("Price")
    public double getPrice() { return price; }

    @JsonProperty("OrderQty")
    public long getQuantity() { return quantity; }

    @JsonProperty("Side")
    public char getSide() { return side; }

    @JsonProperty("Symbol")
    public String getSymbol() { return symbol; }

    @JsonProperty("TargetCompID")
    public String getTarget() { return target; }

    @JsonProperty("OrdType")
    public char getType() { return type; }
    
    @JsonProperty("TransactTime")  // TODO: Not exactly the same as entry time
    public long getEntryTime() { return entryTime; }
	public void setEntryTime(long entryTime) { this.entryTime = entryTime; }
    
    @JsonProperty("LastPx")
    public double getLastExecutedPrice() { return lastExecutedPrice; }

    public boolean isFilled() { return quantity == executedQuantity; }

    public boolean isClosed() { return getOpenQuantity() == 0; }
}
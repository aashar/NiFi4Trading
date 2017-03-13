package org.aasharblog.nifi4trading.qfixsamples.matching;

public class IdGenerator {
    private int orderIdCounter = 0;
    private int executionIdCounter = 0;
    
    public String genExecutionID() {
        return Integer.toString(executionIdCounter++);
    }

    public String genOrderID() {
        return Integer.toString(orderIdCounter++);
    }
}

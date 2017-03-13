package org.aasharblog.nifi4trading.services.matching;

import java.util.ArrayList;

import org.aasharblog.nifi4trading.qfixsamples.matching.ExecReport;
import org.aasharblog.nifi4trading.qfixsamples.matching.OrderRequest;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

@Tags({"FIX Protocol"})
@CapabilityDescription("Matching service specifications.")
public interface MatchingService extends ControllerService {
    public ArrayList<ExecReport> acceptOrder(OrderRequest order, boolean performMatch) throws Exception ;
    public ArrayList<ExecReport> matchOpenOrders() throws Exception;
    public String getOrderBook() throws Exception;
}

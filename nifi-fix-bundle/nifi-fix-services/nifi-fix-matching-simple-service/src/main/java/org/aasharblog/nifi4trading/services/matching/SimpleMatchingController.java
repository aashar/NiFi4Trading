package org.aasharblog.nifi4trading.services.matching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.aasharblog.nifi4trading.qfixsamples.matching.ExecReport;
import org.aasharblog.nifi4trading.qfixsamples.matching.OrderMatcher;
import org.aasharblog.nifi4trading.qfixsamples.matching.OrderRequest;
import org.aasharblog.nifi4trading.qfixsamples.matching.Serializers;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

@Tags({ "simple matching"})
@CapabilityDescription("Simple matching service based on quickfix examples.")
public class SimpleMatchingController extends AbstractControllerService implements MatchingService {
    OrderMatcher orderMatcher = new OrderMatcher();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        properties = Collections.unmodifiableList(props);
    }

    @Override
    public ArrayList<ExecReport> acceptOrder(OrderRequest order, boolean performMatch) throws Exception {
		return orderMatcher.processOrder(order, performMatch);
    }

    public ArrayList<ExecReport> matchOpenOrders() throws Exception  {
    	return orderMatcher.match();
    }

    public String getOrderBook() throws Exception  {
		return Serializers.Json.getOrderBook(orderMatcher);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
    }

    @OnDisabled
    public void shutdown() {

    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}

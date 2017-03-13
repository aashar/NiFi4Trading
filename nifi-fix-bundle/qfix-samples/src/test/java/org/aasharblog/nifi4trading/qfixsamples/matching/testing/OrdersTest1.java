package org.aasharblog.nifi4trading.qfixsamples.matching.testing;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.aasharblog.nifi4trading.qfixsamples.matching.ExecReport;
import org.aasharblog.nifi4trading.qfixsamples.matching.OrderMatcher;
import org.aasharblog.nifi4trading.qfixsamples.matching.OrderRequest;
import org.aasharblog.nifi4trading.qfixsamples.matching.Serializers;
import org.junit.Assert;
import org.junit.Test;

public class OrdersTest1 {
    OrderMatcher orderMatcher = new OrderMatcher();
	
	@Test
	public void test1JsonOrdersTest() {
		String[] orders = {
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"1\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882577\",\"Side\":\"1\"}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"ORCL\",\"OrdType\":\"1\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157895032\",\"Side\":\"1\"}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"SPY\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157912357\",\"Side\":\"2\",\"Price\":10}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"IBM\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157912351\",\"Side\":\"1\",\"Price\":10}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"F\",\"OrderQty\":10000,\"Symbol\":\"IBM\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157912352\",\"OrigClOrdID\":\"1352157912351\",\"Side\":\"1\"}",
				"{\"SenderCompID\":\"BANZAI2\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"1\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882572\",\"Side\":\"2\"}",
				"{\"SenderCompID\":\"BANZAI2\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"ORCL\",\"OrdType\":\"1\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157895033\",\"Side\":\"2\"}",
				"{\"SenderCompID\":\"BANZAI2\",\"MsgType\":\"D\",\"OrderQty\":5000,\"Symbol\":\"SPY\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157912354\",\"Side\":\"1\",\"Price\":10}",
				"{\"SenderCompID\":\"BANZAI2\",\"MsgType\":\"D\",\"OrderQty\":5000,\"Symbol\":\"SPY\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157912355\",\"Side\":\"1\",\"Price\":10}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":5000,\"Symbol\":\"SPY\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157912355\",\"Side\":\"1\",\"Price\":10}",
				"{\"SenderCompID\":\"BANZAI2\",\"MsgType\":\"D\",\"OrderQty\":5000,\"Symbol\":\"SPY\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157912355\",\"Side\":\"2\",\"Price\":11}"
			};

		for (String orderJSON : orders) {
			try {
				System.out.println("Order: " + orderJSON);

				OrderRequest order = Serializers.Json.toOrderRequest(orderJSON);
				if (order == null) { throw new Exception("Invalid order string"); }

				ArrayList<ExecReport> ers = orderMatcher.processOrder(order, false);
				Assert.assertEquals(ers.size(), 1);

				for (ExecReport er : ers)
					System.out.println("Exec: " + Serializers.Json.serialize(er));

				System.out.println(Serializers.Json.getOrderBook(orderMatcher));
			} catch (Exception e) {
				System.out.println("Failed to process message:" + orderJSON + "\n" + e.getMessage() + "\n" + e.getStackTrace());
				fail("Failed to process message");
			}
		}

		try {
			ArrayList<ExecReport> ers = orderMatcher.match();

			Assert.assertEquals(ers.size(), 7);

			for (ExecReport er : ers)
				System.out.println("Exec: " + Serializers.Json.serialize(er));

			String md = Serializers.Json.getOrderBook(orderMatcher);
//			Assert.assertEquals(md, "{\"markets\":[{\"market\":{\"MSFT\":{\"bids\":[],\"asks\":[]}}},{\"market\":{\"IBM\":{\"bids\":[],\"asks\":[]}}},{\"market\":{\"ORCL\":{\"bids\":[],\"asks\":[]}}},{\"market\":{\"SPY\":{\"bids\":[],\"asks\":[]}}}]");
			System.out.println(md);
		} catch (Exception e) {
			System.out.println("Failed to match:" + e.getMessage() + "\n" + e.getStackTrace());
			fail("Failed to match");
		}
	}
}

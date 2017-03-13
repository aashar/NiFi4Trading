package org.aasharblog.nifi4trading.qfixsamples.matching.testing;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.aasharblog.nifi4trading.qfixsamples.matching.ExecReport;
import org.aasharblog.nifi4trading.qfixsamples.matching.OrderMatcher;
import org.aasharblog.nifi4trading.qfixsamples.matching.OrderRequest;
import org.aasharblog.nifi4trading.qfixsamples.matching.Serializers;
import org.junit.Assert;
import org.junit.Test;

public class OrdersTest2 {
    OrderMatcher orderMatcher = new OrderMatcher();
	
	@Test
	public void test1JsonOrdersTest() {
		String[] orders = {
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882577\",\"Side\":\"2\",\"Price\":100}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882577\",\"Side\":\"2\",\"Price\":102}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882577\",\"Side\":\"2\",\"Price\":98}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882577\",\"Side\":\"2\",\"Price\":101}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882577\",\"Side\":\"1\",\"Price\":99}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882577\",\"Side\":\"1\",\"Price\":99}",
				"{\"SenderCompID\":\"BANZAI1\",\"MsgType\":\"D\",\"OrderQty\":10000,\"Symbol\":\"MSFT\",\"OrdType\":\"2\",\"TargetCompID\":\"EXEC\",\"ClOrdID\":\"1352157882577\",\"Side\":\"1\",\"Price\":100}"
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

			for (ExecReport er : ers)
				System.out.println("Exec: " + Serializers.Json.serialize(er));

			String md = Serializers.Json.getOrderBook(orderMatcher);
			System.out.println(md);
		} catch (Exception e) {
			System.out.println("Failed to match:" + e.getMessage() + "\n" + e.getStackTrace());
			fail("Failed to match");
		}
	}
}

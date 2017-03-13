package org.aasharblog.nifi4trading.qfixsamples.matching;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

public class Serializers {
	static ObjectMapper vanillaMapper = new ObjectMapper();
	// JSON serialization

	public static class Json {
	    public static OrderRequest toOrderRequest(String jsonString)
	    		throws JsonParseException, JsonMappingException, IOException {
	    	ObjectMapper mapper = new ObjectMapper()
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	
	    	mapper.setSerializationInclusion(Include.NON_NULL);
	
	    	String[] ignorableFieldNames = {"_id", "SendingTime"};  
	
	        FilterProvider filters = new SimpleFilterProvider()  
	    		.addFilter("filterAClass",SimpleBeanPropertyFilter.serializeAllExcept(ignorableFieldNames));
	        mapper.setFilterProvider(filters);
	        
	        OrderRequest order = mapper.readValue(jsonString, OrderRequest.class);
	        order.setOpenQuantity(order.getQuantity());
	        
	        return order;
	    }
	
	    public static String serialize(OrderRequest order) throws JsonProcessingException {
			return vanillaMapper.writeValueAsString(order);
	    }
	
	    public static String serialize(ExecReport er) throws JsonProcessingException {
//			ObjectMapper mapper = new ObjectMapper();
			return vanillaMapper.writeValueAsString(er);
		}
	
	    public static String getOrderBook(OrderMatcher matcher) throws JsonProcessingException {
	    	if (matcher.getMarkets().size() == 0) {
	    		return "{\"markets\":[]}";
	    	}
	
	    	StringBuffer buf = new StringBuffer();
	        buf.append("{\"markets\":[");
	        for (Iterator<String> iter = matcher.getMarkets().keySet().iterator(); iter.hasNext();) {
	            String symbol = iter.next();
	            buf.append("{\"symbol\":\"" + symbol + "\",\"market\":");
	            buf.append(serialize(matcher.getMarket(symbol)));
	            buf.append("},");
	        }
	        buf.deleteCharAt(buf.length()-1);
	        buf.append("]}");
	        return buf.toString();
	    }

	    private static String serialize(Market market) throws JsonProcessingException {
	        return "{" + getPosition(market.getBidOrders(), "\"bids\":") + "," +
	        			 getPosition(market.getAskOrders(), "\"asks\":") + "}";
	    }

	    private static String getPosition(List<OrderRequest> orders, String title) throws JsonProcessingException {
//			return new ObjectMapper().writerWithView(Views.MarketData.class).writeValueAsString(orders);

	        DecimalFormat qtyFormat = new DecimalFormat("######");
	        SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");

	        if (orders.size() == 0) {
	    		return title + "[]";
	    	}
	
	        StringBuffer buf = new StringBuffer();
	
	        buf.append(title + "[");
	        for (int i = 0; i < orders.size(); i++) {
	        	OrderRequest order = orders.get(i);
//				buf.append(vanillaMapper.writerWithView(Views.MarketData.class).writeValueAsString(order) + ",");
//	            buf.append(orders.get(i).toJSON() + ",");

	            buf.append("{\"owner\":\"" + order.getOwner() + "\", \"price\":" +
	            		Double.toString(order.getPrice()) + ",\"quantity\":" +
	                    qtyFormat.format(order.getOpenQuantity())+ ",\"time\":\"" +
	                    dtFormat.format(new Date(order.getEntryTime())) + "\"},");
	        }
	        buf.deleteCharAt(buf.length()-1);
	        buf.append("]");
	        
	        return buf.toString();
	    }
	}
}

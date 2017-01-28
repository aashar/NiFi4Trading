package org.aasharblog.nifi4trading;

import org.aasharblog.nifi4trading.fix_utils.OrderFiller;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import quickfix.ConfigError;
import quickfix.Message;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"FIX Protocol", "Execute", "Orders"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Generate ExecRep messages for orders, developed for testing flows and not for the real use")
public class ProcessOrder extends AbstractProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    public static final PropertyDescriptor DICTIONARY_URI = new PropertyDescriptor.Builder()
            .name("Dictionary URI")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    
    public static final PropertyDescriptor MARKET_ORDERS_FILL_PRICE = new PropertyDescriptor.Builder()
            .name("Market Orders fill price")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    
    public static final PropertyDescriptor VALID_ORDTYPES = new PropertyDescriptor.Builder()
            .name("Comma separated list of valid OrdType")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Order filled")
            .build();
    
    public static final Relationship INVALID_MESSAGE = new Relationship.Builder()
            .name("Invalid Message")
            .description("Not a valid Message")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Order filled")
            .build();
    
    OrderFiller execute = null;

    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DICTIONARY_URI); properties.add(MARKET_ORDERS_FILL_PRICE); properties.add(VALID_ORDTYPES);
        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS); relationships.add(INVALID_MESSAGE); relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ConfigError {
		execute = new OrderFiller(
				context.getProperty(DICTIONARY_URI).getValue(),
				new Double(context.getProperty(MARKET_ORDERS_FILL_PRICE).getValue()).doubleValue(),
				context.getProperty(VALID_ORDTYPES).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        
    	if (flowFile == null) {
            return;
        }
    	
        final ComponentLog log = this.getLogger();
        final AtomicReference<String> fixMsg = new AtomicReference<>();

    	session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                	BufferedInputStream bis = new BufferedInputStream(in);
                	ByteArrayOutputStream buf = new ByteArrayOutputStream();
                	int result = bis.read();
                	while(result != -1) {
                	    buf.write((byte) result);
                	    result = bis.read();
                	}
                	fixMsg.set(buf.toString("US-ASCII"));  // US-ASCII, per the fix spec
                } catch(Exception ex){
                	log.error("Failed to extract message from flowfile", ex);
                	session.transfer(flowFile, FAILURE);
                	return;
                }
            }
        });

    	String fixMsgStr = fixMsg.get();
        if (fixMsgStr.length() == 0) {
        	//log.warn("Empty flowfile");
	    	//session.transfer(flowFile, FAILURE);
	    	return;
        }
        
        Message order;
        
        try {
        	order = execute.parseFIXString(fixMsgStr);
        } catch (Exception ex) {
            session.transfer(flowFile, INVALID_MESSAGE);
            return;
        }

        List<Message> msgs;

        try{
        	msgs = execute.processMessage(order);
        } catch(Exception ex){
        	log.error("Failed to execute order", ex);
        	session.transfer(flowFile, FAILURE);
        	return;
        }

    	if (msgs.size() == 0) {
        	log.warn("No executions");
            session.transfer(flowFile, FAILURE);
            return;
    	}

    	List<FlowFile> outFlowFiles = new ArrayList<FlowFile>();

        try{
            for (final Message msg : msgs) {
                FlowFile anFF = session.create(flowFile);
                anFF = session.write(anFF, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(msg.toString().getBytes());
                    }
                });
            	outFlowFiles.add(anFF);
            }
            
            if (!outFlowFiles.isEmpty()) {
                session.transfer(outFlowFiles, SUCCESS);
                session.remove(flowFile);
                return;
            }
        } catch(Exception ex){
        	log.error("Failed to create flowfiles", ex);
        	session.transfer(flowFile, FAILURE);
        	return;
        }
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        return results;
    }
  
    @org.junit.Test
    public void onTriggerTestCase() {

    	TestRunner runner = TestRunners.newTestRunner(new ProcessOrder());

    	runner.setProperty(ProcessOrder.DICTIONARY_URI, "C:\\tools\\nifi4trading\\executorA\\conf\\FIX44.XML");
    	runner.setProperty(ProcessOrder.MARKET_ORDERS_FILL_PRICE, "100");
    	runner.setProperty(ProcessOrder.VALID_ORDTYPES, "1,2");

    	InputStream content = new ByteArrayInputStream("8=FIX.4.49=10335=D34=349=BANZAI52=20121105-23:24:4256=EXEC11=135215788257721=138=1000040=154=155=MSFT59=010=065".getBytes());
    	runner.enqueue(content);

    	runner.run(1);

    	runner.assertQueueEmpty();

    	List<MockFlowFile> results = runner.getFlowFilesForRelationship(ProcessOrder.SUCCESS);

    	assertTrue("Ack+Fill received", results.size() == 2);
    	results.get(0).assertContentEquals("8=FIX.4.49=9335=849=EXEC56=BANZAI6=011=135215788257714=017=137=139=054=155=MSFT150=2151=1000010=228");
		results.get(1).assertContentEquals("8=FIX.4.49=12035=849=EXEC56=BANZAI6=10011=135215788257714=1000017=231=10032=1000037=238=1000039=254=155=MSFT150=2151=010=209");

		for (MockFlowFile ff : results) {
			String resultValue = new String(runner.getContentAsByteArray(ff));
			System.out.println(resultValue);
		}
    }
}

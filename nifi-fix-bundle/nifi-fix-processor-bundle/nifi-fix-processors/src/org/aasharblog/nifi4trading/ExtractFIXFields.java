package org.aasharblog.nifi4trading;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.FieldNotFound;
import quickfix.InvalidMessage;
import quickfix.Message;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"FIX Protocol", "Extract"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso(ConvertFIXtoXML.class)
@CapabilityDescription("Extract FIX fields as attributes. The new attributes will be named as per the field name defined in the data dictionary")

public class ExtractFIXFields extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    public static final PropertyDescriptor DICTIONARY_URI = new PropertyDescriptor.Builder()
            .name("Dictionary URI")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    
    public static final PropertyDescriptor INCLUDE_MISSING_FIELD = new PropertyDescriptor.Builder()
            .name("Include Missing Fields?")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    
    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("List of Fields for extraction")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Successful FIX to XML conversion")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("FIX to XML conversion failed")
            .build();
    
    DataDictionary dd;
    Boolean includeMissingFields = false;
    Map<Integer, String> fieldMap = new HashMap<Integer, String>();

    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DICTIONARY_URI); properties.add(INCLUDE_MISSING_FIELD); properties.add(FIELDS);
        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS); relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ConfigError {
		dd = new DataDictionary(context.getProperty(DICTIONARY_URI).getValue());
		includeMissingFields = context.getProperty(INCLUDE_MISSING_FIELD).asBoolean();
	    List<String> fields = Arrays.asList(context.getProperty(FIELDS).getValue().trim().split("\\s*,\\s*"));
	    for (String field : fields) {
	    	int intField = Integer.parseInt(field);
            if (dd.isField(intField)) {
            	fieldMap.put(intField, dd.getFieldName(intField));
            }
	    }
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
                }catch(Exception ex){
                	log.error("Failed to extract message from flowfile", ex);
                	session.transfer(flowFile, FAILURE);
                	return;
                }
            }
        });

        if (fixMsg.get().length() == 0) {
        	log.warn("Empty flowfile");
	    	session.transfer(flowFile, FAILURE);
	    	return;
        }

        Message fixMessage = new Message();

        try{
            fixMessage.fromString(fixMsg.get(), dd, false);
        }catch(InvalidMessage ex){
        	log.error("Failed to construct FIX message object", ex);
        	session.transfer(flowFile, FAILURE);
        	return;
        }

        FlowFile returnflowfile = flowFile;

        for (final Entry<Integer, String> field : fieldMap.entrySet()) {
        	String value;

            if (dd.isHeaderField(field.getKey())) {
            	try {
                	value = fixMessage.getHeader().getString(field.getKey());
                	returnflowfile = session.putAttribute(returnflowfile, field.getValue(), value);
            	} catch (FieldNotFound e) {
					if (context.getProperty(INCLUDE_MISSING_FIELD).asBoolean()) {
	                	returnflowfile = session.putAttribute(returnflowfile, field.getValue(), "");
					}
            	}
            }
            else if (dd.isTrailerField(field.getKey())) {
            	try {
                	value = fixMessage.getTrailer().getString(field.getKey());
                	returnflowfile = session.putAttribute(returnflowfile, field.getValue(), value);
            	} catch (FieldNotFound exc) {
					if (context.getProperty(INCLUDE_MISSING_FIELD).asBoolean()) {
	                	returnflowfile = session.putAttribute(returnflowfile, field.getValue(), "");
					}
				}
        	}
            else {
				try {
                	value = fixMessage.getString(field.getKey());
                	returnflowfile = session.putAttribute(returnflowfile, field.getValue(), value);
				} catch (FieldNotFound ex) {
					if (context.getProperty(INCLUDE_MISSING_FIELD).asBoolean()) {
	                	returnflowfile = session.putAttribute(returnflowfile, field.getValue(), "");
					}
            	}
            }
        }

        session.transfer(returnflowfile, SUCCESS);                                                                                                                                                                                                                                                                                                                                                                                                 
    }
    
    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    @org.junit.Test
    public void onTriggerTestCase() {
    	TestRunner runner = TestRunners.newTestRunner(new ExtractFIXFields());

    	runner.setProperty(ExtractFIXFields.DICTIONARY_URI, "C:\\tools\\nifi4trading\\executorA\\conf\\FIX44.XML");
    	runner.setProperty(ExtractFIXFields.INCLUDE_MISSING_FIELD, "FALSE");
    	runner.setProperty(ExtractFIXFields.FIELDS, "21,55");

    	InputStream content = new ByteArrayInputStream("8=FIX.4.49=10335=D34=349=BANZAI52=20121105-23:24:4256=EXEC11=135215788257721=138=1000040=154=155=MSFT59=010=065".getBytes());
    	runner.enqueue(content);

    	runner.run(1);

    	runner.assertQueueEmpty();

    	List<MockFlowFile> results = runner.getFlowFilesForRelationship(ExtractFIXFields.SUCCESS);

    	assertTrue("Fields extracted", results.size() == 1);
    	results.get(0).assertAttributeExists("HandlInst");
    	results.get(0).assertAttributeExists("Symbol");
    	results.get(0).assertContentEquals("8=FIX.4.49=10335=D34=349=BANZAI52=20121105-23:24:4256=EXEC11=135215788257721=138=1000040=154=155=MSFT59=010=065");

		for (Entry<String,String> attr : results.get(0).getAttributes().entrySet()) {
			String resultValue = new String(attr.toString());
			System.out.println(resultValue);
		}
    }
}

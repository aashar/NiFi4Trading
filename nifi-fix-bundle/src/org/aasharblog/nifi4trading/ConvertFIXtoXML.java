package org.aasharblog.nifi4trading;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.InvalidMessage;
import quickfix.Message;

@EventDriven
@SideEffectFree
@Tags({"FIX Protocol", "XML"})
@CapabilityDescription("Convert FIX message to XML")
@InputRequirement(Requirement.INPUT_REQUIRED)
//@SeeAlso(ExtractFIXFields.class)

public class ConvertFIXtoXML extends AbstractProcessor {
   
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    public static final PropertyDescriptor DICTIONARY_URI = new PropertyDescriptor.Builder()
            .name("Dictionary URI")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
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
    
    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DICTIONARY_URI);
        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS); relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ConfigError {
		dd = new DataDictionary(context.getProperty(DICTIONARY_URI).getValue());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowfile = session.get();
        
    	if (flowfile == null) {
            return;
        }
    	
        final ComponentLog log = this.getLogger();
        final AtomicReference<String> fixMsg = new AtomicReference<>();

    	session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
//					fixMsg.set(IOUtils.toString(in, StandardCharsets.US_ASCII));
// faster options? - http://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string

                	BufferedInputStream bis = new BufferedInputStream(in);
                	ByteArrayOutputStream buf = new ByteArrayOutputStream();
                	int result = bis.read();
                	while(result != -1) {
                	    buf.write((byte) result);
                	    result = bis.read();
                	}
                	fixMsg.set(buf.toString("US-ASCII"));  // US-ASCII, per the fix spec
                }catch(Exception ex){
                	log.error("Failed to convert FIX message", ex);
                }
            }
        });

        if (fixMsg.get().length() == 0) {
	    	session.transfer(flowfile, FAILURE);
	    	return;
        }

        Message fixMessage = new Message();

        try{
            fixMessage.fromString(fixMsg.get(), dd, false);
        }catch(InvalidMessage ex){
        	log.error("Failed to construct FIX message object", ex);
        	session.transfer(flowfile, FAILURE);
        	return;
        }

        final AtomicReference<String> fixXml = new AtomicReference<>();
    	fixXml.set(fixMessage.toXML()); // Do not use dictionary to use field numbers instead of names

        flowfile = session.write(flowfile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(fixXml.get().getBytes());
            }
        });
        
        session.transfer(flowfile, SUCCESS);                                                                                                                                                                                                                                                                                                                                                                                                 
    }
    
    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }
}
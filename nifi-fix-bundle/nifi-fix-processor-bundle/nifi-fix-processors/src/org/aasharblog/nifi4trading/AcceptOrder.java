package org.aasharblog.nifi4trading;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

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

import org.aasharblog.nifi4trading.qfixsamples.matching.ExecReport;
import org.aasharblog.nifi4trading.qfixsamples.matching.OrderRequest;
import org.aasharblog.nifi4trading.qfixsamples.matching.Serializers;
import org.aasharblog.nifi4trading.services.matching.MatchingService;

@Tags({"FIX Protocol", "Trading", "Accept Order"})
@CapabilityDescription("Accept order for matching")
@SeeAlso({MatchOrders.class})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class AcceptOrder extends AbstractProcessor {
    public static final PropertyDescriptor MATCHING_SERVICE = new PropertyDescriptor
            .Builder().name("Matching Service")
            .description("Example Controller Service Property")
            .required(true)
            .identifiesControllerService(MatchingService.class)
            .build();

    public static final PropertyDescriptor PERFORM_MATCH = new PropertyDescriptor
            .Builder().name("Perform Matching?")
            .description("Example Controller Service Property")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Response")
            .description("Order accept response")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failed")
            .description("Order accept failed")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MATCHING_SERVICE);
        descriptors.add(PERFORM_MATCH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	MatchingService service = context.getProperty(MATCHING_SERVICE).asControllerService(MatchingService.class);
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
        
		OrderRequest order;
		try {
			order = Serializers.Json.toOrderRequest(fixMsgStr);
		} catch (IOException e) {
        	log.error("Invalid JSON message", e);
            session.transfer(flowFile, FAILURE);
            return;
		}
		if (order == null) {
        	log.error("Invalid JSON message");
            session.transfer(flowFile, FAILURE);
            return;
		}

		ArrayList<ExecReport> ers;
		try {
			boolean performMatch = context.getProperty(PERFORM_MATCH).asBoolean();
			ers = service.acceptOrder(order, performMatch);
		} catch (Exception e) {
        	log.error("Unable to accept message for matching", e);
            session.transfer(flowFile, FAILURE);
            return;
		}

		if (ers.size() == 0) {
        	log.warn("No executions");
            session.transfer(flowFile, FAILURE);
            return;
    	}

    	List<FlowFile> outFlowFiles = new ArrayList<FlowFile>();

        try{
            for (final ExecReport er : ers) {
                FlowFile anFF = session.create(flowFile);
                anFF = session.write(anFF, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(Serializers.Json.serialize(er).getBytes());
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
}

package org.aasharblog.nifi4trading;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

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
import org.apache.nifi.processor.io.OutputStreamCallback;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aasharblog.nifi4trading.services.matching.MatchingService;

@Tags({"FIX Protocol", "Trading", "Market Data"})
@CapabilityDescription("Get Market Data")
@SeeAlso({AcceptOrder.class})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class GetOrderBook extends AbstractProcessor {

    public static final PropertyDescriptor MATCHING_SERVICE = new PropertyDescriptor
            .Builder().name("Matching Service")
            .description("Example Controller Service Property")
            .required(true)
            .identifiesControllerService(MatchingService.class)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Response")
            .description("Market Data Output")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failed")
            .description("Get market data failed")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MATCHING_SERVICE);
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
        final FlowFile flowFile = session.get();
    	if (flowFile == null) { return; }

    	final ComponentLog log = this.getLogger();

		String md;
		try {
			md = context.getProperty(MATCHING_SERVICE)
					.asControllerService(MatchingService.class)
					.getOrderBook();
		} catch (Exception e) {
        	log.error("Unable to accept message for matching", e);
            session.transfer(flowFile, FAILURE);
            return;
		}

        FlowFile anFF = session.create(flowFile);
        anFF = session.write(anFF, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(md.getBytes());
            }
        });

        session.transfer(anFF, SUCCESS);
        session.remove(flowFile);
        return;
	}
}

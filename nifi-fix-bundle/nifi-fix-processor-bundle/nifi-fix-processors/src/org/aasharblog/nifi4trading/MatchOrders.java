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
import org.apache.nifi.processor.io.OutputStreamCallback;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.aasharblog.nifi4trading.qfixsamples.matching.ExecReport;
import org.aasharblog.nifi4trading.qfixsamples.matching.Serializers;
import org.aasharblog.nifi4trading.services.matching.MatchingService;

@Tags({"FIX Protocol", "Trading", "Match Orders"})
@CapabilityDescription("Match open orders")
@SeeAlso({AcceptOrder.class})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class MatchOrders extends AbstractProcessor {
    public static final PropertyDescriptor MATCHING_SERVICE = new PropertyDescriptor
            .Builder().name("Matching Service")
            .description("Example Controller Service Property")
            .required(true)
            .identifiesControllerService(MatchingService.class)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Response")
            .description("Matched orders")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failed")
            .description("Match process failed")
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
        final ComponentLog log = this.getLogger();
    	MatchingService service = context.getProperty(MATCHING_SERVICE).asControllerService(MatchingService.class);

		ArrayList<ExecReport> ers;
		try {
			ers = service.matchOpenOrders();
		} catch (Exception e) {
        	log.error("Unable to accept message for matching", e);
            return;
		}

		if (ers.size() == 0) {
//        	log.debug("No executions");
            return;
    	}

    	List<FlowFile> outFlowFiles = new ArrayList<FlowFile>();

        for (final ExecReport er : ers) {
            FlowFile anFF = session.create();
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
            return;
        }
    }
}

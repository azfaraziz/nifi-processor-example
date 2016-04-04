package com.tba.processor;

/**
 * Used this tutorial: http://www.nifi.rocks/developing-a-custom-apache-nifi-processor-json/
 * Github: https://github.com/pcgrenier/nifi-examples
 */


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.jayway.jsonpath.JsonPath;


@Tags({"JSON", "MyExample"})
@CapabilityDescription("Fetch value from json path")
public class ProcessorExample extends AbstractProcessor {

	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	
	public static final String MATCH_ATTRS = "match";
	
	public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
			.name("Json Path")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final Relationship SUCCESS = new Relationship.Builder()
			.name("SUCCESS")
			.description("Success relationship")
			.build();
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.nifi.processor.AbstractSessionFactoryProcessor#init(org.apache.nifi.processor.ProcessorInitializationContext)
	 *   The init function is called at the start of Apache Nifi
	 *   Remember that this is highly multi-threaded env
	 */
	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> props = new ArrayList<>();
		props.add(JSON_PATH);
		this.properties = Collections.unmodifiableList(props);
		
		Set<Relationship> rel = new HashSet<>();
		rel.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(rel);
	}
	
	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}
	
	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.nifi.processor.AbstractProcessor#onTrigger(org.apache.nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
	 * Called whenever a flow file is passed to the processor.
	 * 
	 * In general:
	 * 1) Pull the flow file out of session
	 * 2) Read and write to the flow files
	 * 3) Add attributes where needed
	 * 
	 */
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		final ProcessorLog log = this.getLogger();
		final AtomicReference<String> value = new AtomicReference<>();
		
		// Gets the flowfile
		FlowFile flowfile = session.get();
		
		session.read(flowfile,  new InputStreamCallback() {
			
			@Override
			public void process(InputStream in) throws IOException {
				try {
					String json = IOUtils.toString(in);
					String result = JsonPath.read(json, "$.hello");
					value.set(result);
				} catch (Exception ex) {
					ex.printStackTrace();
					log.error("Failed to read json string");
				}
			}
		});
		
		// Write the results to an attribute
		String results = value.get();
		if (results != null && !results.isEmpty()) {
			flowfile = session.putAttribute(flowfile, MATCH_ATTRS, results);
		}
		
		// To write the results back out to flow file
		flowfile = session.write(flowfile, new OutputStreamCallback() {
			
			@Override
			public void process(OutputStream out) throws IOException {
				out.write(value.get().getBytes());
			}
		});
		
		session.transfer(flowfile, SUCCESS);
	}

}

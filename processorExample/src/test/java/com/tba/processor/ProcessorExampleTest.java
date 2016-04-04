package com.tba.processor;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;



public class ProcessorExampleTest {

	@org.junit.Test
	public void testOnTrigger() throws IOException {
		// Content to be mocked as a json file
		InputStream content = new ByteArrayInputStream("{\"hello\":\"nifi rocks\"}".getBytes());
		
		// Generate a test runner to mock a processor in a flow
		TestRunner runner = TestRunners.newTestRunner(new ProcessorExample());
		
		// Add properties
		runner.setProperty(ProcessorExample.JSON_PATH, "$.hello");
		
		// Add the content to the runner
		runner.enqueue(content);
		
		// Run the enqueued content, it also takes an int which is the number of contents queueud
		runner.run(1);
		
		// All results were processed without failure
		runner.assertQueueEmpty();
		
		// If you need to read or do additional tests on results, you can access the content
		List<MockFlowFile> results = runner.getFlowFilesForRelationship(ProcessorExample.SUCCESS);
		assertTrue("1 match", results.size() == 1);
		MockFlowFile result = results.get(0);
		String resultsValue = new String(runner.getContentAsByteArray(result));
		System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result)));
		
		// Test attributes and content
		result.assertAttributeEquals(ProcessorExample.MATCH_ATTRS, "nifi rocks");
		result.assertContentEquals("nifi rocks");
				
	}
}

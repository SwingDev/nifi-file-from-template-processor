package io.swingdev.processors.filefromtemplate;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;


public class PutFileFromTemplateTest {
    @Test
    public void testSimpleTemplate() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new PutFileFromTemplate());
        testRunner.setProperty(PutFileFromTemplate.TEMPLATE_PROPERTY, "hello");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        MockFlowFile successFlowFile = testRunner.getFlowFilesForRelationship(PutFileFromTemplate.SUCCESS_RELATIONSHIP).get(0);
        successFlowFile.assertAttributeExists(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);

        String outputFilePath = successFlowFile.getAttribute(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);
        String renderedTemplate = FileUtils.readFileToString(new File(outputFilePath));

        assertEquals(renderedTemplate, "hello");

        testRunner.assertTransferCount(PutFileFromTemplate.SUCCESS_RELATIONSHIP, 1);
        testRunner.assertTransferCount(PutFileFromTemplate.FAILURE_RELATIONSHIP, 0);
        testRunner.assertTransferCount(PutFileFromTemplate.JSON_PARSING_FAILURE_RELATIONSHIP, 0);
    }

    @Test
    public void testPrefixSuffix() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new PutFileFromTemplate());
        testRunner.setProperty(PutFileFromTemplate.TEMPLATE_PROPERTY, "hello");
        testRunner.setProperty(PutFileFromTemplate.FILE_PREFIX_PROPERTY, "prefix");
        testRunner.setProperty(PutFileFromTemplate.FILE_SUFFIX_PROPERTY, ".suffix");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        MockFlowFile successFlowFile = testRunner.getFlowFilesForRelationship(PutFileFromTemplate.SUCCESS_RELATIONSHIP).get(0);
        successFlowFile.assertAttributeExists(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);

        String outputFilePath = successFlowFile.getAttribute(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);
        Path p = Paths.get(outputFilePath);

        assertTrue(p.getFileName().toString().startsWith("prefix"));
        assertTrue(p.getFileName().toString().endsWith(".suffix"));

        // Ensure something more than just prefix and suffix.
        assertNotEquals(outputFilePath, "prefix.suffix");

        testRunner.assertTransferCount(PutFileFromTemplate.SUCCESS_RELATIONSHIP, 1);
        testRunner.assertTransferCount(PutFileFromTemplate.FAILURE_RELATIONSHIP, 0);
        testRunner.assertTransferCount(PutFileFromTemplate.JSON_PARSING_FAILURE_RELATIONSHIP, 0);
    }

    @Test
    public void testAttributesInTemplate() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new PutFileFromTemplate());
        testRunner.setProperty(PutFileFromTemplate.TEMPLATE_PROPERTY, "hello_{{ attributes.attr }}");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, "attr", "test");

        testRunner.enqueue(ff);
        testRunner.run();

        MockFlowFile successFlowFile = testRunner.getFlowFilesForRelationship(PutFileFromTemplate.SUCCESS_RELATIONSHIP).get(0);
        successFlowFile.assertAttributeExists(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);

        String outputFilePath = successFlowFile.getAttribute(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);
        String renderedTemplate = FileUtils.readFileToString(new File(outputFilePath));

        assertEquals(renderedTemplate, "hello_test");

        testRunner.assertTransferCount(PutFileFromTemplate.SUCCESS_RELATIONSHIP, 1);
        testRunner.assertTransferCount(PutFileFromTemplate.FAILURE_RELATIONSHIP, 0);
        testRunner.assertTransferCount(PutFileFromTemplate.JSON_PARSING_FAILURE_RELATIONSHIP, 0);
    }

    @Test
    public void testMissingAttributesInTemplate() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new PutFileFromTemplate());
        testRunner.setProperty(PutFileFromTemplate.TEMPLATE_PROPERTY, "hello_{{ attributes.attr }}");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        MockFlowFile successFlowFile = testRunner.getFlowFilesForRelationship(PutFileFromTemplate.SUCCESS_RELATIONSHIP).get(0);
        successFlowFile.assertAttributeExists(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);

        String outputFilePath = successFlowFile.getAttribute(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);
        String renderedTemplate = FileUtils.readFileToString(new File(outputFilePath));

        assertEquals(renderedTemplate, "hello_");

        testRunner.assertTransferCount(PutFileFromTemplate.SUCCESS_RELATIONSHIP, 1);
        testRunner.assertTransferCount(PutFileFromTemplate.FAILURE_RELATIONSHIP, 0);
        testRunner.assertTransferCount(PutFileFromTemplate.JSON_PARSING_FAILURE_RELATIONSHIP, 0);
    }

    @Test
    public void testEmptyJSONContent() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new PutFileFromTemplate());
        testRunner.setProperty(PutFileFromTemplate.TEMPLATE_PROPERTY, "hello_{{ content.attr }}");
        testRunner.setProperty(PutFileFromTemplate.PARSE_JSON_CONTENT_PROPERTY, "true");

        ProcessSession session = testRunner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        testRunner.enqueue(ff);
        testRunner.run();

        testRunner.assertTransferCount(PutFileFromTemplate.SUCCESS_RELATIONSHIP, 1);
        testRunner.assertTransferCount(PutFileFromTemplate.FAILURE_RELATIONSHIP, 0);
        testRunner.assertTransferCount(PutFileFromTemplate.JSON_PARSING_FAILURE_RELATIONSHIP, 0);
    }

    @Test
    public void testInvalidJSONContent() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new PutFileFromTemplate());
        testRunner.setProperty(PutFileFromTemplate.TEMPLATE_PROPERTY, "hello_{{ content.attr }}");
        testRunner.setProperty(PutFileFromTemplate.PARSE_JSON_CONTENT_PROPERTY, "true");

        testRunner.enqueue(Paths.get("src/test/resources/invalid.json"));
        testRunner.run();

        testRunner.assertTransferCount(PutFileFromTemplate.SUCCESS_RELATIONSHIP, 0);
        testRunner.assertTransferCount(PutFileFromTemplate.FAILURE_RELATIONSHIP, 0);
        testRunner.assertTransferCount(PutFileFromTemplate.JSON_PARSING_FAILURE_RELATIONSHIP, 1);
    }

    @Test
    public void testValidJSONObjectContent() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new PutFileFromTemplate());
        testRunner.setProperty(PutFileFromTemplate.TEMPLATE_PROPERTY, "hello_{{ content.array[0].text }}_{{ content.array[1].text }}_{{ content.object.text }}");
        testRunner.setProperty(PutFileFromTemplate.PARSE_JSON_CONTENT_PROPERTY, "true");

        testRunner.enqueue(Paths.get("src/test/resources/valid_object.json"));
        testRunner.run();

        MockFlowFile successFlowFile = testRunner.getFlowFilesForRelationship(PutFileFromTemplate.SUCCESS_RELATIONSHIP).get(0);
        successFlowFile.assertAttributeExists(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);

        String outputFilePath = successFlowFile.getAttribute(PutFileFromTemplate.DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE);
        String renderedTemplate = FileUtils.readFileToString(new File(outputFilePath));

        assertEquals(renderedTemplate, "hello_sample array 1_sample array 2_sample object");

        testRunner.assertTransferCount(PutFileFromTemplate.SUCCESS_RELATIONSHIP, 1);
        testRunner.assertTransferCount(PutFileFromTemplate.FAILURE_RELATIONSHIP, 0);
        testRunner.assertTransferCount(PutFileFromTemplate.JSON_PARSING_FAILURE_RELATIONSHIP, 0);
    }

}

package io.swingdev.processors.filefromtemplate;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Tags({"template", "jinja", "json"})
@CapabilityDescription("Creates a file by rendering a Jinja2 template with the FlowFile's attributes as the context.")
@SeeAlso({})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PutFileFromTemplate extends AbstractProcessor {
    public static final PropertyDescriptor PARSE_JSON_CONTENT_PROPERTY = new PropertyDescriptor
            .Builder().name("JSON content")
            .description("Parse Flow File content as JSON for the template.")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMPLATE_PROPERTY = new PropertyDescriptor
            .Builder().name("Template")
            .description("Jinja2 Template")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILE_PREFIX_PROPERTY = new PropertyDescriptor
            .Builder().name("File Prefix")
            .description("File Prefix.")
            .defaultValue("rendered")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILE_SUFFIX_PROPERTY = new PropertyDescriptor
            .Builder().name("File Suffix")
            .description("File Suffix.")
            .defaultValue(".out")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final String DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE = "template.rendered.path";
    public static final PropertyDescriptor OUTPUT_PATH_SAVED_IN_ATTRIBUTE_PROPERTY = new PropertyDescriptor
            .Builder().name("Output Path attribute")
            .description("Output Path will be saved in this attribute")
            .required(true)
            .defaultValue(DEFAULT_OUTPUT_PATH_SAVED_IN_ATTRIBUTE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("On success")
            .build();
    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("On failure")
            .build();
    public static final Relationship JSON_PARSING_FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("json_failure")
            .description("On JSON parsing failure")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    volatile Jinjava jinjava;
    volatile JsonFactory jsonFactory;
    volatile ObjectMapper jsonMapper;

    Map<String, Object> validateAndParseJSONContent(ProcessSession processSession, FlowFile flowFile) throws JsonParseException {
        final Map<String, Object> jsonContent = Maps.newHashMap();

        try {
            processSession.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    JsonParser jp = jsonFactory.createParser(in);
                    jsonContent.putAll(jp.readValueAs(Map.class));
                }
            });
        } catch (ProcessException e) {
            if (e.getCause() instanceof JsonParseException) {
                throw (JsonParseException)e.getCause();
            } else {
                throw e;
            }
        }

        return jsonContent;
    }

    Map<String, Object> templateContextFromFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) throws JsonParseException {
        Map<String, Object> templateContext = Maps.newHashMap();

        templateContext.put("attributes", flowFile.getAttributes());

        if (context.getProperty(PARSE_JSON_CONTENT_PROPERTY).asBoolean() == Boolean.TRUE && flowFile.getSize() > 0) {
            Map<String, Object> jsonContent = validateAndParseJSONContent(session, flowFile);

            templateContext.put("content", jsonContent);
        }

        return templateContext;
    }

    String getTemplate(final ProcessContext context) {
        return context.getProperty(TEMPLATE_PROPERTY).getValue();
    }

    File createOutputFile(final ProcessContext context) throws IOException{
        File tempFile = File.createTempFile(context.getProperty(FILE_PREFIX_PROPERTY).getValue(), context.getProperty(FILE_SUFFIX_PROPERTY).getValue());
        tempFile.deleteOnExit();

        return tempFile;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PARSE_JSON_CONTENT_PROPERTY);
        descriptors.add(TEMPLATE_PROPERTY);
        descriptors.add(FILE_PREFIX_PROPERTY);
        descriptors.add(FILE_SUFFIX_PROPERTY);
        descriptors.add(OUTPUT_PATH_SAVED_IN_ATTRIBUTE_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        relationships.add(JSON_PARSING_FAILURE_RELATIONSHIP);
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
        jsonMapper = new ObjectMapper();
        jsonFactory = jsonMapper.getFactory();
        jinjava = new Jinjava();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}

        final ProcessorLog logger = getLogger();

        final String template = getTemplate(context);
        Map<String, Object> templateContext;
        try {
            templateContext = templateContextFromFlowFile(context, session, flowFile);
        } catch (JsonParseException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{flowFile});
            session.transfer(flowFile, JSON_PARSING_FAILURE_RELATIONSHIP);

            return;
        }

        final String renderedTemplate = jinjava.render(template, templateContext);

        File outputFile;
        try {
            outputFile = createOutputFile(context);
            FileUtils.writeStringToFile(outputFile, renderedTemplate);
        } catch (IOException e) {
            logger.error("Could not create a temp. file.");
            session.transfer(flowFile, FAILURE_RELATIONSHIP);

            return;
        }

        flowFile = session.putAttribute(flowFile, context.getProperty(OUTPUT_PATH_SAVED_IN_ATTRIBUTE_PROPERTY).getValue(), outputFile.getAbsolutePath());
        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }

}

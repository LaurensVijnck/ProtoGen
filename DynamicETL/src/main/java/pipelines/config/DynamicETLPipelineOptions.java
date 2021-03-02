package pipelines.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface DynamicETLPipelineOptions extends DataflowPipelineOptions {

    @Validation.Required
    @Default.InstanceFactory(GcpOptions.DefaultProjectFactory.class)
    String getProject();
    void setProject(String value);

    @Description("Input subscription")
    @Validation.Required
    ValueProvider<String> getPubSubInputSubscription();
    void setPubSubInputSubscription(ValueProvider<String> value);

    @Description("Environment (dev, acc, prod)")
    @Validation.Required
    ValueProvider<String> getEnvironment();
    void setEnvironment(ValueProvider<String> value);

    @Description("BigQuery output dead-letter table")
    @Validation.Required
    @Default.String("geometric-ocean-284614:dynamic_etl.event_failed")
    String getDeadLetterTable();
    void setDeadLetterTable(String value);
}

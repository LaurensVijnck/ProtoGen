package pipelines.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface DynamicETLPipelineOptions extends DataflowPipelineOptions {

    @Description("Input directory")
    @Validation.Required
    @Default.String("projects/geometric-ocean-284614/subscriptions/dynamic_etl_subscription")
    String getPubSubInputSubscription();
    void setPubSubInputSubscription(String value);

    @Description("BigQuery output table")
    @Validation.Required
    @Default.String("geometric-ocean-284614:dynamic_etl.event")
    String getOutputTable();
    void setOutputTable(String value);

    @Description("BigQuery output dead-letter table")
    @Validation.Required
    @Default.String("geometric-ocean-284614:dynamic_etl.event_failed")
    String getDeadLetterTable();
    void setDeadLetterTable(String value);
}

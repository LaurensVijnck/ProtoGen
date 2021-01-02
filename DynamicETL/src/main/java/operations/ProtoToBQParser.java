package operations;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import lvi.BQParserImp;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProtoToBQParser<InputT> extends PTransform<PCollection<InputT>, PDone> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoToBQParser.class);

    private final SerializableFunction<InputT, String> protoTypeExtractor;
    private final SerializableFunction<InputT, byte[]> protoPayloadExtractor;
    private final SerializableFunction<InputT, String> destinationExtractor;

    public ProtoToBQParser(SerializableFunction<InputT, String> protoTypeExtractor,
                           SerializableFunction<InputT, String> destinationExtractor,
                           SerializableFunction<InputT, byte[]> protoPayloadExtractor) {

        this.protoTypeExtractor = protoTypeExtractor;
        this.destinationExtractor = destinationExtractor;
        this.protoPayloadExtractor = protoPayloadExtractor;
    }

    @Override
    public PDone expand(PCollection<InputT> input) {

        // Apply ParDo
        input
                .apply("MapToJSON", ParDo.of(new ProtoToBQ()))
                .apply(BigQueryIO.<KV<KV<TableDestination, String>, TableRow>>write()
                        .to(new ProtoToBQDynamicDestinations())
                        .withFormatFunction((SerializableFunction<KV<KV<TableDestination, String>, TableRow>, TableRow>) el -> {
                            // FUTURE: A shame this function can't be used to return multiple TableRows
                            return el.getValue();
                        })
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return PDone.in(input.getPipeline());
    }

    private class ProtoToBQ extends DoFn<InputT, KV<KV<TableDestination, String>, TableRow>> {

        @ProcessElement
        public void processElement(@Element InputT input, ProcessContext c) {

            try {

                // Retrieve parser for type
                String protoType = protoTypeExtractor.apply(input);
                BQParserImp parser = BQParserImp.getParserForType(protoType);

                // Extract rows and destination
                for(TableRow row: parser.convertToTableRow(protoPayloadExtractor.apply(input))) {

                    LOG.info(row.toPrettyString());

                    // Generate output object
                    c.output(KV.of(
                            KV.of(new TableDestination(
                                    new TableReference()
                                            .setDatasetId(destinationExtractor.apply(input))
                                            .setTableId(parser.getBigQueryTableName()), null),
                                    protoType),
                            row));
                }
            } catch (Exception e) {
                LOG.info("Error " + e.getMessage());

                // FUTURE: Add DLQ path to pipeline
                // c.output(DLQTag, new FailSafeElement<>(input, e.getMessage()));
            }
        }
    }

    private class ProtoToBQDynamicDestinations extends DynamicDestinations<KV<KV<TableDestination, String>, TableRow>, KV<TableDestination, String>> {

        @Override
        public KV<TableDestination, String> getDestination(ValueInSingleWindow<KV<KV<TableDestination, String>, TableRow>> element) {
            return element.getValue().getKey();
        }

        @Override
        public TableDestination getTable(KV<TableDestination, String> destination) {
            return destination.getKey();
        }

        @Override
        public TableSchema getSchema(KV<TableDestination, String> destination) {
            try {
                // TableSchemas are not serializable by default, hence we obtain it here.
                return BQParserImp.getParserForType(destination.getValue()).getBigQueryTableSchema();
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }
    }
}

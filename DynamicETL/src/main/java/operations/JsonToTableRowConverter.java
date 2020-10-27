package operations;

import com.google.api.services.bigquery.model.TableRow;
import models.FailSafeElement;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class JsonToTableRowConverter<OriginalT> extends SimpleFunction<FailSafeElement<OriginalT, String>, TableRow> {

    public TableRow apply(FailSafeElement<OriginalT, String> input) {
        return convertJsonToTableRow(input.getPayload());
    }

    private static TableRow convertJsonToTableRow(String el) {
        TableRow row;

        try (InputStream inputStream = new ByteArrayInputStream(el.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + el, e);
        }

        return row;
    }
}

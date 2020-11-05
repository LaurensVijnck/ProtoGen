// Generated by the proto-to-bq Proto compiler plugin.  DO NOT EDIT!
package lvi;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableCell;

import java.util.LinkedList;

public final class EventParser {
	public static LinkedList<TableRow> convertToTableRow(EventOuterClass.Event event) throws Exception {
		LinkedList<TableRow> rows = new LinkedList<TableRow>();
		TableRow common = new TableRow();
		if(event.hasClient()) {
			TableCell client = new TableCell();
			if(event.getClient().hasTenantId()) {
				client.set("tenantId", event.getClient().getTenantId());
			} else {
				throw new Exception();
			}
			client.set("name", event.getClient().getName());
			common.set("client", client);
		} else {
			throw new Exception();
		}
		for(EventOuterClass.BatchEvent batchEvent: event.getEventsList()) {
			TableRow row = new TableRow();
			if(batchEvent.hasEvents()) {
				TableCell events = new TableCell();
				if(batchEvent.getEvents().hasActor()) {
					TableCell actor = new TableCell();
					if(batchEvent.getEvents().getActor().hasUserId()) {
						actor.set("userId", batchEvent.getEvents().getActor().getUserId());
					} else {
						throw new Exception();
					}
					if(batchEvent.getEvents().getActor().hasEmail()) {
						actor.set("email", batchEvent.getEvents().getActor().getEmail());
					}
					if(batchEvent.getEvents().getActor().hasAddress()) {
						TableCell address = new TableCell();
						address.set("street", batchEvent.getEvents().getActor().getAddress().getStreet());
						address.set("number", batchEvent.getEvents().getActor().getAddress().getNumber());
						address.set("country", batchEvent.getEvents().getActor().getAddress().getCountry());
						actor.set("address", address);
					}
					events.set("actor", actor);
				} else {
					throw new Exception();
				}
				row.set("events", events);
			} else {
				throw new Exception();
			}
			row.setF(common.getF());
			rows.add(row);
		}
		return rows;
	}
}

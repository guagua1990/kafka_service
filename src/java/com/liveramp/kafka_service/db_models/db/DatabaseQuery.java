
/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.kafka_service.db_models.db;

import com.rapleaf.jack.DatabaseConnection;
import com.rapleaf.jack.queries.GenericQuery;
import com.rapleaf.jack.queries.Table;

public class DatabaseQuery extends GenericQuery {
  private DatabaseQuery(Table table) {
    super(new DatabaseConnection("database"), table);
  }

  public static GenericQuery from(Table table) {
    return new DatabaseQuery(table);
  }
}

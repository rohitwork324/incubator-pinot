/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.util.AvroUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class MapTypeClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_DOCS = 1000;
  private static final String MAP_FIELD_NAME = "myMap";

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create the tables
    addOfflineTable(getTableName());

    // Create and upload segments
    File avroFile = createAvroFile();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMultiValueDimension(MAP_FIELD_NAME + AvroUtils.MAP_KEY_COLUMN_SUFFIX, DataType.STRING)
        .addMultiValueDimension(MAP_FIELD_NAME + AvroUtils.MAP_VALUE_COLUMN_SUFFIX, DataType.INT).build();
    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(Collections.singletonList(avroFile), 0, _segmentDir, _tarDir, getTableName(), false,
            null, null, schema, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);
    uploadSegments(getTableName(), _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(60_000);
  }

  private File createAvroFile()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema mapAvroSchema = org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(Type.INT));
    List<Field> fields = Collections.singletonList(new Field(MAP_FIELD_NAME, mapAvroSchema, null, null));
    avroSchema.setFields(fields);

    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_DOCS; i++) {
        Map<String, Integer> map = new HashMap<>();
        map.put("k1", i);
        map.put("k2", NUM_DOCS + i);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(MAP_FIELD_NAME, map);
        fileWriter.append(record);
      }
    }

    return avroFile;
  }

  @Test
  public void testQueries()
      throws Exception {
    // Selection only
    String query = "SELECT MAP_VALUE(myMap__KEYS, 'k1', myMap__VALUES) FROM " + getTableName();
    JsonNode pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode selectionResults = pinotResponse.get("selectionResults").get("results");
    assertEquals(selectionResults.size(), 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(Integer.parseInt(selectionResults.get(i).get(0).textValue()), i);
    }

    // Selection order-by
    query = "SELECT MAP_VALUE(myMap__KEYS, 'k2', myMap__VALUES) FROM " + getTableName()
        + " ORDER BY MAP_VALUE(myMap__KEYS, 'k1', myMap__VALUES)";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    selectionResults = pinotResponse.get("selectionResults").get("results");
    assertEquals(selectionResults.size(), 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(Integer.parseInt(selectionResults.get(i).get(0).textValue()), NUM_DOCS + i);
    }

    // Aggregation only
    query = "SELECT MAX(MAP_VALUE(myMap__KEYS, 'k1', myMap__VALUES)) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode aggregationResult = pinotResponse.get("aggregationResults").get(0).get("value");
    assertEquals((int) Double.parseDouble(aggregationResult.textValue()), NUM_DOCS - 1);

    // Aggregation group-by
    query = "SELECT MIN(MAP_VALUE(myMap__KEYS, 'k2', myMap__VALUES)) FROM " + getTableName()
        + " GROUP BY MAP_VALUE(myMap__KEYS, 'k1', myMap__VALUES)";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    JsonNode groupByResults = pinotResponse.get("aggregationResults").get(0).get("groupByResult");
    assertEquals(groupByResults.size(), 10);
    for (int i = 0; i < 10; i++) {
      JsonNode groupByResult = groupByResults.get(i);
      assertEquals(Integer.parseInt(groupByResult.get("group").get(0).asText()), i);
      assertEquals((int) Double.parseDouble(groupByResult.get("value").asText()), NUM_DOCS + i);
    }

    // Filter
    query = "SELECT MAP_VALUE(myMap__KEYS, 'k2', myMap__VALUES) FROM " + getTableName()
        + " WHERE MAP_VALUE(myMap__KEYS, 'k1', myMap__VALUES) = 25";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    selectionResults = pinotResponse.get("selectionResults").get("results");
    assertEquals(selectionResults.size(), 1);
    assertEquals(Integer.parseInt(selectionResults.get(0).get(0).textValue()), NUM_DOCS + 25);

    // Non-existing key
    query = "SELECT MAP_VALUE(myMap__KEYS, 1, myMap__VALUES) FROM " + getTableName();
    pinotResponse = postQuery(query);
    assertNotEquals(pinotResponse.get("exceptions").size(), 0);

    query = "SELECT MAP_VALUE(myMap__KEYS, 1, myMap__VALUES) FROM " + getTableName() + " WHERE myMap__KEYS = 1";
    pinotResponse = postQuery(query);
    assertEquals(pinotResponse.get("exceptions").size(), 0);
    selectionResults = pinotResponse.get("selectionResults").get("results");
    assertEquals(selectionResults.size(), 0);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}

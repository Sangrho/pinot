/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LimitStringLengthRecordReaderTest {
  private static final String COLUMN_NAME = "testColumn";
  private static final int LENGTH_LIMIT = 10;
  private static final int MAX_LENGTH = 20;
  private static final int NUM_ROWS = 100;

  @Test
  public void testTrimString() throws IOException {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING).build();
    schema.getFieldSpecFor(COLUMN_NAME).setStringLengthLimit(10);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    long expectedNumStringsTrimmed = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      String value = RandomStringUtils.random(MAX_LENGTH);
      if (value.length() > LENGTH_LIMIT) {
        expectedNumStringsTrimmed++;
      }
      row.putField(COLUMN_NAME, value);
      rows.add(row);
    }

    try (LimitStringLengthRecordReader recordReader = new LimitStringLengthRecordReader(
        new GenericRowRecordReader(rows, schema))) {
      while (recordReader.hasNext()) {
        GenericRow next = recordReader.next();
        assertTrue(((String) next.getValue(COLUMN_NAME)).length() <= LENGTH_LIMIT);
      }
      assertEquals(recordReader.getNumStringsTrimmed(), expectedNumStringsTrimmed);
    }
  }
}

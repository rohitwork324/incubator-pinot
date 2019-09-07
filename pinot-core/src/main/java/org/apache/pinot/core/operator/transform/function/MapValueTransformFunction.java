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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.segment.index.readers.Dictionary;


/**
 * The MAP_VALUE transform function takes 3 arguments: keyColumn, keyValue, valueColumn, where keyColumn and valueColumn
 * are dictionary-encoded multi-value columns, and keyValue must be a literal (number or string). In order to make
 * MAP_VALUE transform function work, the keyValue provided must exist in the keyColumn. To ensure that, the query can
 * have a filter on the keyColumn, e.g. SELECT MAP_VALUE(key, 'myKey', value) FROM myTable WHERE key = 'myKey'.
 */
public class MapValueTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "map_value";

  private TransformFunction _keyColumnFunction;
  private int _inputKeyDictId;
  private TransformFunction _valueColumnFunction;
  private TransformResultMetadata _resultMetadata;

  private int[] _dictIds;
  private int[] _intValues;
  private long[] _longValues;
  private float[] _floatValues;
  private double[] _doubleValues;
  private String[] _stringValues;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() == 3,
        "3 arguments are required for MAP_VALUE transform function: keyColumn, keyValue, valueColumn, e.g. MAP_VALUE(key, 'myKey', value)");

    _keyColumnFunction = arguments.get(0);
    TransformResultMetadata keyColumnMetadata = _keyColumnFunction.getResultMetadata();
    Preconditions.checkState(!keyColumnMetadata.isSingleValue() && keyColumnMetadata.hasDictionary(),
        "Key column must be dictionary-encoded multi-value column");

    TransformFunction keyValueFunction = arguments.get(1);
    Preconditions.checkState(keyValueFunction instanceof LiteralTransformFunction,
        "Key value must be a literal (number or string)");
    String keyValue = ((LiteralTransformFunction) keyValueFunction).getLiteral();
    _inputKeyDictId = _keyColumnFunction.getDictionary().indexOf(keyValue);

    _valueColumnFunction = arguments.get(2);
    TransformResultMetadata valueColumnMetadata = _valueColumnFunction.getResultMetadata();
    Preconditions.checkState(!valueColumnMetadata.isSingleValue() && valueColumnMetadata.hasDictionary(),
        "Value column must be dictionary-encoded multi-value column");
    _resultMetadata = new TransformResultMetadata(valueColumnMetadata.getDataType(), true, true);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public Dictionary getDictionary() {
    return _valueColumnFunction.getDictionary();
  }

  @Override
  public int[] transformToDictIdsSV(@Nonnull ProjectionBlock projectionBlock) {
    Preconditions.checkState(_inputKeyDictId >= 0,
        "Key value must exist in the key column. Add filter on key column if necessary, e.g. SELECT MAP_VALUE(key, 'myKey', value) FROM myTable WHERE key = 'myKey'");

    if (_dictIds == null) {
      _dictIds = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    int[][] keyDictIds = _keyColumnFunction.transformToDictIdsMV(projectionBlock);
    int[][] valueDictIds = _valueColumnFunction.transformToDictIdsMV(projectionBlock);
    int length = projectionBlock.getNumDocs();
    for (int i = 0; i < length; i++) {
      int numKeys = keyDictIds[i].length;
      for (int j = 0; j < numKeys; j++) {
        if (keyDictIds[i][j] == _inputKeyDictId) {
          _dictIds[i] = valueDictIds[i][j];
          break;
        }
      }
    }
    return _dictIds;
  }

  @Override
  public int[] transformToIntValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    if (_intValues == null) {
      _intValues = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    dictionary.readIntValues(dictIds, 0, projectionBlock.getNumDocs(), _intValues, 0);
    return _intValues;
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    if (_longValues == null) {
      _longValues = new long[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    dictionary.readLongValues(dictIds, 0, projectionBlock.getNumDocs(), _longValues, 0);
    return _longValues;
  }

  @Override
  public float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    if (_floatValues == null) {
      _floatValues = new float[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    dictionary.readFloatValues(dictIds, 0, projectionBlock.getNumDocs(), _floatValues, 0);
    return _floatValues;
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    if (_doubleValues == null) {
      _doubleValues = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    dictionary.readDoubleValues(dictIds, 0, projectionBlock.getNumDocs(), _doubleValues, 0);
    return _doubleValues;
  }

  @Override
  public String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType() != DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    if (_stringValues == null) {
      _stringValues = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    Dictionary dictionary = getDictionary();
    int[] dictIds = transformToDictIdsSV(projectionBlock);
    dictionary.readStringValues(dictIds, 0, projectionBlock.getNumDocs(), _stringValues, 0);
    return _stringValues;
  }
}

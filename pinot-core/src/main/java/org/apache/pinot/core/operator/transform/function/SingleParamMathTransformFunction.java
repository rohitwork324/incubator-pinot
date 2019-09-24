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

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.util.ArrayCopyUtils;

/**
 * A group of commonly used math transformation which has only one single parameter,
 * including abs, exp, ceil, floor, sqrt.
 */
public abstract class SingleParamMathTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;
  protected double[] _results;

  @Override
  public abstract String getName();

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 1 argument
    if (arguments.size() != 1) {
      throw new IllegalArgumentException("Exactly 1 arguments are required for " + getName() + " transform function");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction) {
      throw new IllegalArgumentException("Argument of " + getName() + " should not be literal");
    } else {
      if (!firstArgument.getResultMetadata().isSingleValue()) {
        throw new IllegalArgumentException("First argument of " + getName() + " transform function must be single-valued");
      }
      _transformFunction = firstArgument;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();

    if (_transformFunction.getResultMetadata().getDataType() == DataType.STRING) {
      String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
      ArrayCopyUtils.copy(stringValues, _results, length);
      applyMathOperator(_results, length);
    } else {
      double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
      applyMathOperator(doubleValues, length);
    }

    return _results;
  }

  abstract protected void applyMathOperator(double[] values, int length);

  public static class AbsTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "abs";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = Math.abs(values[i]);
      }
    }
  }

  public static class CeilTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "ceil";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = Math.ceil(values[i]);
      }
    }
  }

  public static class ExpTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "exp";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = Math.exp(values[i]);
      }
    }
  }

  public static class FloorTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "floor";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = Math.floor(values[i]);
      }
    }
  }

  public static class LnTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "ln";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = Math.log(values[i]);
      }
    }
  }

  public static class SqrtTransformFunction extends SingleParamMathTransformFunction {
    public static final String FUNCTION_NAME = "sqrt";

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    protected void applyMathOperator(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        _results[i] = Math.sqrt(values[i]);
      }
    }
  }
}

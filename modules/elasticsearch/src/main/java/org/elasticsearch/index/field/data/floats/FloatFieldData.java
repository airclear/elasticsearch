/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.field.data.floats;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;
import org.elasticsearch.common.trove.TFloatArrayList;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.field.data.support.FieldDataLoader;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class FloatFieldData extends NumericFieldData<FloatDocFieldData> {

    static final float[] EMPTY_FLOAT_ARRAY = new float[0];

    protected final float[] values;

    protected FloatFieldData(String fieldName, float[] values) {
        super(fieldName);
        this.values = values;
    }

    abstract public float value(int docId);

    abstract public float[] values(int docId);

    @Override public FloatDocFieldData docFieldData(int docId) {
        return super.docFieldData(docId);
    }

    @Override protected FloatDocFieldData createFieldData() {
        return new FloatDocFieldData(this);
    }

    @Override public String stringValue(int docId) {
        return Float.toString(value(docId));
    }

    @Override public void forEachValue(StringValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(Float.toString(values[i]));
        }
    }

    @Override public byte byteValue(int docId) {
        return (byte) value(docId);
    }

    @Override public short shortValue(int docId) {
        return (short) value(docId);
    }

    @Override public int intValue(int docId) {
        return (int) value(docId);
    }

    @Override public long longValue(int docId) {
        return (long) value(docId);
    }

    @Override public float floatValue(int docId) {
        return value(docId);
    }

    @Override public double doubleValue(int docId) {
        return (double) value(docId);
    }

    @Override public FieldDataType type() {
        return FieldDataType.DefaultTypes.FLOAT;
    }

    public void forEachValue(ValueProc proc) {
        for (int i = 1; i < values.length; i++) {
            proc.onValue(values[i]);
        }
    }

    public static interface ValueProc {
        void onValue(float value);
    }


    public static FloatFieldData load(IndexReader reader, String field) throws IOException {
        return FieldDataLoader.load(reader, field, new FloatTypeLoader());
    }

    static class FloatTypeLoader extends FieldDataLoader.FreqsTypeLoader<FloatFieldData> {

        private final TFloatArrayList terms = new TFloatArrayList();

        FloatTypeLoader() {
            super();
            // the first one indicates null value
            terms.add(0);
        }

        @Override public void collectTerm(String term) {
            terms.add(FieldCache.NUMERIC_UTILS_FLOAT_PARSER.parseFloat(term));
        }

        @Override public FloatFieldData buildSingleValue(String field, int[] ordinals) {
            return new SingleValueFloatFieldData(field, ordinals, terms.toNativeArray());
        }

        @Override public FloatFieldData buildMultiValue(String field, int[][] ordinals) {
            return new MultiValueFloatFieldData(field, ordinals, terms.toNativeArray());
        }
    }
}
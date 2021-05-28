package com.data.navi;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import static com.data.navi.Util.throwError;

public class StructListReduce extends GenericUDF {
    private ListObjectInspector listOI;
    private StructObjectInspector structOI;
    private String key;
    private String func;
    private StructField sf;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        validateInput(objectInspectors);

        listOI = ((ListObjectInspector) objectInspectors[0]);
        structOI = ((StructObjectInspector) listOI.getListElementObjectInspector());
        key = getConstantStringValue(objectInspectors, 1);
        sf = structOI.getStructFieldRef(key);

        if(sf == null)
            throwError(String.format("Key %s not found in struct", key));
        if(!(sf.getFieldObjectInspector() instanceof IntObjectInspector))
            throwError("Aggregation supported on Int types only");

        func = getConstantStringValue(objectInspectors, 2);
        if(!func.equals("min"))
            throwError("Only min aggregation supported");

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        int numElements = listOI.getListLength(deferredObjects[0].get());

        int minValue = Integer.MAX_VALUE;

        for (int i = 0; i < numElements; i++) {
            LazyInteger currValue = (LazyInteger)(structOI.getStructFieldData(listOI.getListElement(deferredObjects[0].get(), i), sf));
            minValue = Math.min(currValue.getWritableObject().get(), minValue);
        }
        return minValue;
    }

    private void validateInput(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 3)
            throwError("Three parameters are required");

        if(!(objectInspectors[0] instanceof ListObjectInspector))
            throwError("first argument should be List");

        if(!(((ListObjectInspector) objectInspectors[0]).getListElementObjectInspector() instanceof StructObjectInspector))
            throwError("first argument should be list of struct");

        if(!(objectInspectors[1] instanceof StringObjectInspector))
            throwError("second argument should of String type");

        if(!(objectInspectors[2] instanceof StringObjectInspector))
            throwError("third argument should of String type");

    }

    @Override
    public String getDisplayString(String[] strings) {
        StringBuilder sb = new StringBuilder();
        sb.append("struct_reduce(")
                .append(strings[0])
                .append(",")
                .append(strings[1])
                .append(",")
                .append(strings[2])
                .append(")");
        return sb.toString();
    }
}

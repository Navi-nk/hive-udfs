package com.data.navi;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.HashMap;
import java.util.Map;

import static com.data.navi.Util.throwError;

public class MapStructReduce extends GenericUDF {
    private MapObjectInspector mapOI;
    private ListObjectInspector listOI;
    private StructObjectInspector structOI;
    private String key;
    private String func;
    private StructField sf;

    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        validateInput(objectInspectors);

        mapOI = ((MapObjectInspector) objectInspectors[0]);
        listOI = ((ListObjectInspector) mapOI.getMapValueObjectInspector());
        structOI = ((StructObjectInspector) listOI.getListElementObjectInspector());

        key = getConstantStringValue(objectInspectors, 1);
        sf = structOI.getStructFieldRef(key);
        if(sf == null)
            throwError("key not found in struct");
        if(!(sf.getFieldObjectInspector() instanceof IntObjectInspector))
            throwError("Aggregation on Int types supported");

        func = getConstantStringValue(objectInspectors, 2);
        if(!func.equals("min"))
            throwError("Only min aggregation supported");

        return ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Map<String,Integer> result = new HashMap<String, Integer>();
        Map<?,?> m =  mapOI.getMap(deferredObjects[0].get());
        for(Map.Entry<?,?> e : m.entrySet()) {
            DeferredObject keyObject = new DeferredJavaObject(e.getKey());
            DeferredObject valueObject = new DeferredJavaObject(e.getValue());

            int numElements = listOI.getListLength(valueObject.get());

            int minValue = Integer.MAX_VALUE;

            for (int i = 0; i < numElements; i++) {
                LazyInteger currValue = (LazyInteger)(structOI.getStructFieldData(listOI.getListElement(valueObject.get(), i), sf));
                minValue = Math.min(currValue.getWritableObject().get(), minValue);
            }

            result.put(keyObject.get().toString(), minValue);
        }
        return result;

    }

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

    private void validateInput(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 3)
            throwError("Three parameters are required");

        if(!(objectInspectors[0] instanceof MapObjectInspector))
            throwError("first argument should be Map");

        MapObjectInspector mapOI = ((MapObjectInspector) objectInspectors[0]);
        if(!(mapOI.getMapKeyObjectInspector() instanceof StringObjectInspector))
            throwError("first argument should be Map of String,  List<Struct>");
        if(!(mapOI.getMapValueObjectInspector() instanceof ListObjectInspector))
            throwError("first argument should be Map of String, List<Struct>");

        ListObjectInspector listOI = ((ListObjectInspector) mapOI.getMapValueObjectInspector());
        if(!(listOI.getListElementObjectInspector() instanceof StructObjectInspector)){
            throwError("first argument should be Map of String, List<Struct>");
        }

        if(!(objectInspectors[1] instanceof StringObjectInspector))
            throwError("second argument should of String type");

        if(!(objectInspectors[2] instanceof StringObjectInspector))
            throwError("third argument should of String type");

    }
}
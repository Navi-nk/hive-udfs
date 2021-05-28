package com.data.navi;

import javafx.beans.value.WritableIntegerValue;
import javafx.beans.value.WritableMapValue;
import javafx.beans.value.WritableStringValue;
import org.apache.arrow.flatbuf.Int;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;

public class MapStructReduce extends GenericUDF {
    private MapObjectInspector mapOI;
    private ListObjectInspector listOI;
    private StructObjectInspector structOI;
    private String key;
    private String func;
    private StructField sf;

    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 3) {
            throw new UDFArgumentException("Three parameters are required");
        }

        if(!(objectInspectors[0] instanceof MapObjectInspector))
            throw new UDFArgumentException("first argument should be Map");
        mapOI = ((MapObjectInspector) objectInspectors[0]);
        ObjectInspector mapKeyOI = mapOI.getMapKeyObjectInspector();
        ObjectInspector mapValueOI = mapOI.getMapValueObjectInspector();
        if(!(mapKeyOI instanceof StringObjectInspector))
            throw new UDFArgumentException("first argument should be Map of String,  List<Struct>");
        if(!(mapValueOI instanceof ListObjectInspector))
            throw new UDFArgumentException("first argument should be Map of String, List<Struct>");

        listOI = ((ListObjectInspector) mapValueOI);
        if(!(listOI.getListElementObjectInspector() instanceof StructObjectInspector)){
            throw new UDFArgumentException("first argument should be Map of String, List<Struct>");
        }
        structOI = ((StructObjectInspector) listOI.getListElementObjectInspector());

        if(!(objectInspectors[1] instanceof StringObjectInspector))
            throw new UDFArgumentException("second argument should of String type");
        key = getConstantStringValue(objectInspectors, 1);

        sf = structOI.getStructFieldRef(key);
        if(sf == null)
            throw new UDFArgumentException("key not found in struct");
        if(!(sf.getFieldObjectInspector() instanceof IntObjectInspector))
            throw new UDFArgumentException("Aggregation on Int types supported");

        if(!(objectInspectors[2] instanceof StringObjectInspector))
            throw new UDFArgumentException("third argument should of String type");
        func = getConstantStringValue(objectInspectors, 2);

        if(!func.equals("min"))
            throw new UDFArgumentException("Only min aggregation supported");

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
            System.out.println(minValue);
            System.out.println(keyObject.get().toString());
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
}
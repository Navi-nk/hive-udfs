package com.data.navi;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.HashMap;
import java.util.Map;

import static com.data.navi.Util.throwError;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

public class NestedStructMinReduce extends GenericUDF {
    private MapObjectInspector mapOI;
    private StructObjectInspector firstStructOI;
    private ListObjectInspector listOI;
    private StructObjectInspector structOI;
    private MapObjectInspector innerMapOI;

    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        validateInput(objectInspectors);

        mapOI = ((MapObjectInspector) objectInspectors[0]);
        firstStructOI = (StructObjectInspector) mapOI.getMapValueObjectInspector();
        innerMapOI = (MapObjectInspector)firstStructOI.getStructFieldRef("advertisers").getFieldObjectInspector();
        listOI = ((ListObjectInspector) innerMapOI.getMapValueObjectInspector());
        structOI = ((StructObjectInspector) listOI.getListElementObjectInspector());

        return getStandardMapObjectInspector(javaIntObjectInspector,
                getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector));
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Map<Integer, Map<String , Integer>> result  = new HashMap<>();
        Map<?,?> m =  mapOI.getMap(deferredObjects[0].get());
        for(Map.Entry<?,?> e : m.entrySet()) {
            DeferredObject keyObject = new DeferredJavaObject(e.getKey());
            DeferredObject valueObject = new DeferredJavaObject(e.getValue());
            Object obj = firstStructOI.getStructFieldData(valueObject.get(), firstStructOI.getStructFieldRef("advertisers"));
            Map<?, ?> map = innerMapOI.getMap(obj);
            result.put(Integer.parseInt(keyObject.get().toString()), createMinMap(map));
        }
        return result;

    }

    private Map<String, Integer> createMinMap(Map<?, ?> deferredMap) throws HiveException{
        Map<String,Integer> result = new HashMap<>();
        for(Map.Entry<?,?> e : deferredMap.entrySet()) {
            DeferredObject keyObject = new DeferredJavaObject(e.getKey());
            DeferredObject valueObject = new DeferredJavaObject(e.getValue());

            int numElements = listOI.getListLength(valueObject.get());

            int minValue = Integer.MAX_VALUE;

            for (int i = 0; i < numElements; i++) {
                LazyInteger currValue = (LazyInteger)(structOI.getStructFieldData(listOI.getListElement(valueObject.get(), i), structOI.getStructFieldRef("eurocents")));
                minValue = Math.min(currValue.getWritableObject().get(), minValue);
            }

            result.put(keyObject.get().toString(), minValue);
        }
        return result;
    }

    public String getDisplayString(String[] strings) {
        StringBuilder sb = new StringBuilder();
        sb.append("min_map_reduce(")
                .append(strings[0])
                .append(")");
        return sb.toString();
    }

    private void validateInput(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1)
            throwError("Only parameters are supported");

        if(!(objectInspectors[0] instanceof MapObjectInspector))
            throwError("first argument should be Map");
    }
}
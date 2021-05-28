package com.data.navi;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluatorResolver;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;

import static com.data.navi.Util.throwError;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

public class NestedMapReduce extends GenericUDF {
    private MapObjectInspector iMapOI;
    private MapObjectInspector oMapOI;

    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        validateInput(objectInspectors);

        iMapOI = ((MapObjectInspector) objectInspectors[1]);
        oMapOI = ((MapObjectInspector) objectInspectors[2]);

        return getStandardMapObjectInspector(javaIntObjectInspector,
                getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector));
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Integer key  = Integer.parseInt(deferredObjects[0].get().toString());

        Map<Integer, Map<String , Integer>> result = (Map<Integer, Map<String, Integer>>) oMapOI.getMap(deferredObjects[2].get());
        Map<String, Integer> m = (Map<String, Integer>) iMapOI.getMap(deferredObjects[1].get());

        Map<String, Integer> innerMap = result.getOrDefault(key, new HashMap<>());

        for(Map.Entry<String,Integer> e : m.entrySet()) {
            innerMap.compute(e.getKey(), (k, v) -> {
                if (v == null)
                    return e.getValue();
                else {
                    return e.getValue() < v ? e.getValue() : v;
                }
            });
        }

        result.put(key, innerMap);
        return result;

    }

    public String getDisplayString(String[] strings) {
        StringBuilder sb = new StringBuilder();
        sb.append("nested_map_reduce(")
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

        if(!(objectInspectors[0] instanceof IntObjectInspector))
        throwError("first argument should be Int");

        if(!(objectInspectors[1] instanceof MapObjectInspector))
            throwError("second argument should be Map");

        if(!(objectInspectors[2] instanceof MapObjectInspector))
            throwError("third argument should of Map type");
    }
}

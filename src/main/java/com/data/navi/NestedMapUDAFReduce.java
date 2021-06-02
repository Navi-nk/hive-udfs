package com.data.navi;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.HashMap;
import java.util.Map;

import static com.data.navi.Util.throwError;

public class NestedMapUDAFReduce extends UDAF {
    public static class NestedMapUDAFEvaluator implements UDAFEvaluator {
        Map<Integer, Map<String , Integer>> result;

        public NestedMapUDAFEvaluator() {
            super();
            init();
        }

        @Override
        public void init() {
            result = new HashMap<>();
        }

        public boolean iterate(Map<Integer, Map<String , Integer>> row) throws HiveException {
            if(result == null)
                throwError("Result map not initialized");

            return merge(row);
        }

        public Map<Integer, Map<String , Integer>> terminatePartial() {
            return result;
        }

        public boolean merge(Map<Integer, Map<String , Integer>> other) throws HiveException {
            if (other == null) {
                return true;
            }
            for(Map.Entry<Integer, Map<String , Integer>> entry : other.entrySet()) {
                Integer key = entry.getKey();
                Map<String, Integer> value = entry.getValue();

                if (result.containsKey(key)) {
                    result.put(key, mergeMap(Maps.newHashMap(result.get(key)), value));
                } else {
                    result.put(key, value);
                }
            }
            return true;
        }

        /*public boolean merge(Map<Integer, Map<String , Integer>> other) {
            if (other == null) {
                return true;
            }
            other.forEach(
                    (k,v) -> result.merge(k, v, (v1, v2) -> mergeMap(v1, v2))
            );
            return true;
        }
        private Map<String , Integer> mergeMap(Map<String , Integer> v1, Map<String , Integer> v2){
            for(Map.Entry<String,Integer> e : v2.entrySet()) {
                v1.compute(e.getKey(), (k, v) -> {
                    if (v == null)
                        return e.getValue();
                    else {
                        return e.getValue() < v ? e.getValue() : v;
                    }
                });
            }
            return v1;
        }*/

        private Map<String, Integer> mergeMap(Map<String, Integer> map, Map<String, Integer> from) throws HiveException {
            for(Map.Entry<String, Integer> entry : from.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                if (map.containsKey(key)) {
                    Integer v = map.get(key);
                    map.put(key, (value < v) ? value : v);
                } else {
                    map.put(key, value);
                }
            }
            return map;
        }

        public Map<Integer, Map<String , Integer>> terminate(){
            return result;
        }
    }
}

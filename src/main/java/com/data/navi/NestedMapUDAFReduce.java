package com.data.navi;

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

        public boolean iterate(Map<String, Integer> inner, Integer key) throws HiveException {
            if(result == null)
                throwError("Result map not initialized");

            Map<String, Integer> innerMap = result.getOrDefault(key, new HashMap<>());

            for(Map.Entry<String,Integer> e : inner.entrySet()) {
                innerMap.compute(e.getKey(), (k, v) -> {
                    if (v == null)
                        return e.getValue();
                    else {
                        return e.getValue() < v ? e.getValue() : v;
                    }
                });
            }

            result.put(key, innerMap);
            return true;
        }

        public Map<Integer, Map<String , Integer>> terminatePartial() {
            return result;
        }

        public boolean merge(Map<Integer, Map<String , Integer>> other) {
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
        }

        public Map<Integer, Map<String , Integer>> terminate(){
            return result;
        }
    }
}

package com.data.navi;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MapStructReduceTest {

    @Test
    public void testing() {
        Map<String, Integer> map1 = new HashMap<>();
        map1.put("a", 1);
        map1.put("b", 4);
        map1.put("c", 10);

        Map<String, Integer> map2 = new HashMap<>();
        map2.put("a", 2);
        map2.put("b", 1);
        map2.put("d", 2);

        for(Map.Entry<String,Integer> e : map2.entrySet()) {
            map1.compute(e.getKey(),
                    (k, v) -> {
                        if (v == null)
                            return e.getValue();
                        else {
                            return e.getValue() < v ? e.getValue() : v;
                        }
                    });
        }

        System.out.println(map1);
        System.out.println(map2);

    }
}

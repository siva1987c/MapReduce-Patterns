package com.xboxng.topn;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by qiang on 12/28/14.
 */
public class TopNFilter <T extends Comparable<T>, V> {
    private TreeMap<T, V> map = new TreeMap<T, V>();
    private int size;

    public TopNFilter(int N) {
        this.size = N;
    }

    public void put(T key, V value) {
        this.map.put(key, value);
        if (map.size() > size) {
            this.map.remove(map.firstKey());
        }
    }

    public Map<T, V> topN() {
        return Collections.unmodifiableMap(map);
    }
}

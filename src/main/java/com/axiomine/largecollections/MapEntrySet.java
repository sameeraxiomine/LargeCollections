package com.axiomine.largecollections;

/*
 * Copyright 2014 Sameer Wadkar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Function;

public class MapEntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {
    private MapEntryIterator<K, V> iterator = null;
    private Map<K, V> map = null;

    public MapEntrySet(Map map,Function<byte[],? extends K> keyDeSerFunc,Function<byte[],? extends V> valDeSerFunc) {
        this.iterator = new MapEntryIterator(((IDb)map).getDB(),keyDeSerFunc,valDeSerFunc);
        this.map = map;
    }

    @Override
    public Iterator<java.util.Map.Entry<K, V>> iterator() {
        // TODO Auto-generated method stub
        return this.iterator;
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return this.map.size();
    }

}
package com.axiomine.largecollections.util;

import java.util.Iterator;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

import com.google.common.base.Function;


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
public final class MapKeyIterator<K> implements Iterator<K> {

    private DBIterator iter = null;
    private Function<byte[],? extends K> keyDeSerFunc = null;
    protected MapKeyIterator(DB db,Function<byte[],? extends K> keyDeSerFunc) {
        ReadOptions ro = new ReadOptions();
        ro.fillCache(false);
        this.iter =db.iterator(ro);
        this.keyDeSerFunc=keyDeSerFunc;
        this.iter.seekToFirst();
    }

    public boolean hasNext() {
        // TODO Auto-generated method stub
        return this.iter.hasNext();
    }

    public K next() {
        // TODO Auto-generated method stub
        Entry<byte[], byte[]> entry = this.iter.next();
        return this.keyDeSerFunc.apply(entry.getKey());
    }

    public void remove() {
        // TODO Auto-generated method stub
        this.iter.remove();
    }

}
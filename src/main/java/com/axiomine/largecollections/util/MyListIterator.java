package com.axiomine.largecollections.util;
import java.io.Closeable;
import java.io.IOException;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

import com.axiomine.largecollections.serdes.IntegerSerDes;
import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.axiomine.largecollections.serdes.TurboSerializer;
import com.google.common.hash.BloomFilter;
import com.google.common.primitives.Ints;
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
public class MyListIterator<T> implements ListIterator<T>,Closeable {
    private DBIterator iter = null;
    private TurboSerializer<Integer> keySerFunc = new IntegerSerDes.SerFunction();
    private TurboSerializer<T> tSerFunc = null;
    private TurboDeSerializer<T> tDeSerFunc = null;    
    private LargeCollection lColl = null;
    int index = -1;
    int prevIndex = -1;
    int nextIndex = -1;

    protected MyListIterator(LargeCollection coll,TurboSerializer<T> tSerFunc,TurboDeSerializer<T> tDeSerFunc) {

        ReadOptions ro = new ReadOptions();
        ro.fillCache(false);
        this.lColl = coll;
        this.iter =this.lColl.getDB().iterator(ro);
        this.tSerFunc = tSerFunc;
        this.tDeSerFunc=tDeSerFunc;
        this.iter.seekToFirst();
        index++;
        this.managePrevIndex(this.index);
        this.manageNextIndex(this.index);        
    }

    protected MyListIterator(LargeCollection coll,TurboSerializer<T> tSerFunc,TurboDeSerializer<T> tDeSerFunc,int stIndex) {

        ReadOptions ro = new ReadOptions();
        ro.fillCache(false);
        this.lColl = coll;
        this.iter =this.lColl.getDB().iterator(ro);
        this.tSerFunc = tSerFunc;
        this.tDeSerFunc=tDeSerFunc;
        System.out.println(stIndex +"="+this.lColl.size);
        if(stIndex<0 || stIndex>=this.lColl.size){
            throw new IndexOutOfBoundsException ();
        }

        
        if(stIndex==0){
            this.iter.seekToFirst();
        }
        else if(stIndex>=(this.lColl.size-1)){
            this.iter.seekToLast();
            this.index=this.lColl.size;
            this.manageNextIndex(this.index);
            this.managePrevIndex(this.index);
        }
        else{
            for(int i=0;(i <(stIndex));i++){
                if(this.iter.hasNext()){
                    this.iter.next();
                    index++;
                    this.managePrevIndex(this.index);
                    this.manageNextIndex(this.index);
                }
            }
        }
    }

    
    @Override
    public boolean hasNext() {        
        return this.iter.hasNext();
        //System.out.println(this.index + "="+this.lColl.size);
        //return ((this.index)<this.lColl.size);
    }
    
    @Override
    public boolean hasPrevious() {
        return this.iter.hasPrev();
        //return (this.index>0);
    }

    @Override
    public T next() {
        if(!this.iter.hasNext()){
            throw new NoSuchElementException();
        }
        else{            
            Entry<byte[], byte[]> entry = this.iter.next();
            this.index++;
            this.managePrevIndex(this.index);
            this.manageNextIndex(this.index);            
            return this.tDeSerFunc.apply(entry.getValue());
        }
    }

    
    @Override
    public T previous() {
        if(!this.iter.hasPrev()){
            throw new NoSuchElementException();
        }
        else{
            T val = null;
            
            if(this.index==this.lColl.size){                
                val = this.tDeSerFunc.apply(this.lColl.getDB().get(Ints.toByteArray((this.index-1))));
            }
            else{
                val = this.tDeSerFunc.apply(this.iter.prev().getValue());
            }
            
            this.index--;
            
            this.managePrevIndex(this.index);
            this.manageNextIndex(this.index);
            return val;
        }
    }

    private void managePrevIndex(int index){
        if(index<=0){
            this.prevIndex=0;  
        }
        else{
            this.prevIndex = (index-1);
        }            
    }

    private void manageNextIndex(int index){
        if(index>=this.lColl.size){
            this.nextIndex=this.lColl.size;    
        }
        else{
            this.nextIndex = (index + 1);
        }            
    }
    
    @Override
    public int nextIndex() {
        return this.nextIndex;
    }

    @Override
    public int previousIndex() {
        return this.prevIndex;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
        
    }

    @Override
    public void set(T e) {
        if(prevIndex==-1){
            throw new IllegalStateException("Neither getNext nor getPrev has been called");
        }
        byte[] fullValArr = this.tSerFunc.apply(e);
        this.lColl.getDB().put(keySerFunc.apply(this.index-1), fullValArr);
        this.lColl.getBloomFilter().put(e);
        
    }

    @Override
    public void add(T e) {
        throw new UnsupportedOperationException();
        
    }

    @Override
    public void close() throws IOException {
        this.iter.close();
    }
    
}

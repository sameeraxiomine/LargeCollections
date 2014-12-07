package com.axiomine.largecollections;



import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.axiomine.largecollections.IntIntMap;
import com.axiomine.largecollections.utils.FileSerDeUtils;

public class IntIntMapTest {
    public  void writeseq(Map<Integer, Integer> map,int size) {
        long ts = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            if(i%100000==0){
                System.out.println("adding the " + i + "record");
            }
            
            map.put(i,i);

        }

        System.err.println("Time to insert a  " + size + " rows "
                + (System.currentTimeMillis() - ts));
    }
    public  void rewriteseq(Map<Integer, Integer> map,int size,int maxSize) {
        long ts = System.currentTimeMillis();
        Random rnd = new Random();
        for (int i = 0; i < size; i++) {
            int v = rnd.nextInt(maxSize);
            map.put(v,v);
        }

        System.err.println("Time to randomly insert a  " + size + " rows "
                + (System.currentTimeMillis() - ts));
    }
    public  void randomGets(Map<Integer, Integer> map,int size,int maxSize) {
        long ts = System.currentTimeMillis();
        Random rnd = new Random();
        for (int i = 0; i < size; i++) {
            int k = rnd.nextInt(maxSize);
            int v = map.get(k);
        }

        System.err.println("Time to randomly get a  " + size + " rows "
                + (System.currentTimeMillis() - ts));
    }
    
    @Test
    public void test00BasicTest() {
        IntIntMap map  = null;
        try{
            map = new IntIntMap("c:/tmp/",
                    "cacheMap");
            for(int i=0;i<10;i++){
            	map.put(i,i);
            }
            Assert.assertEquals(10, map.size);
        
            map.destroy();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    
    @Test
    public void test01SimpleCreateAndPut() {
        IntIntMap map  = null;
        try{
            map = new IntIntMap("c:/tmp/",
                    "cacheMap");
            writeseq(map,1*2000);
            System.out.println("Size=" + map.size());
            map.remove(0);
            System.out.println("Size=" + map.size());
            
            for(int i=0;i<10;i++){
                Integer v = map.get(i);
                System.out.println("Value=" + v);
            }
            //map.close();
            //FileSerDeUtils.serializeToFile(map,new File("c:/tmp/mymap.ser"));
            map.destroy();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    @Test
    public void test02SerDe() {
        IntIntMap map  = null;
        try{
            File deSerFile = new File("c:/tmp/mymap.ser");
            map = new IntIntMap("c:/tmp/",
                    "cacheMap2");
            writeseq(map,1*1000);
            System.out.println("Size=" + map.size());
            Assert.assertEquals(1000, map.size());
            map.remove(0);
            System.out.println("Size=" + map.size());
            Assert.assertEquals(999, map.size());
            for(int i=0;i<10;i++){
                Integer v = map.get(i);
                System.out.println("Value=" + v);
            }
            //map.close();
            FileSerDeUtils.serializeToFile(map,deSerFile);
            //map.close();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
        try{
            File deSerFile = new File("c:/tmp/mymap.ser");
            map = (IntIntMap) FileSerDeUtils.deserializeFromFile(deSerFile);
            Assert.assertEquals(999, map.size());
            map.remove(1);
            Assert.assertEquals(998, map.size());
            for(int i=0;i<10;i++){
                Integer v = map.get(i);
                System.out.println("Value=" + v);
            }
            map.destroy();
            FileUtils.deleteQuietly(deSerFile);
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void test04TestGetKeySet() {
        IntIntMap map  = null;
        try{
            map = new IntIntMap("c:/tmp/",
                    "cacheMap");
            writeseq(map,1*10);
            for(int i=0;i<10;i++){
                Integer v = map.get(i);
                System.out.println("Value=" + v);
            }
            Set<Integer> st = map.keySet();
            for(Integer i:st){
                System.out.println("Key from set:"+i);
            }
            map.destroy();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    @Test
    public void test05TestGetEntrySet() {
        IntIntMap map  = null;
        try{
            map = new IntIntMap("c:/tmp/",
                    "cacheMap");
            writeseq(map,1*10);
            Set<java.util.Map.Entry<Integer, Integer>> es = map.entrySet();
            for(java.util.Map.Entry<Integer, Integer> i:es){
                System.out.println("Entry:"+i.getKey() +"=" + i.getValue());
            }
            map.destroy();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    
    @Test
    public void test05TestGetValues() {
        IntIntMap map  = null;
        try{
            map = new IntIntMap("c:/tmp/",
                    "cacheMap");
            writeseq(map,1*10);
           
            Collection<Integer> c = map.values();
            Iterator<Integer> iter  = c.iterator();
            map.remove(0);   
            while(iter.hasNext()){
                Integer i = iter.next();
                System.out.println("Coll Val="+i);
            }
            
            c = map.values();
            iter  = c.iterator();
            map.remove(1);
            while(iter.hasNext()){
                Integer i = iter.next();
                System.out.println("Coll Val2="+i);
            }
            map.destroy();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    
    @Test
    public void test06Performance() {
        IntIntMap map  = null;
        try{
            map = new IntIntMap("c:/tmp/",
                    "cacheMap");
            int millions = 1;
            writeseq(map,millions*1000000);
            randomGets(map,100000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            rewriteseq(map,10000,millions*1000000);
            rewriteseq(map,10000,millions*1000000);
            rewriteseq(map,10000,millions*1000000);
            rewriteseq(map,10000,millions*1000000);
            rewriteseq(map,10000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            randomGets(map,100000,millions*1000000);
            map.destroy();
        }
        catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
    

}

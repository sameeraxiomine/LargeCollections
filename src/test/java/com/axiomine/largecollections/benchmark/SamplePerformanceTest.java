package com.axiomine.largecollections.benchmark;

import java.io.File;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.serdes.IntegerSerDes;
import com.axiomine.largecollections.serdes.KryoSerDes;
import com.axiomine.largecollections.serdes.WritableSerDes;
import com.axiomine.largecollections.util.KryoKVMap;
import com.axiomine.largecollections.utilities.KryoUtils;

public class SamplePerformanceTest {
    
  
    
    @Test
    public void primitivePerformanceTest() {
        int size = 1000;
        KryoKVMap<Integer,Integer> map = new KryoKVMap<Integer,Integer>();
        long ts = System.currentTimeMillis();
        for(int i=0;i<size;i++){
            map.put(i, i);
        }
        System.out.println("Time to put " + size + " elements :"+(System.currentTimeMillis()-ts));
        ts = System.currentTimeMillis();
        for(int i=0;i<size;i++){
            int j = map.get(i);
            if(j==1000){
                Assert.assertEquals(i, j);
            }
        }
        System.out.println("Time to sequentially get " + size + " elements :"+(System.currentTimeMillis()-ts));
        int[] x = new int[size];
        Random rnd = new Random();
        for(int i=0;i<size;i++)
            x[i]=rnd.nextInt(size);
            
        
        ts = System.currentTimeMillis();
        for(int i=0;i<size;i++){
            int j = map.get(x[i]);
            if(j==1000){
                Assert.assertEquals(x[i], j);
            }
        }
        System.out.println("Time to randomly get " + size + " elements :"+(System.currentTimeMillis()-ts));
        map.destroy();

        
    }

   
    
}

package com.axiomine.largecollections.functions;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.functions.WritableSerDe;
import com.axiomine.largecollections.utilities.KryoUtils;

public class SerDePerformanceComparisonTest {
    
    @Test
    public void writablePerformancetest() {
        KryoSerDe.SerFunction<IntWritable> ser1 = new KryoSerDe.SerFunction<IntWritable>();        
        KryoSerDe.DeSerFunction<IntWritable> deser1 = new KryoSerDe.DeSerFunction<IntWritable>();

        WritableSerDe.SerFunction ser2 = new WritableSerDe.SerFunction();
        WritableSerDe.DeSerFunction deser2 = new WritableSerDe.DeSerFunction(IntWritable.class);
        WritableSerDe.IntWritableDeSerFunction deser3 = new WritableSerDe.IntWritableDeSerFunction();
        
        long st = System.currentTimeMillis();
        
        long timerSt = 0;
        long totalSer1 = 0;
        long totalSer2 = 0;
        
        long totalDeSer1 = 0;

        long totalDeSer2 = 0;
        long totalDeSer3 = 0;
        
        long stop = System.currentTimeMillis();
        
        System.out.println("Test WritableSerDe.SerFunction");
        for(int i=0;i<1000000;i++){
            IntWritable ii = new IntWritable(0);
            timerSt=System.currentTimeMillis();
            byte[] sba0 = ser1.apply(ii);
            totalSer1=totalSer1+(System.currentTimeMillis()-timerSt);

            
            timerSt=System.currentTimeMillis();
            byte[] sba1 = ser2.apply(ii);
            totalSer2=totalSer2+(System.currentTimeMillis()-timerSt);
            
            timerSt=System.currentTimeMillis();
            IntWritable ss1 = (IntWritable)deser1.apply(sba1);
            totalDeSer1=totalDeSer1+(System.currentTimeMillis()-timerSt);

            
            timerSt=System.currentTimeMillis();
            IntWritable ss2 = (IntWritable)deser2.apply(sba1);
            totalDeSer2=totalDeSer2+(System.currentTimeMillis()-timerSt);
            
            timerSt=System.currentTimeMillis();
            IntWritable ss3 = (IntWritable)deser3.apply(sba1);
            totalDeSer3=totalDeSer3+(System.currentTimeMillis()-timerSt);
        }
        
        System.out.println("Kryo Serialization time="+totalSer1);
        System.out.println("Custom Serialization time="+totalSer2);

        System.out.println("Kryo DeSerialization time     ="+totalDeSer1);
        System.out.println("Custom DeSerialization(1) time="+totalDeSer2);
        System.out.println("Custom DeSerialization(2) time="+totalDeSer2);

        
    }
    
   
    
    
}

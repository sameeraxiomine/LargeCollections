package com.axiomine.largecollections.benchmark;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.serdes.basic.IntegerSerDe;
import com.axiomine.largecollections.serdes.basic.KryoSerDe;
import com.axiomine.largecollections.serdes.basic.WritableSerDe;
import com.axiomine.largecollections.utilities.KryoUtils;

public class PrimitivePerformanceComparisonTest {
    
  
    
    @Test
    public void primitivePerformanceTest() {
        //File root = new File("");
        //File p = new File(root.getAbsolutePath()+"/");
        //System.setProperty(KryoUtils.KRYO_REGISTRATION_PROP_FILE,root.getAbsolutePath()+ "/src/test/resources/KryoRegistrationCustom.properties");

        KryoSerDe.SerFunction<Integer> ser1 = new KryoSerDe.SerFunction<Integer>();        
        KryoSerDe.DeSerFunction<Integer> deser1 = new KryoSerDe.DeSerFunction<Integer>();

        IntegerSerDe.SerFunction ser2 = new IntegerSerDe.SerFunction();
        IntegerSerDe.DeSerFunction deser2 = new IntegerSerDe.DeSerFunction();
        
        long st = System.currentTimeMillis();
        
        long timerSt = 0;
        long totalSer1 = 0;
        long totalSer2 = 0;
        
        long totalDeSer1 = 0;
        long totalDeSer2 = 0;
        
        long stop = System.currentTimeMillis();
        
        System.out.println("Test Integer.SerFunction");
        for(int i=0;i<1000000;i++){
            timerSt=System.currentTimeMillis();
            byte[] sba0 = ser1.apply(new Integer(i));
            totalSer1=totalSer1+(System.currentTimeMillis()-timerSt);

            
            timerSt=System.currentTimeMillis();
            byte[] sba1 = ser2.apply(new Integer(i));
            totalSer2=totalSer2+(System.currentTimeMillis()-timerSt);
            
            timerSt=System.currentTimeMillis();
            try{
                int ss1 = deser1.apply(sba0);    
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
            
            totalDeSer1=totalDeSer1+(System.currentTimeMillis()-timerSt);

            
            timerSt=System.currentTimeMillis();
            int ss2 = deser2.apply(sba1);
            totalDeSer2=totalDeSer2+(System.currentTimeMillis()-timerSt);
            
        }
        
        System.out.println("Custom Integer Kryo Serialization time="+totalSer1);
        System.out.println("Custom Integer Serialization time="+totalSer2);

        System.out.println("Custom Integer Kryo DeSerialization time     ="+totalDeSer1);
        System.out.println("Custom Integer DeSerialization(1) time="+totalDeSer2);

        
    }

    @Test
    public void customKryoPrimitivePerformanceTest() {
        File root = new File("");
        File p = new File(root.getAbsolutePath()+"/");
        System.setProperty(KryoUtils.KRYO_REGISTRATION_PROP_FILE,root.getAbsolutePath()+ "/src/test/resources/KryoRegistrationCustom.properties");

        KryoSerDe.SerFunction<Integer> ser1 = new KryoSerDe.SerFunction<Integer>();        
        KryoSerDe.DeSerFunction<Integer> deser1 = new KryoSerDe.DeSerFunction<Integer>();

        IntegerSerDe.SerFunction ser2 = new IntegerSerDe.SerFunction();
        IntegerSerDe.DeSerFunction deser2 = new IntegerSerDe.DeSerFunction();
        
        long st = System.currentTimeMillis();
        
        long timerSt = 0;
        long totalSer1 = 0;
        long totalSer2 = 0;
        
        long totalDeSer1 = 0;
        long totalDeSer2 = 0;
        
        long stop = System.currentTimeMillis();
        
        System.out.println("Test Custom Kryo Integer.SerFunction");
        for(int i=0;i<1000000;i++){
            timerSt=System.currentTimeMillis();
            byte[] sba0 = ser1.apply(new Integer(i));
            totalSer1=totalSer1+(System.currentTimeMillis()-timerSt);

            
            timerSt=System.currentTimeMillis();
            byte[] sba1 = ser2.apply(new Integer(i));
            totalSer2=totalSer2+(System.currentTimeMillis()-timerSt);
            
            timerSt=System.currentTimeMillis();
            try{
                int ss1 = deser1.apply(sba0);    
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
            
            totalDeSer1=totalDeSer1+(System.currentTimeMillis()-timerSt);

            
            timerSt=System.currentTimeMillis();
            int ss2 = deser2.apply(sba1);
            totalDeSer2=totalDeSer2+(System.currentTimeMillis()-timerSt);
            
        }
        
        System.out.println("Custom Kryo Integer Kryo Serialization time="+totalSer1);
        System.out.println("Custom Kryo Integer Serialization time="+totalSer2);

        System.out.println("Custom Kryo Integer Kryo DeSerialization time     ="+totalDeSer1);
        System.out.println("Custom Kryo Integer DeSerialization(1) time="+totalDeSer2);

        
    }

    
}

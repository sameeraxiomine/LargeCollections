package com.axiomine.largecollections.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;





import com.axiomine.largecollections.utilities.KryoUtils;

public class KryoKWritableVMapBasicTest {
    private String dbPath="";
    
    @Before
    public void setup() throws Exception{
        dbPath = System.getProperty("java.io.tmpdir")+"/test/";
        File f = new File(dbPath);
        if(f.exists()){
            FileUtils.deleteDirectory(f);
        }
    }
    
    @Test
    public void test00BasicTest() {
        File root = new File("");
        File p = new File(root.getAbsolutePath()+"/");
        System.setProperty(KryoUtils.KRYO_REGISTRATION_PROP_FILE,root.getAbsolutePath()+ "/src/test/resources/KryoRegistration.properties");
        KryoKWritableVMap<Integer,IntWritable> map = null;
        try {
            map = new KryoKWritableVMap<Integer,IntWritable>(dbPath, "cacheMap",IntWritable.class);
            Assert.assertTrue(map.isEmpty());
            for (int i = 0; i < 10; i++) {
                IntWritable r = (IntWritable) map.put(i, new IntWritable(i));
                Assert.assertEquals(i,r.get());
            }
            Assert.assertFalse(map.isEmpty());
            Assert.assertEquals(10, map.size());
            
            IntWritable i = (IntWritable) map.remove(0);
            Assert.assertEquals(0, i.get());
            
            Writable nullI = map.remove(0);
            Assert.assertNull(nullI);
            
            Assert.assertTrue(map.containsKey(1));
            Assert.assertFalse(map.containsKey(0));
            
            try {
                map.containsValue(1);
            } catch (Exception ex) {
                boolean b = ex instanceof UnsupportedOperationException;
                Assert.assertTrue(b);
            }
            Assert.assertEquals(9, map.size());
            
            
            
            Map<Integer,IntWritable> m = new HashMap<Integer,IntWritable>();
            m.put(0,new IntWritable(0));
            m.put(1,new IntWritable(11));
            m.put(2,new IntWritable(22));
            map.putAll(m);
            Assert.assertEquals(10, map.size());
            map.close();
            boolean b = false;
            try{
                map.put(0,new IntWritable(0));    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertTrue(b);
            map.destroy();
            
            
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
}

package com.axiomine.largecollections.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;





import com.axiomine.largecollections.utilities.KryoUtils;

public class WritableKKryoVMapBasicTest {
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
        WritableKKryoVMap<IntWritable,Integer> map = null;
        try {
            map = new WritableKKryoVMap<IntWritable,Integer>(dbPath, "cacheMap",IntWritable.class);
            Assert.assertTrue(map.isEmpty());
            for (int i = 0; i < 10; i++) {
                int r = map.put(new IntWritable(i), i);
                Assert.assertEquals(r, i);
            }
            Assert.assertFalse(map.isEmpty());
            Assert.assertEquals(10, map.size());
            
            int i = map.remove(new IntWritable(0));
            Assert.assertEquals(0, i);
            
            Integer nullI = map.remove(new IntWritable(0));
            Assert.assertNull(nullI);
            
            Assert.assertTrue(map.containsKey(new IntWritable(1)));
            Assert.assertFalse(map.containsKey(new IntWritable(0)));
            
            try {
                map.containsValue(1);
            } catch (Exception ex) {
                boolean b = ex instanceof UnsupportedOperationException;
                Assert.assertTrue(b);
            }
            Assert.assertEquals(9, map.size());
            
            
            
            Map<IntWritable,Integer> m = new HashMap<IntWritable,Integer>();
            m.put(new IntWritable(0),0);
            m.put(new IntWritable(1),11);
            m.put(new IntWritable(2),22);
            map.putAll(m);
            Assert.assertEquals(10, map.size());
            map.close();
            boolean b = false;
            try{
                map.put(new IntWritable(0),0);    
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

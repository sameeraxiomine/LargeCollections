package com.axiomine.largecollections.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.axiomine.largecollections.customutil.MyTestMap;

public class MyMapBasicTest {
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
        MyTestMap map = null;
        try {
            map = new MyTestMap(dbPath, "cacheMap");
            Assert.assertTrue(map.isEmpty());
            for (int i = 0; i < 10; i++) {
                int r = map.put(i, i);
                Assert.assertEquals(r, i);
            }
            Assert.assertFalse(map.isEmpty());
            Assert.assertEquals(10, map.size());
            
            int i = map.remove(0);
            Assert.assertEquals(0, i);
            
            Integer nullI = map.remove(0);
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
            
            
            
            Map<Integer,Integer> m = new HashMap<Integer,Integer>();
            m.put(0,0);
            m.put(1,11);
            m.put(2,22);
            map.putAll(m);
            Assert.assertEquals(10, map.size());
            map.close();
            boolean b = false;
            try{
                map.put(0,0);    
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

package samples.com.axiomine.largecollections.util;

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.axiomine.largecollections.serdes.TurboSerializer;
import com.axiomine.largecollections.util.TurboKVMap;
import com.axiomine.largecollections.util.KryoKVMap;
import com.axiomine.largecollections.util.LargeCollection;
import com.axiomine.largecollections.util.WritableSet;
import com.axiomine.largecollections.utilities.FileSerDeUtils;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class WritableSetSample {

    public static void main(String[] args) {
        createWritableSet();
        System.out.println("Create Map overriding the dbName");  
        createWritableSet("KVMAP1");
        System.out.println("Create Map overriding the dbPath,dbName");
        createWritableSet(System.getProperty("java.io.tmpdir"),"KVMAP2");
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB)");
        createWritableSet(System.getProperty("java.io.tmpdir"),"KVMAP3",50);
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createWritableSet(System.getProperty("java.io.tmpdir"),"KVMAP4",50,1000);
        System.out.println("Override the path and name at the time of Deserialization.Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createWritableSetOveridePathAndNameWhileDSer("c:/tmp","KVMAP",50,1000);
    }
    
    public static void printListCharacteristics(LargeCollection m){
        System.out.println("DB Path="+m.getDBPath());
        System.out.println("DB Name="+m.getDBName());
        System.out.println("Cache Size="+m.getCacheSize() + "MB");
        System.out.println("Bloomfilter Size="+m.getBloomFilterSize());
    }
    
    
    public static void workOnWritableSet(WritableSet<IntWritable> lst){
        try {
            
            
            for (int i = 0; i < 10; i++) {
                boolean b = lst.add(new IntWritable(i));
                Assert.assertEquals(true, true);
            }
            System.out.println("Size of map="+lst.size());
            System.out.println("Value for key 0="+lst.contains(new IntWritable(0)));;
            System.out.println("Value for key 5="+lst.contains(new IntWritable(5)));;
            System.out.println("Value for key 9="+lst.contains(new IntWritable(9)));;
            System.out.println("Value for key 11="+lst.contains(new IntWritable(11)));;
            System.out.println("Now remove key 0");
            try{
                boolean b = lst.remove(new IntWritable(0));    
            }
            catch(Exception ex){
                System.out.println(ex.getMessage());
            }
            
            lst.remove(new IntWritable(9));

            System.out.println("Size of map="+lst.size());
            lst.close();
            
            boolean b = false;
            try{
                lst.add(new IntWritable(9));    
            }
            catch(Exception ex){
                System.out.println("Exception because acces after close");
                b=true;
            }
            lst.open();
            System.out.println("Open again");
            b = lst.add(new IntWritable(9));            
            
            Assert.assertEquals(true, b);
            lst.add(new IntWritable(9));
            lst.add(new IntWritable(5));
            System.out.println("Now put worked. Size of map should be 10. Size of the map ="+lst.size());

            Iterator<Writable> iter = lst.iterator(); 
            try{
                while(iter.hasNext()){
                    int i = ((IntWritable)iter.next()).get();
                    System.out.println("From ITerator = "+i);
                }
            }
            finally{
                //Always close and iterator after use. Otherwise you will not be able to call the clear function
                ((Closeable)iter).close();    
            }
            
            
            
            System.out.println("Now Serialize the List");
            File serFile = new File(System.getProperty("java.io.tmpdir")+"/x.ser");
            FileSerDeUtils.serializeToFile(lst,serFile);            
            System.out.println("Now De-Serialize the List");
            lst = (WritableSet<IntWritable>) FileSerDeUtils.deserializeFromFile(serFile);
            System.out.println("After De-Serialization "+lst);
            System.out.println("After De-Serialization Size of map should be 10. Size of the map ="+lst.size());
            printListCharacteristics(lst);
            System.out.println("Now calling lst.clear()");
            lst.clear();
            System.out.println("After clear list size should be 0 and ="+lst.size());
            lst.add(new IntWritable(0));    
            System.out.println("Just added a record and lst size ="+lst.size());
            System.out.println("Finally Destroying");
            lst.destroy();
            
            System.out.println("Cleanup serialized file");
            FileUtils.deleteQuietly(serFile);
            
            
        } catch (Exception ex) {            
            throw Throwables.propagate(ex);
        }
        
    }
    public static void createWritableSet(){        
        WritableSet<IntWritable> lst = new WritableSet<IntWritable>(IntWritable.class);
        printListCharacteristics(lst);
        workOnWritableSet(lst);
    }

    public static void createWritableSet(String dbName){
        WritableSet<IntWritable> lst = new WritableSet<IntWritable>(dbName,IntWritable.class);
        printListCharacteristics(lst);
        workOnWritableSet(lst);
    }

    public static void createWritableSet(String dbPath,String dbName){
        WritableSet<IntWritable> lst = new WritableSet<IntWritable>(dbPath,dbName,IntWritable.class);
        printListCharacteristics(lst);
        workOnWritableSet(lst);
    }

    public static void createWritableSet(String dbPath,String dbName,int cacheSize){
        WritableSet<IntWritable> lst = new WritableSet<IntWritable>(dbPath,dbName,cacheSize,IntWritable.class);
        printListCharacteristics(lst);
        workOnWritableSet(lst);
    }

    public static void createWritableSet(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        WritableSet<IntWritable> lst = new WritableSet<IntWritable>(dbPath,dbName,cacheSize,bloomfilterSize,IntWritable.class);
        printListCharacteristics(lst);
        workOnWritableSet(lst);
    }

    public static void createWritableSetOveridePathAndNameWhileDSer(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        System.setProperty(LargeCollection.OVERRIDE_DB_PATH, dbPath);        
        System.setProperty(LargeCollection.OVERRIDE_DB_NAME, dbName);
        WritableSet<IntWritable> lst = new WritableSet<IntWritable>(dbPath,dbName,cacheSize,bloomfilterSize,IntWritable.class);
        printListCharacteristics(lst);
        workOnWritableSet(lst);
    }

}

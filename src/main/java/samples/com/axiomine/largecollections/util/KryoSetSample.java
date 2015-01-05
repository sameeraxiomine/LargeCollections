package samples.com.axiomine.largecollections.util;

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;

import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.axiomine.largecollections.serdes.TurboSerializer;
import com.axiomine.largecollections.util.KryoSet;
import com.axiomine.largecollections.util.LargeCollection;
import com.axiomine.largecollections.utilities.FileSerDeUtils;
import com.google.common.base.Throwables;

public class KryoSetSample {
    public static TurboSerializer<Integer> TSERIALIZER = new com.axiomine.largecollections.serdes.IntegerSerDes.SerFunction();
    public static TurboDeSerializer<Integer>  TDESERIALIZER = new com.axiomine.largecollections.serdes.IntegerSerDes.DeSerFunction();

    public static void main(String[] args) {
        createKryoSet();
        System.out.println("Create Map overriding the dbName");  
        createKryoSet("KVMAP1");
        System.out.println("Create Map overriding the dbPath,dbName");
        createKryoSet(System.getProperty("java.io.tmpdir"),"KVMAP2");
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB)");
        createKryoSet(System.getProperty("java.io.tmpdir"),"KVMAP3",50);
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createKryoSet(System.getProperty("java.io.tmpdir"),"KVMAP4",50,1000);
        System.out.println("Override the path and name at the time of Deserialization.Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createKryoSetOveridePathAndNameWhileDSer("c:/tmp","KVMAP",50,1000);
    }
    
    public static void printListCharacteristics(LargeCollection m){
        System.out.println("DB Path="+m.getDBPath());
        System.out.println("DB Name="+m.getDBName());
        System.out.println("Cache Size="+m.getCacheSize() + "MB");
        System.out.println("Bloomfilter Size="+m.getBloomFilterSize());
    }
    
    
    public static void workOnKryoSet(KryoSet<Integer> lst){
        try {
            
            
            for (int i = 0; i < 10; i++) {
                boolean b = lst.add(i);
                Assert.assertEquals(true, true);
            }
            System.out.println("Size of map="+lst.size());
            System.out.println("Value for key 0="+lst.contains(0));;
            System.out.println("Value for key 5="+lst.contains(5));;
            System.out.println("Value for key 9="+lst.contains(9));;
            System.out.println("Value for key 11="+lst.contains(11));;
            System.out.println("Now remove key 0");
            try{
                boolean b = lst.remove(0);    
            }
            catch(Exception ex){
                System.out.println(ex.getMessage());
            }
            
            lst.remove(9);

            System.out.println("Size of map="+lst.size());
            lst.close();
            
            boolean b = false;
            try{
                lst.add(9);    
            }
            catch(Exception ex){
                System.out.println("Exception because acces after close");
                b=true;
            }
            lst.open();
            System.out.println("Open again");
            b = lst.add(9);            
            
            Assert.assertEquals(true, b);
            lst.add(9);
            lst.add(5);
            System.out.println("Now put worked. Size of map should be 10. Size of the map ="+lst.size());

            Iterator<Integer> iter = lst.iterator(); 
            try{
                while(iter.hasNext()){
                    int i = iter.next();
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
            lst = (KryoSet<Integer>) FileSerDeUtils.deserializeFromFile(serFile);
            System.out.println("After De-Serialization "+lst);
            System.out.println("After De-Serialization Size of map should be 10. Size of the map ="+lst.size());
            printListCharacteristics(lst);
            System.out.println("Now calling lst.clear()");
            lst.clear();
            System.out.println("After clear list size should be 0 and ="+lst.size());
            lst.add(0);    
            System.out.println("Just added a record and lst size ="+lst.size());
            System.out.println("Finally Destroying");
            lst.destroy();
            
            System.out.println("Cleanup serialized file");
            FileUtils.deleteQuietly(serFile);
            
            
        } catch (Exception ex) {            
            throw Throwables.propagate(ex);
        }
        
    }
    public static void createKryoSet(){        
        KryoSet<Integer> lst = new KryoSet<Integer>();
        printListCharacteristics(lst);
        workOnKryoSet(lst);
    }

    public static void createKryoSet(String dbName){
        KryoSet<Integer> lst = new KryoSet<Integer>(dbName);
        printListCharacteristics(lst);
        workOnKryoSet(lst);
    }

    public static void createKryoSet(String dbPath,String dbName){
        KryoSet<Integer> lst = new KryoSet<Integer>(dbPath,dbName);
        printListCharacteristics(lst);
        workOnKryoSet(lst);
    }

    public static void createKryoSet(String dbPath,String dbName,int cacheSize){
        KryoSet<Integer> lst = new KryoSet<Integer>(dbPath,dbName,cacheSize);
        printListCharacteristics(lst);
        workOnKryoSet(lst);
    }

    public static void createKryoSet(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        KryoSet<Integer> lst = new KryoSet<Integer>(dbPath,dbName,cacheSize,bloomfilterSize);
        printListCharacteristics(lst);
        workOnKryoSet(lst);
    }

    public static void createKryoSetOveridePathAndNameWhileDSer(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        System.setProperty(LargeCollection.OVERRIDE_DB_PATH, dbPath);        
        System.setProperty(LargeCollection.OVERRIDE_DB_NAME, dbName);
        KryoSet<Integer> lst = new KryoSet<Integer>(dbPath,dbName,cacheSize,bloomfilterSize);
        printListCharacteristics(lst);
        workOnKryoSet(lst);
    }

}

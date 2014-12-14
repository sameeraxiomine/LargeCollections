package com.axiomine.largecollections.util;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.axiomine.largecollections.utilities.KryoUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public abstract class LargeCollection implements IDb  {
    public static final Random rnd = new Random();
    public static int DEFAULT_CACHE_SIZE = 25;
    public static int DEFAULT_BLOOM_FILTER_SIZE = 10000000;
    protected String dbPath=System.getProperty("java.io.tmpdir");
    protected String dbName = "f"+System.currentTimeMillis()+rnd.nextInt();
    protected int cacheSize = DEFAULT_CACHE_SIZE;
    protected long size;
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;

    //private SerializationUtils<K, V> serdeUtils = new SerializationUtils<K, V>();
    protected int bloomFilterSize = DEFAULT_BLOOM_FILTER_SIZE;
    protected  transient Funnel<Integer> myFunnel = null;
    protected BloomFilter bloomFilter = null;
    protected transient boolean opened=false;
    //protected transient Kryo kryo = null;
    //protected boolean kryoUsed = false;

    protected ThreadLocal<Kryo> kryos = null;
    
    
    public ThreadLocal<Kryo> getThreadLocalKryos(){
        if(kryos==null){
            kryos = KryoUtils.getThreadLocalKryos();
            this.configureKryoSerializers();
        }
        return  this.kryos;
    }
    
    public void configureKryoSerializers(){
       Kryo kryo = this.getThreadLocalKryos().get();
       //Configure Kryo classes
    }
    
    public Kryo getKryo(){
        return this.getThreadLocalKryos().get();
    }
    
    protected void initializeBloomFilter(){
        this.myFunnel = new Funnel() {
            public void funnel(Object obj, PrimitiveSink into) {
                into.putInt(obj.hashCode());                  
              }
            };  
        this.bloomFilter = BloomFilter.create(myFunnel, this.bloomFilterSize);
    }
    

    public LargeCollection(){        
        this.open();
    }

    public LargeCollection(String dbName){
        this.dbName=dbName;
        this.open();

    }
    public LargeCollection(String dbPath,String dbName){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.open();
    }

    public LargeCollection(String dbPath,String dbName,int cacheSize){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.cacheSize = cacheSize;
        this.open();
    }
    
    public LargeCollection(String dbPath,String dbName, int cacheSize,int bloomFilterSize){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.cacheSize = cacheSize;
        this.bloomFilterSize = bloomFilterSize;
        this.initialize();
        this.initializeBloomFilter();   
    }

    
    protected void registerKryoClasses(){
        //Default is to do nothing. Override this class 
    }

    protected void initialize(){
        this.initialize(true);
    }
    
    protected void initialize(boolean exceptionOnExistingFolder){
        try {
            dbFile = new File(this.dbPath+File.separator+this.dbName);
            options = new Options();
            options.cacheSize(cacheSize * 1048576);
            options.compressionType(CompressionType.SNAPPY);            
            if(!dbFile.exists()){
                dbFile.mkdirs();
            }
            else{
                if(exceptionOnExistingFolder){
                    throw new RuntimeException(dbFile.getAbsolutePath() + " already exists");
                }
            }
            db = factory.open(dbFile,options);       
            this.opened = true;
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }
    
    public String getDBPath(){
        return this.dbPath;
    }
    
    protected void serialize(java.io.ObjectOutputStream stream)
            throws IOException {
        System.out.println("Now serializing " + this.dbName);
        stream.writeObject(this.dbPath);
        stream.writeObject(this.dbName);
        stream.writeInt(this.cacheSize);
        stream.writeLong(this.size);
        stream.writeInt(this.bloomFilterSize);
        stream.writeObject(this.bloomFilter);
        this.db.close();

    }

    protected void deserialize(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.dbPath = (String) in.readObject();
        this.dbName = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.size = in.readLong();
        this.bloomFilterSize = in.readInt();
        this.bloomFilter = (BloomFilter<Integer>) in.readObject();

        this.initialize(false);
        System.out.println("Now deserialized " + this.dbName);
    }
    
    public DB getDB() {
        return this.db;
    }
    
    public long getLSize(){
        return size;
    }
    /* Destroys the map */
    
    @Override
    public void close() throws IOException {
        try {
            this.initializeBloomFilter();
            this.db.close();
            //factory.destroy(this.dbFile, this.options);
            this.db=null;//The map should be unusable after close. Must call open first
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
    @Override
    public void destroy() {
        try {
            if(this.db==null){
                this.open();
            }
            this.initializeBloomFilter();
            this.db.close();
            factory.destroy(this.dbFile, this.options);
            this.db=null;//The map should be unusable after close
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
    
    @Override
    public void open() {
        try {
            if(!opened){
                this.initialize();    
            }
            else{
                this.initialize(false);
            }
            
            this.initializeBloomFilter();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public void clearDB(){
        try{
            if(this.db!=null){
                this.db.close();
                FileUtils.deleteQuietly(this.dbFile);
            }
            this.size=0;
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
        this.initialize();
        this.initializeBloomFilter();

    }
    
    
    
    public abstract void optimize();
}

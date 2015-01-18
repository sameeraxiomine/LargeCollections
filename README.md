# LargeCollections #

LargeCollections supports java.util.Map, java.util.List and java.util.Set implementations which are backed by LevelDB. This allows your collections to grow very large as it does not use the JVM heap memory.

The [Javadoc](http://sameeraxiomine.github.io/largecollections/javadocs "LargeCollections JavaDoc") for this project should be browsed for more information.

#Key Design Principles#
The underlying java.util.Map(java.util.List and java.util.Set) implementations are backed by [https://github.com/google/leveldb](https://github.com/google/leveldb "LevelDB"). LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from byte array keys to byte array values.

While LargeCollections supports any Serializable/Externalizable/Writable/Kryo-Serializable Key and Value classes, the underlying implementations stores everything as byte-array key value store (similar to HBase). Thus every key/value instance needs to be converted to a byte array to write to the LevelDB backing store and convert back from byte array to a Java instance.

To support the conversions from object to byte-array and back, every java.util.Map sub-class provided by LargeCollections library needs to have Serializer-Deserializer (SerDes) pair, one each for Key and Value class. 

These SerDes pair implement the following standard Interfaces

1. `com.axiomine.largecollections.serdes.TurboSerializer<T>` 
2. `com.axiomine.largecollections.serdes.TurboDeSerializer<T>` 


# When should you use LargeCollections #

You should use LargeCollections in the following situations

1. The size of your Map is larger than over 100-200 MB. It is well known fact about Java that despite the availability of Heap Memory, the JVM gets slower as the size of objects gets larger. The situation gets worse when these objects are long-lived which is typical of caches as major  GC is initiated more often.

2. The size of your Key-Value store is not so large as to justify a NoSQL store like HBase or Cassandra

3. You would like to use large Caches which support the java.util.Map API.

You you find yourself wishing you could use an in-process MongoDB instance, you should consider using LargeCollections. It is designed to be an in-process persistent Map which allows you to store a few million entries.


# Usage #
The best way to learn how to use the API is to follow the samples contained in the `samples` package. Alternatively the comprehensive list of unit test cases also demonstrate how to use the library.

The main thing to remember when using this library is, it supports the java.util.Map interface. The only method not supported (for performance reason) is  `public boolean containsValue(Object value)`

For examples of how to use the library classes in the `com.axiomine.largecollections.util` package see the source code for the classes below

    samples.com.axiomine.largecollections.util.KryoKVMapSample
    samples.com.axiomine.largecollections.util.TurboKVMapSample
    samples.com.axiomine.largecollections.util.WritableKVMapSample
	samples.com.axiomine.largecollections.util.KryoKTurboVMapSample
    samples.com.axiomine.largecollections.util.TurboKKryoVMapSample
	samples.com.axiomine.largecollections.util.KryoKWritableVMapSample
    samples.com.axiomine.largecollections.util.WritableKKryoVMapSample
	samples.com.axiomine.largecollections.util.TurboKWritableVMapSample
    samples.com.axiomine.largecollections.util.WritableKTurboVMapSample

    samples.com.axiomine.largecollections.util.TurboSetSample
	samples.com.axiomine.largecollections.util.KryoSetSample
	samples.com.axiomine.largecollections.util.WritableSetSample

    samples.com.axiomine.largecollections.util.TurboListSample
	samples.com.axiomine.largecollections.util.KryoListSample
	samples.com.axiomine.largecollections.util.WritableListSample


For examples of how to use the library classes in the `com.axiomine.largecollections.turboutil` package see the source code for the classes below

    samples.com.axiomine.largecollections.turboutil.IntegerIntegerMapSample
    samples.com.axiomine.largecollections.turboutil.IntegerIntWritableMapSample
	samples.com.axiomine.largecollections.turboutil.IntWritableIntegerMapSample
    samples.com.axiomine.largecollections.turboutil.IntegerKryoVMapSample
	samples.com.axiomine.largecollections.turboutil.KryoKIntegerMapSample
	samples.com.axiomine.largecollections.turboutil.IntegerKryoVMapSample

The classes in `com.axiomine.largecollections.turboutil` are helper classes provided to handle primitive (almost) types as follows
    Integer
    Long
    Double
    Float
    Byte
    BytesArray(byte[])
    Character
    String
     
The classes in `com.axiomine.largecollections.turboutil` are helper classes provided to handle the following Writable types
    IntWritable
    LongWritable
    FloatWritable
    DoubleWritable
    Text
	BooleanWritable
	ArrayPrimitiveWritable
	BytesWritable
	ShortWritable
	MapWritable

The classes in `com.axiomine.largecollections.turboutil` also support combinations of all of the above classes with each other as well as templatized when K/V use Kryo based Serialization, Examples are -

     KryoKIntegerMap 
     IntegerKryoVMap
     IntWritableIntegerMap 
     IntegerIntWritableMap
     IntWritableKryoVMap
     KryoKIntWritableMap


----------
    
## Some points to remember ##

>`java.util.Map` implementations are complete. Most of the methods from the >java.util.Map are implemented with the exception of `containsValue` which will >through an exception if invoked.

>`java.util.Set` implementations are complete. Most of the methods from the >java.util.Set are implemented with the exception of `toArray` methods which will >through an exception if invoked.

>`java.util.List` implementations have significant number of caveats. These are 

> 1.  List implementation for the purpose of "write once and read many" semantics. 
> 2.  You can update values for a specific index but you cannot insert values at a >specified index using the `public void add(int index, T element)`
> 3.  You cannot remove from the list using the `remove` method.The list of methods >not supported are
 
    	 	public Object[] toArray()
    	 	public <TT[] toArray(T[] a) 
    	 	public boolean remove(Object o)
    	 	public boolean addAll(int index, Collection<? extends Tc) 
    	 	public boolean retainAll(Collection<?c)
    	 	public void add(int index, T element)
    	 	public List<TsubList(int fromIndex, int toIndex)
	 	
>4.The other key limitation is in the implementation of the `contains` and >`containsAll` methods. Both of them are implemented using bloom filters. Hence they >are heuristic in nature and their actual implementation follow the semantics of >`mightContain` or `mightContainsAll` implementation. When using List if you expect >to invoke those methods frequently provide larger than expected values for >collection properties `bloomFilterSize` and provide a low value for the System >property `override.bf.fpp` such as 0.01(See the next section on Collection >Attributes). Doing this will increase the in-memory size of your Lists but will >increase the accuracy of `contains` and `containsAll` invocations.     

----------


## Collection Attributes ##
We provide implementations of `java.util.Map`, `java.util.List` and `java.util.Set`. Each of these Map implementations has the following attributes


1. `dbPath` - This is a folder where the LevelDB named cache is created. Default value is the folder provided by the java System property `java.io.tmpdir`. However it can be overridden by providing a path in a custom System Property - `override.dbpath`: for example, you can pass this in the command line as follows `-Doverride.dbpath="c:/mydb/"`

2. `dbName` - The name of LevelDB Cache. Each Map should have a unique LevelDB backing database. The default value of this attribute is a value which is randomly generated. You can override it with your own user-defined name

3. `cacheSize` - LevelDB uses an internal cache to improve its response time. This parameter is specified in MB. Its default value is `25`.

4. `bloomFilterSize` - Internally we use bloom filter to make swift determination of if a key is contained in the underlying LevelDB database. BloomFilters need to know at the outset the approximate number of elements they will contain. Underestimating this size will lead to more false positives and consequently degrade your performance as the LevelDB will be checked for a key even when the key does not exist. Default value is `10000000` (10 million). By default the bloom filter supports 3% for false positives. This default cannot be overridden. It can be overridden at the JVM level by configuring the system property `override.bf.fpp`: for example, you can pass this in the command line as follows `-Doverride.bf.fpp="0.01"`
 
## Collection Constructors ##
The constructors supported for each of the Map's are as follows-

1. () - All default values Map attributes mentioned above. Note that the dbName is a randomly generated attribute for each map. The `dbPath` can be overridden via the system property `override.dbpath`. If the `override.dbpath` is not provided the name of the value of the  `dbPath` defaults to `java.io.tmpdir`. The other attributes take on their default values.

2. (dbName) - Override the dbName attribute above. Other attributes take on default values

3. (dbPath,dbName) - Override the dbPath and dbName attributes

4. (dbPath,dbName,cacheSize) - Override the dbPath,dbName,cacheSize attributes

5. (dbPath,dbName,cacheSize,bloomFilterSize) - Override the dbPath,dbName,cacheSize,bloomFilterSize attributes

LargeCollections provides a Map implementations based on how the SerDes classes are provided for K and V class types-

1. Turbo SerDes are highly custom SerDes for primitive (mostly) types which are designed to be very efficient. The following turbo serdes classes are provided
	- 	`com.axiomine.largecollections.serdes.IntegerSerdes` 
	- 	`com.axiomine.largecollections.serdes.LongSerdes 	`
	- 	`com.axiomine.largecollections.serdes.FloatSerdes`
	- 	`com.axiomine.largecollections.serdes.DoubleSerdes`
	- 	`com.axiomine.largecollections.serdes.StringSerdes`
	- 	`com.axiomine.largecollections.serdes.CharacterSerdes`
	- 	`com.axiomine.largecollections.serdes.ByteSerdes`
	- 	`com.axiomine.largecollections.serdes.ByteArraySerdes`
 
2. `com.axiomine.largecollections.serdes.KryoSerdes` class utilizes [Kryo](https://github.com/EsotericSoftware/kryo/ "Kryo") for Serialization and Deserialization. More details on how to register custom Kryo Serializers will be discussed in the next sections.
3. `com.axiomine.largecollections.serdes.WritableSerdes` class provides SerDes for `org.hadoop.io.Writable` implementations. There is only one Serialization implementation and several deserialization implementations. Below are the constructors to various Deserialization implementations
	- 	`public DeSerFunction(Class<? extends Writable> wCls)`  - Utilize this you are using your own custom `Writable` implementation.
	- 	`public ArrayPrimitiveWritableDeSerFunction()` - Utilize this when your Key or Value is of type `ArrayPrimitiveWritable`
	- 	`public BooleanWritableDeSerFunction()` - Utilize this when your Key or Value is of type `BooleanWritable`
	- 	`public BytesWritableDeSerFunction()` - Utilize this when your Key or Value is of type `BytesWritable` 
	- 	`public DoubleWritableDeSerFunction()` - Utilize this when your Key or Value is of type `DoubleWritable` 
	- 	`public FloatWritableDeSerFunction()` - Utilize this when your Key or Value is of type `FloatWritable` 
	- 	`public LongWritableDeSerFunction()` - Utilize this when your Key or Value is of type `LongWritable` 
	- 	`public MapWritableDeSerFunction()` - Utilize this when your Key or Value is of type `MapWritable` 
	- 	`public ShortWritableDeSerFunction()` - Utilize this when your Key or Value is of type `ShortWritable` 
	- 	`public TextDeSerFunction()` - Utilize this when your Key or Value is of type `Text` 
For each of the above `Writable` implementation classes, Kryo Serializers have been provided. 

## Map Types Supported ##
There 8 main types of Maps provided are -

1. TurboKVMap<K,V> - This implementation of `java.util.Map` expects you to provide SerDes classes for key and value classes used with this map. Each of the above constructor will take four more trailing parameters

	- 	kSerClass This is a Serializer class used to serialize K instance. For example `com.axiomine.largecollections.serdes.IntegerSerDes$SerFunction` if K is of type `java.lang.Integer`
	- 	vSerClass This is a Serializer class used to serialize V instance. For example `com.axiomine.largecollections.serdes.IntegerSerDes$SerFunction` if V is of type `java.lang.Integer`
	- 	kDeSerClass This is a DeSerializer class used to deserialize K instance. For example `com.axiomine.largecollections.serdes.IntegerSerDes$DeSerFunction` if K is of type `java.lang.Integer`
	- 	vDeSerClass This is a DeSerializer class used to deserialize V instance. For example `com.axiomine.largecollections.serdes.IntegerSerDes$DeSerFunction` if V is of type `java.lang.Integer`

	For examples of how to use this class see `samples.com.axiomine.largecollections.util.TurboKVMapSample.java`

    TurboSerializer<Integer> KSERIALIZER = new com.axiomine.largecollections.serdes.IntegerSerDes.SerFunction();
    TurboSerializer<Integer> VSERIALIZER = new com.axiomine.largecollections.serdes.IntegerSerDes.SerFunction();
    TurboDeSerializer<Integer> KDESERIALIZER =new com.axiomine.largecollections.serdes.IntegerSerDes.DeSerFunction();
    TurboDeSerializer<Integer> VDESERIALIZER =new com.axiomine.largecollections.serdes.IntegerSerDes.DeSerFunction();
    java.util.Map<Integer,Integer> map =   new FastKVMap<Integer,Integer>(KSERIALIZER,VSERIALIZER,KDESERIALIZER,VDESERIALIZER);    					
 	//Use it like a regular java.util.Map`
        
2.  KryoKVMap<K,V> - This implementation of `java.util.Map` utilizes Kryo for high performance serialization and deserialization. For all the standard primitive types Kryo provides default serializers. LargeCollections also provides Kryo Serializers for the following standard org.hadoop.io.Writable implementations in the package `com.axiomine.largecollections.kryo.serializers`
	- 	`ArrayPrimitiveWritable` 
	- 	`BooleanWritable`
	- 	`BytesWritable`
	- 	`ByteWritable`
	- 	`DoubleWritable`
	- 	`FloatWritable`
	- 	`IntWritable`
	- 	`LongWritable`
	- 	`MapWritable`
	- 	`ShortWritable`
	- 	`Text`	

	KryoKVMap can be used where TurboKVMap is used for standard types mentioned in the TurboSerDes described above. However using the TurboKVMap will perform significantly faster if you are working with the primitive types. 
	You should review the classes in the package `com.axiomine.largecollections.kryo.serializers` for examples of how to write your own KryoSerializers. The Kryo documentation is the best resource on how to write your own Kryo Serializers
	
	You do have to register your own KryoSerializers. For example `com.axiomine.largecollections.kryo.serializers.MyIntSerializer` is an example of the custom Kryo serializer. If you want to register your own serializer you should create a property file and make entries as follows

		             	java.lang.Integer=com.axiomine.largecollections.kryo.serializers.MyIntSerializer
    	java.lang.Float=com.axiomine.largecollections.kryo.serializers.MyFloatSerializer   	
	
	An example file is in location `src/test/resources/KryoRegistration.properties`. See the following test case to `com.axiomine.largecollections.util.KryoKTurboVMapBasicTest` for example on how to register custom Kryo serializers. The above mentioned Property file must be passed to the JVM using the following System Property
	
	    -DKRYO_REG_PROP_FILE=${PATH_TO_KRYO_PROPS}/KryoRegistration.properties

3. WritableKVMap<K extends Writable,V extends Writable> - Use this Map implementation when you have a highly custom Writable K and V classes and you do not wish to create Kryo Serializer. If you Writable classes are one of those mentioned earlier then using the KryoKVMap should be adequate. All the standard constructors take two trailing parameters 
	- 	`Class<K extends Writable>` Ex. IntWritable.class
	- 	`Class<V extends Writable>` Ex. Text.class

	For the above mentioned examples the WritableKVMap will be instantiated as follows
    `Map<IntWritable,Text> m = new WritableKVMap<IntWritable,Text>(IntWritable.class,Text.class)`

	Similar to the TurboKVMap helper Map implementations are provided for the following Writable types in the package `com.axiomine.largecollections.turboutil`

	- 	`ArrayPrimitiveWritable` 
	- 	`BooleanWritable`
	- 	`BytesWritable`
	- 	`ByteWritable`
	- 	`DoubleWritable`
	- 	`FloatWritable`
	- 	`IntWritable`
	- 	`LongWritable`
	- 	`MapWritable`
	- 	`ShortWritable`
	- 	`Text`	
	
	Examples of such classes in the `com.axiomine.largecollections.turboutil` package are `IntWritableIntWritableMap`, `IntWritableTextMap`, etc.
    
4. KryoKTurboVMap<K,V>  - This is implementation where the SerDes used for the Key is a Kryo SerDes and the one used for Value is a custom SerDes.the package `com.axiomine.largecollections.turboutil` has various pre-created versions of this Map. Examples are `KryoKIntegerMap`, `KryoKStringMap`, etc.

5.	TurboKKryoV<K,V> - This is implementation where the SerDes used for the Key is a custom SerDes and the one used for Value is a Kryo SerDes.the package `com.axiomine.largecollections.turboutil` has various pre-created versions of this Map. Examples are `IntegerKryoVMap`, `StringKryoVMap`, etc.

6.	TurboKWritableV<K,V extends Writable> - This is implementation where the SerDes used for the Key is a custom SerDes and the one used for Value is a Writable SerDes.the package `com.axiomine.largecollections.turboutil` has various pre-created versions of this Map. Examples are `IntegerIntWritableMap`, `IntegerIntWritableMap`, etc.

7.	WritableKTurboV<K extends Writable,V> - This is implementation where the SerDes used for the Value is a custom SerDes and the one used for Key is a Writable SerDes.the package `com.axiomine.largecollections.turboutil` has various pre-created versions of this Map. Examples are `IntWritableIntegerMap`, `IntWritableStringMap`, etc.

7.	WritableKKryoV<K extends Writable,V> - This is implementation where the SerDes used for the Value is a Kryo SerDes and the one used for Key is a Writable SerDes.the package `com.axiomine.largecollections.turboutil` has various pre-created versions of this Map. Examples are `IntWritableKryoVMap`, `TextKryoVMap`, etc.

8.	KryoKWritableV<K,V extends Writable> - This is implementation where the SerDes used for the Key is a Kryo SerDes and the one used for Value is a Writable SerDes.the package `com.axiomine.largecollections.turboutil` has various pre-created versions of this Map. Examples are `KryoKIntWritableMap`, `KryoKTextMap`, etc.

## List and Set Types Supported ##
Similar to Map, the following List and Set classes are supported

1.  TurboList<E>
2.  KryoList<E>
3.  TurboSet<E>
4.  KryoSet<E>


##Serializing the LargeCollection Map to disk##

The Map implementations can be serialized like any other java.util.Map instance. Only the meta-data is serialized. The actual data is stored in the LevelDB instance which is not serialized. When the Map instance is deserialized, only the underlying meta-data is deserialized which effectively leads to the correct LevelDB database being pointed to. 
    
    TurboKVMap<Integer,Integer> map = /* initialized the map */
    FileSerDeUtils.serializeToFile(map,new File("c:/tmp/map.ser"));
    map = (TurboKVMap<Integer,Integer>) FileSerDeUtils.deserializeFromFile(new File("c:/tmp/map.ser"));

It is possible to change the path of the underlying LevelDB database during De-Serialization. This can happen when you are porting your serialized Map and the associated LevelDB database (which nothing but a folder and some files on your disk). In order to indicate to the deserialization process the new path to the LevelDB database you need to configure the following System Properties

    /*
     LargeCollection.OVERRIDE_DB_PATH="override.dbpath" overrides the base folder for the LevelDB Database
    */
    System.setProperty(LargeCollection.OVERRIDE_DB_PATH, dbPath);
    /*
     LargeCollection.OVERRIDE_DB_PATH="override.dbname" overrides the name of the sub-folder contained in the override.dbpath which is the name of the LevelDB 
     database. If you only change the root folder of the LevelDB database but left its name unchanged, you do not have to provide this property value.
    */
    System.setProperty(LargeCollection.OVERRIDE_DB_NAME, dbName);
    map = (TurboKVMap<Integer,Integer>) FileSerDeUtils.deserializeFromFile(new File("c:/tmp/map.ser"));



#SerDes#

It was important to select the fasted possible serialization/deserialization mechanisms. The default java serialization is supported but we have done considerably better. Below is a list of various mechanisms used for SerDes-

1. Kryo - This offers better control over serialization. Also the interface is considerably simpler and non-intrusive from the Key/Value class design. It support the default java serialization via the Serializable and Externalizable interface

2. Custom SerDes for primitive types - Where possible we will utilize highly customized mechanisms. For example, it is lot simpler (an faster) to invoke `String.getBytes()` and new String(byte[]) to serialize and deserialize strings. 

3. For basic `org.apache.hadoop.io.Writable` types we provide very fast Kryo SerDes

We will discuss all the above methods and how to utilize them in detail in the subsequent sections.  


##Out of box SerDes##

We provide the following SerDe (Serialization/Deserialization) classes out of the box. They implement the com.google.common.base.Function interface. You can create your own by implementing the same interface and utilize the provided templates to generate your own Map classes-

01. `com.axiomine.largeCollections.serdes.BytesArraySerDes`

02. `com.axiomine.largeCollections.serdes.ByteSerDes`

03. `com.axiomine.largeCollections.serdes.CharacterSerDes`

04. `com.axiomine.largeCollections.serdes.StringSerDes`

05. `com.axiomine.largeCollections.serdes.IntegerSerDes`

06. `com.axiomine.largeCollections.serdes.LongSerDes`

07. `com.axiomine.largeCollections.serdes.FloatSerDes`

08. `com.axiomine.largeCollections.serdes.DoubleSerDes`

09. `com.axiomine.largeCollections.serdes.SerializableSerDes` 

10. `com.axiomine.largeCollections.serdes.ExternalizableSerDes`

11. `com.axiomine.largeCollections.serdes.KryoSerDes`

12. `com.axiomine.largeCollections.serdes.WritableSerDes`

At this point let us examine a sample Serdes class, `StringSerdes`

    package com.axiomine.largecollections.serdes;

    import com.google.common.base.Function;
    
    public class StringSerDes {
    
		public static class SerFunction implements Function<String,byte[]>{
	    	public byte[] apply(String arg) {
	    		if(arg==null){
	    			return null;
	    		}
	    		else{
	    			return arg.getBytes();
			    }
		    }
	    }
    
	    public static class DeSerFunction implements Function<byte[],String>{
			    public String apply(byte[] arg) {
	    			if(arg==null){
	    				return null;
				    }
	    			else{
	    				return new String(arg);
	    			}
	    		}
	    }    
    }

###Note:`com.axiomine.largecollections.serdes.WritableSerDes`##
When using `com.axiomine.largecollections.serdes.WritableSerDes` be mindful of using the correct Serializer and DeSerializer pairs. For example the following pairs must be used together

1. `SerFunction implements TurboSerializer<Writable>` & `DeSerFunction implements TurboDeSerializer<Writable>` 
2. `TextSerFunction implements TurboSerializer<Text>` & `TextDeSerFunction implements TurboDeSerializer<Text>` 
3. `ArrayPrimitiveWritableSerFunction implements TurboSerializer<ArrayPrimitiveWritable>` & `ArrayPrimitiveWritableDeSerFunction implements TurboDeSerializer<ArrayPrimitiveWritable>` 
4. `BooleanWritableSerFunction implements TurboSerializer<BooleanWritable>` & `BooleanWritableDeSerFunction implements TurboDeSerializer<BooleanWritable>` 
5. `BytesWritableSerFunction implements TurboSerializer<BytesWritable>` & `BytesWritableDeSerFunction implements TurboDeSerializer<BytesWritable>` 
6. `DoubleWritableSerFunction implements TurboSerializer<DoubleWritable>` & `DoubleWritableDeSerFunction implements TurboDeSerializer<DoubleWritable>` 
7. `FloatWritableSerFunction implements TurboSerializer<FloatWritable>` & `FloatWritableDeSerFunction implements TurboDeSerializer<FloatWritable>` 
8. `IntWritableSerFunction implements TurboSerializer<IntWritable>` & `IntWritableDeSerFunction implements TurboDeSerializer<IntWritable>` 
7. `LongWritableSerFunction implements TurboSerializer<LongWritable>` & `LongWritableDeSerFunction implements TurboDeSerializer<LongWritable>` 
8. `MapWritableSerFunction implements TurboSerializer<MapWritable>` & `MapWritableDeSerFunction implements TurboDeSerializer<MapWritable>` 
9. `ShortWritableSerFunction implements TurboSerializer<ShortWritable>` & `ShortWritableDeSerFunction implements TurboDeSerializer<ShortWritable>` 


The pairs from 2 through 9 are specialized version of the one in 1 and much faster. Use them if possible.

#Performance Considerations"

We tested `KryoKVMap<Integer,Integer>` for the following. 

1. 10 Million Puts - Total time 106487 milliseconds
2. 10 Million Sequential Gets - Total time 252885
3. 10 Million Random Gets - Total time 234790

The specifications of the machine used were-
Windows 7 64 bit, 8GB RAM, Intel dual core i7-3612 QM CPU @2.10 GHz 2.10 GHz 

You performance will vary based on the type of machine used. But you should expect performance to be of the same order as above.

#License#

Copyright 2014 Axiomine
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

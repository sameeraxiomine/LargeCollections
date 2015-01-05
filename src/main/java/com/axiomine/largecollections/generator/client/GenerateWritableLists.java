package com.axiomine.largecollections.generator.client;


import com.axiomine.largecollections.generator.GeneratorWritableList;

public class GenerateWritableLists {
    public static void main(String[] args) throws Exception{
        String[] tclses={"ArrayPrimitiveWritable","BooleanWritable","BytesWritable","ByteWritable","DoubleWritable","FloatWritable","IntWritable","LongWritable","MapWritable","ShortWritable","Text"};
        String myPackage = "com.axiomine.largecollections.turboutil";
        String customImports = "-";
        String tClass="";
        
        for(String t:tclses){
            tClass=t;
            String[] myArgs = {myPackage,customImports,tClass};
            GeneratorWritableList.main(myArgs);
        }
    }
}
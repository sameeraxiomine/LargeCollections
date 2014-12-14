package com.axiomine.largecollections.generator.client;

import com.axiomine.largecollections.generator.GeneratorWritableKeyWritableValue;

public class GenerateWritableKeyWritableValueMaps {
    public static void main(String[] args) throws Exception{
        String[] keys={"ArrayPrimitiveWritable","BooleanWritable","BytesWritable","ByteWritable","DoubleWritable","FloatWritable","IntWritable","LongWritable","MapWritable","ShortWritable","Text"};
        String[] vals={"ArrayPrimitiveWritable","BooleanWritable","BytesWritable","ByteWritable","DoubleWritable","FloatWritable","IntWritable","LongWritable","MapWritable","ShortWritable","Text"};
        //String[] keys={"String"};
        //String[] vals={"Text"};

        String myPackage = "com.axiomine.largecollections.turboutil";
        String customImports = "-";
        String kPackage = "com.axiomine.largecollections.functions";
        String vPackage = "com.axiomine.largecollections.functions";
        String kClass="";
        String vClass="";
        for(String k:keys){
            kClass=k;
            for(String v:vals){
                vClass=v;
                String[] myArgs = {myPackage,customImports,kPackage,vPackage,kClass,vClass};
                GeneratorWritableKeyWritableValue.main(myArgs);
            }
        }
    }
}

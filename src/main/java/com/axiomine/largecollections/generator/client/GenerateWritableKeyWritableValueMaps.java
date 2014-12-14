package com.axiomine.largecollections.generator.client;

import com.axiomine.largecollections.generator.GeneratorWritableKeyWritableValue;

public class GenerateWritableKeyWritableValueMaps {
    public static void main(String[] args) throws Exception{
        String[] keys={"Text","IntWritable"};
        String[] vals={"Text","IntWritable"};
        //String[] keys={"String"};
        //String[] vals={"Text"};

        String myPackage = "com.axiomine.largecollections";
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

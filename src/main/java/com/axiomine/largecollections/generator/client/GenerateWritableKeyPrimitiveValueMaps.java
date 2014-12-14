package com.axiomine.largecollections.generator.client;

import com.axiomine.largecollections.generator.GeneratorWritableKeyPrimitiveValue;

public class GenerateWritableKeyPrimitiveValueMaps {
    public static void main(String[] args) throws Exception{        
        //String[] keys={"Text","IntWritable"};
        //String[] vals={"String","Integer","Long","Double","Float"};
        //
        String[] keys={"Text"};
        String[] vals={"String","Integer","Long","Double","Float","Byte","Character","byte[]"};;
        
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
                GeneratorWritableKeyPrimitiveValue.main(myArgs);
            }
        }
    }
}

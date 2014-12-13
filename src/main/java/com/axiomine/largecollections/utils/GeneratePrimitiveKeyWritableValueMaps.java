package com.axiomine.largecollections.utils;

public class GeneratePrimitiveKeyWritableValueMaps {
    public static void main(String[] args) throws Exception{
        String[] keys={"String","Integer","Long","Double","Float"};
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
                GeneratorPrimitiveKeyWritableValue.main(myArgs);
            }
        }
    }
}

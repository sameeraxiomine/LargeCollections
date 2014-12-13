package com.axiomine.largecollections.utils;

public class GeneratePrimitiveMaps {
    public static void main(String[] args) throws Exception{
        String[] keys={"String","Integer","Long","Double","Float"};
        String[] vals={"String","Integer","Long","Double","Float"};
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
                GeneratorPrimitivePrimitive.main(myArgs);
            }
        }
    }
}

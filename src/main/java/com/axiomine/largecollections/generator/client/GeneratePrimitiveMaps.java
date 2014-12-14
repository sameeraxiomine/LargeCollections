package com.axiomine.largecollections.generator.client;

import com.axiomine.largecollections.generator.GeneratorPrimitivePrimitive;

public class GeneratePrimitiveMaps {
    public static void main(String[] args) throws Exception{
        String[] keys={"Character","String","Integer","Long","Double","Float","Byte","byte[]"};
        String[] vals={"Character","String","Integer","Long","Double","Float","Byte","byte[]"};
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
                GeneratorPrimitivePrimitive.main(myArgs);
            }
        }
    }
}

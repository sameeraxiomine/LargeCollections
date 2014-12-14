package com.axiomine.largecollections.generator.client;

import com.axiomine.largecollections.generator.KryoGeneratorObjectPrimitive;

public class GenerateKryoKeyPrimitiveValueMaps {
    public static void main(String[] args) throws Exception {
        //String[] vals={"String","Integer","Long","Double","Float"};
        String[] vals={"Integer","byte[]"};
        String myPackage = "com.axiomine.largecollections.turboutil";
        String customImports = "-";
        String vPackage = "com.axiomine.largecollections.functions";
        String vClass = "";
        for (String v : vals) {
            vClass = v;
            String[] myArgs = { myPackage, customImports, vPackage,vClass };
            KryoGeneratorObjectPrimitive.main(myArgs);
        }
    }
}

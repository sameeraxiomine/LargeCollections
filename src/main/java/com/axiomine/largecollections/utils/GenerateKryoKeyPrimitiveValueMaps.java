package com.axiomine.largecollections.utils;

public class GenerateKryoKeyPrimitiveValueMaps {
    public static void main(String[] args) throws Exception {
        String[] vals={"String","Integer","Long","Double","Float"};
        String myPackage = "com.axiomine.largecollections";
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

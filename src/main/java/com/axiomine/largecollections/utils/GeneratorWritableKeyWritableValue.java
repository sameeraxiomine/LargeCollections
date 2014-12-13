package com.axiomine.largecollections.utils;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

public class GeneratorWritableKeyWritableValue {
    
    /*
     * Sample invocation
     * java GeneratorPrimitivePrimitive com.axiomine.largecollections - com.axiomine.largecollections.functions com.axiomine.largecollections.functions Integer Integer
     */
    public static void main(String[] args) throws Exception{
        //Package of the new class you are generating Ex. com.mypackage
        String MY_PACKAGE = args[0];
        //Any custom imports you need (: seperated). Use - if no custom imports are included
        //Ex. java.util.*:java.lang.Random     
        String CUSTOM_IMPORTS = args[1].equals("-")?"":args[1]; 
        //Package of your Key serializer class. Use com.axiomine.bigcollections.functions
        String KPACKAGE = args[2];
        //Package of your value serializer class. Use com.axiomine.bigcollections.functions
        String VPACKAGE = args[3];
        //Class name (no packages) of the Key class Ex. String
        String K = args[4];
        //Class name (no packages) of the value class Ex. Integer
        String V = args[5];
        String CLASS_NAME = K+V+"Map"; //Default
    
        
        
        //String templatePath = args[5];
        File root = new File("");
        File outFile = new File(root.getAbsolutePath() + "/src/main/java/"+MY_PACKAGE.replaceAll("\\.", "/")+"/"+CLASS_NAME+".java");

        if(outFile.exists()){
            System.out.println(outFile.getAbsolutePath() +" already exists. Please delete it and try again");
        }
        {
            String[] imports = null;
            String importStr = "";

            if(!StringUtils.isBlank(CUSTOM_IMPORTS)){
                CUSTOM_IMPORTS.split(":");
                for(String s:imports){
                    importStr="import "+s+";\n";
                }
            }

            String program = FileUtils.readFileToString(new File(root.getAbsolutePath()+"/src/main/resources/WritableKeyWritableValueMapTemplate.java"));

            program = program.replaceAll("#MY_PACKAGE#", MY_PACKAGE);
            program = program.replaceAll("#CUSTOM_IMPORTS#", importStr);
            
            program = program.replaceAll("#CLASS_NAME#", CLASS_NAME);
            program = program.replaceAll("#K#", K);
            program = program.replaceAll("#V#", V);
            program = program.replaceAll("#KPACKAGE#", KPACKAGE);
            program = program.replaceAll("#VPACKAGE#", VPACKAGE);
            System.out.println(outFile.getAbsolutePath());
            FileUtils.writeStringToFile(outFile, program);
        }
    }
}

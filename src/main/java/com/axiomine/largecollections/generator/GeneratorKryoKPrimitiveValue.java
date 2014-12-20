package com.axiomine.largecollections.generator;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

public class GeneratorKryoKPrimitiveValue {
    /*
     * Sample invocation
     * java GeneratorKryoKPrimitiveValue com.axiomine.largecollections - com.axiomine.largecollections.functions Integer
     */
    public static void main(String[] args) throws Exception{
        //Package of the new class you are generating Ex. com.mypackage
        String MY_PACKAGE = args[0];
        //Any custom imports you need (: seperated). Use - if no custom imports are included
        //Ex. java.util.*:java.lang.Random     
        String CUSTOM_IMPORTS = args[1].equals("-")?"":args[1]; 
        //Package of your Key serializer class. Use com.axiomine.bigcollections.functions
        //Package of your value serializer class. Use com.axiomine.bigcollections.functions
        String VPACKAGE = args[2];
        //Class name (no packages) of the Key class Ex. String
        //Class name (no packages) of the value class Ex. Integer
        String V = args[3];
        //Specify if you are using a KryoTemplate to generate your classes
        //If true the template used to generate the class is KryoBasedMapTemplte, if false the JavaLangBasedMapTemplate is used
        //You can customize the name of the class generated
        String vCls = V;
        
   
        if(vCls.equals("byte[]")){
            vCls = "BytesArray";
        }
        
        String CLASS_NAME = "KryoK"+vCls+"Map"; //Default
        
        
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
            String program = FileUtils.readFileToString(new File(root.getAbsolutePath()+"/src/main/resources/KryoKeyPrimitiveValueMapTemplate.java"));
            program = program.replaceAll("#MY_PACKAGE#", MY_PACKAGE);
            program = program.replaceAll("#CUSTOM_IMPORTS#", importStr);
            
            program = program.replaceAll("#CLASS_NAME#", CLASS_NAME);
            program = program.replaceAll("#V#", V);
            program = program.replaceAll("#VPACKAGE#", VPACKAGE);
            
            program = program.replaceAll("#VCLS#", vCls);
            System.out.println(outFile.getAbsolutePath());
            FileUtils.writeStringToFile(outFile, program);
        }
    }
}

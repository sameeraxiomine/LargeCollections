package com.axiomine.largecollections.utils;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class Generator {
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
        //Specify if you are using a KryoTemplate to generate your classes
        //If true the template used to generate the class is KryoBasedMapTemplte, if false the JavaLangBasedMapTemplate is used
        boolean useKryoTemplate = Boolean.parseBoolean(args[6]);
        //You can customize the name of the class generated
        String CLASS_NAME = K+V+"Map"; //Default
        if(args.length>7){
            CLASS_NAME = args[7];
        }
    
        
        
        //String templatePath = args[5];
        File root = new File("");
        File outFile = new File(root.getAbsolutePath() + "/src/main/java/"+MY_PACKAGE.replaceAll("\\.", "/")+"/"+CLASS_NAME+".java");

        if(outFile.exists()){
            System.out.println(outFile.getAbsolutePath() +" already exists. Please delete it and try again");
        }
        {
            String[] imports = CUSTOM_IMPORTS.split(":");
            String importStr = "";
            for(String s:imports){
                importStr="import "+s+"\n";
            }
            String program = "";
            if(useKryoTemplate)
                program = FileUtils.readFileToString(new File(root.getAbsolutePath()+"/src/main/resources/KryoBasedMapTemplate.java"));
            else
                program = FileUtils.readFileToString(new File(root.getAbsolutePath()+"/src/main/resources/JavaLangBasedMapTemplate.java"));
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

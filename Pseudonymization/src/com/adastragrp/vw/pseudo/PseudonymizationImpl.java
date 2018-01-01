package com.adastragrp.vw.pseudo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import sun.util.logging.resources.logging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PseudonymizationImpl extends UDF {
    public Text evaluate(Text inputStr, String filePath) {

  if (inputStr == null || inputStr.getLength()==0) {
            System.out.println("Input String is NULL or empty");
            return null;
        }
        if (filePath == null || filePath.length()==0) {
            System.out.println("Hash salt file path not specified");
            return null;
        }
        BufferedReader br = null;
        StringBuffer sb= null;
        try{
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream in = fs.open(new Path(filePath));
            br = new BufferedReader(new InputStreamReader(in));

            String hashLine= br.readLine();
            String finalText = hashLine + inputStr;
            sb= new StringBuffer();

            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(finalText.getBytes(StandardCharsets.UTF_8));
            byte[] byteData = md.digest();

            for (int i = 0; i < byteData.length; i++) {
                sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
            }
        } catch(Exception e) {

            e.printStackTrace();
        } finally {
            try {
                if (br!=null)
                {br.close();}
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new Text(sb.toString());
    }
}

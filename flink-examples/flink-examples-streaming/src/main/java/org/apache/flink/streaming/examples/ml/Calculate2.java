package org.apache.flink.streaming.examples.ml;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Calculate2 {
    public static void main(String[] args) throws IOException {
        FileInputStream inputStream = new FileInputStream(
                "/Users/rann/Documents/GitHub/flink/build-target/output"+ ".txt");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String str;
        String pattern = "(\\d+) ms";
        Pattern r = Pattern.compile(pattern);
        while((str = bufferedReader.readLine()) != null)
        {
            if(str.startsWith("Job Runtime")){
                Matcher m = r.matcher(str);
                if (m.find( )) {
                    int time = Integer.parseInt(m.group(1));
                    System.out.println(time);
                }
            }
        }

        //close
        inputStream.close();
        bufferedReader.close();
    }
}

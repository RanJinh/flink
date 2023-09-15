package org.apache.flink.streaming.examples.ml;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.min;

public class Calculate {
    public static void main(String[] args) throws IOException {
        int[] ints = {5000000, 5000000, 10000000, 10000000, 20000000, 20000000, 50000000, 50000000};
        ArrayList<Tuple2<Integer, Integer>> ave = new ArrayList<Tuple2<Integer, Integer>>();
        int globalMin = 999999;
        for(int i = 1; i <= 8; ++i){
            FileInputStream inputStream = new FileInputStream(
                    "/Users/rann/Documents/GitHub/flink/build-target/output" + i + ".txt");
            System.out.println("output" + i + ".txt");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str;
            int cnt = 0, sum = 0, max = -1, min = 99999999;
            int average = 0;
            String pattern = "(\\d+) ms";
            Pattern r = Pattern.compile(pattern);
            while((str = bufferedReader.readLine()) != null)
            {
                if(str.startsWith("Job Runtime")){
                    Matcher m = r.matcher(str);
                    if (m.find( )) {
                        int time = Integer.parseInt(m.group(1));
                        if(time > max){
                            max = time;
                        }
                        if(time < min){
                            min = time;
                        }
                        sum += time;
                    }
                    cnt++;
                }
                if(cnt == 5){
                    average = ints[i - 1] / (sum / 5);
                    max = ints[i - 1] / max;
                    min = ints[i - 1] / min;
                    int diff = Math.max(min - average,average - max);
                    System.out.println(sum / 5);
                    ave.add(Tuple2.of(average, diff));
                    globalMin = min(globalMin, average);
                    sum = 0;
                    max = -1;
                    min = 99999999;
                    cnt = 0;
                }
            }
            System.out.println();

            //close
            inputStream.close();
            bufferedReader.close();
        }

        for(Tuple2<Integer, Integer> value : ave){
            int percentage = value.f0 * 100 / globalMin;
            System.out.println(value.f0 + " ± " + value.f1 + " (" + percentage + "%)");
        }
    }
}

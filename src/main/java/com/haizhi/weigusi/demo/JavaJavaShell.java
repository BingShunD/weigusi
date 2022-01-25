package com.haizhi.weigusi.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class JavaJavaShell {
    public static void main(String[] args) {
//        String[] strings = new String[2];
//        strings[0] = "cat";
//        strings[1] = "/etc/hosts";
        String[] strings = {"cat","/Users/duanbingshun/Desktop/dockerload.txt"};
        String s = execShell(strings);
        System.out.println(s);
    }

    public static String execShell (String[] cmds) {
        ProcessBuilder process = new ProcessBuilder(cmds);
        String res = "";
        try {
            Process p = process.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = reader.readLine();
            List<String> strings = new ArrayList<>();
            while (line != null){
                strings.add(line);
                line=reader.readLine();
            }
            for (String string : strings) {
                res+=string;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }
}

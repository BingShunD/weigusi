package com.haizhi.weigusi.demo;

public class StringETL {
    public static void main(String[] args) {
        String ss = "service_type";

        System.out.println(replaceUnderlineAndfirstToUpper(ss, "_", ""));

    }

    public static String firstCharacterToUpper(String srcStr) {
        return srcStr.substring(0, 1).toUpperCase() + srcStr.substring(1);
    }

    public static String replaceUnderlineAndfirstToUpper(String srcStr,String org,String ob)
    {
        String newString = "";
        int first=0;
        while(srcStr.indexOf(org)!=-1)
        {
            first=srcStr.indexOf(org);
            if(first!=srcStr.length())
            {
                newString=newString+srcStr.substring(0,first)+ob;
                srcStr=srcStr.substring(first+org.length(),srcStr.length());
                srcStr=firstCharacterToUpper(srcStr);
            }
        }
        newString=newString+srcStr;
        return newString;
    }

}

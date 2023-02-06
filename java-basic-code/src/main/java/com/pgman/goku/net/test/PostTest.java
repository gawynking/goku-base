package com.pgman.goku.net.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class PostTest {

    public static void main(String[] args) throws IOException {

        String propsFileName = args.length>0?args[0]:"post/post.properties";
        Properties properties = new Properties();
        InputStream in = null;
        try {
            in = Files.newInputStream(Paths.get(propsFileName));
            properties.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }

        String urlString = properties.remove("url").toString();
        Object userAgent = properties.remove("User-Agent");
        Object redirects = properties.remove("redirects");

        CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
        String reslut = doPost(
                new URL(urlString),
                properties,
                userAgent==null?null:userAgent.toString(),
                redirects==null?-1:Integer.parseInt(redirects.toString())
        );
        System.out.println(reslut);
    }

    private static String doPost(URL url, Properties properties, String userAgent, int redirects) throws IOException {
        HttpURLConnection urlConnection = (HttpURLConnection)url.openConnection();
        if(userAgent!=null){
            urlConnection.setRequestProperty("User-Agent",userAgent);
        }
        if(redirects>=0){
            urlConnection.setInstanceFollowRedirects(false);
        }

        urlConnection.setDoOutput(true);

        PrintWriter out = null;
        try {
            out = new PrintWriter(urlConnection.getOutputStream());
            boolean first = true;
            for(Map.Entry<Object,Object> pair : properties.entrySet()){
                if(first)first=false;else out.print("&");
                String name = pair.getKey().toString();
                String value = pair.getValue().toString();
                out.print(name);
                out.print("=");
                out.print(URLEncoder.encode(value, String.valueOf(StandardCharsets.UTF_8)));
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        String encoding = urlConnection.getContentEncoding();
        if(encoding==null)encoding="UTF-8";

//        重定向
        if(redirects>0){
            int responseCode = urlConnection.getResponseCode();
            if(responseCode == HttpURLConnection.HTTP_MOVED_PERM || responseCode == HttpURLConnection.HTTP_MOVED_TEMP || responseCode == HttpURLConnection.HTTP_SEE_OTHER){
                String location = urlConnection.getHeaderField("Location");
                if(location != null){
                    URL base = urlConnection.getURL();
                    urlConnection.disconnect();
                    return doPost(new URL(base,location),properties,userAgent,redirects-1);
                }
            }
        }else if(redirects==0){
            throw new IOException("Too many redirects");
        }

        StringBuffer response = new StringBuffer();
        Scanner in = null;
        try{
            in = new Scanner(urlConnection.getInputStream(), encoding);
            while (in.hasNextLine()){
                response.append(in.nextLine());
                response.append("\n");
            }
        }catch (IOException e){
            InputStream err = urlConnection.getErrorStream();
            if(err == null) throw e;
            in = new Scanner(err);
            response.append(in.nextLine());
            response.append("\n");
        }

        return response.toString();
    }

}

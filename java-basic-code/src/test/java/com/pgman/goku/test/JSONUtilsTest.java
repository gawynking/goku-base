package com.pgman.goku.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;


public class JSONUtilsTest {



    public static void main(String[] args) {

        String s1 = "{\"error\":0,\"status\":\"success\",\"results\":[{\"currentCity\":\"青岛\",\"index\":[{\"title\":\"穿衣\",\"zs\":\"较冷\",\"tipt\":\"穿衣指数\",\"des\":\"建议着厚外套加毛衣等服装。年老体弱者宜着大衣、呢外套加羊毛衫。\"},{\"title\":\"紫外线强度\",\"zs\":\"最弱\",\"tipt\":\"紫外线强度指数\",\"des\":\"属弱紫外线辐射天气，无需特别防护。若长期在户外，建议涂擦SPF在8-12之间的防晒护肤品。\"}],}]}";

        String jsonStr = "{\"order_id\":\"369\",\"amt\":\"999.0\",\"list\":{\"p1\":\"oracle\",\"p2\":\"pgsql\",\"p3\":\"mysql\"},\"user_info\":{\"name\":\"chavin\",\"age\":\"30\",\"address\":{\"addr1\":\"beijing\",\"addr2\":\"shanghai\"}}}";
        JSONObject jsonObject1 = JSONObject.parseObject(s1);


    }
}
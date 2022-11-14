package com.pgman.goku.util;

import com.alibaba.fastjson.JSONObject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 对象通用抽象工具类
 */
public class ObjectUtils extends org.apache.commons.lang3.ObjectUtils{

	public static Map<Character,String> charMapper = new HashMap<>();
	static {
		charMapper.put('A',"_a");
		charMapper.put('B',"_b");
		charMapper.put('C',"_c");
		charMapper.put('D',"_d");
		charMapper.put('E',"_e");
		charMapper.put('F',"_f");
		charMapper.put('G',"_g");
		charMapper.put('H',"_h");
		charMapper.put('I',"_i");
		charMapper.put('J',"_j");
		charMapper.put('K',"_k");
		charMapper.put('L',"_l");
		charMapper.put('M',"_m");
		charMapper.put('N',"_n");
		charMapper.put('O',"_o");
		charMapper.put('P',"_p");
		charMapper.put('Q',"_q");
		charMapper.put('R',"_r");
		charMapper.put('S',"_s");
		charMapper.put('T',"_t");
		charMapper.put('U',"_u");
		charMapper.put('V',"_v");
		charMapper.put('W',"_w");
		charMapper.put('X',"_x");
		charMapper.put('Y',"_y");
		charMapper.put('Z',"_z");
	}

	/**
	 * empty object
	 *
	 * @param obj
	 * @return
	 */
	public static boolean isEmpty(Object obj){
		return obj == null || isEmpty(obj.toString());
	}
	
	/**
	 * not empty object
	 *
	 * @param obj
	 * @return
	 */
	public static boolean isNotEmpty(Object obj){
		return !isEmpty(obj);
	}
	
	/**
	 * empty String
	 *
	 * @param str
	 * @return
	 */
	public static boolean isEmpty(String str){
		return str == null || "".equals(str);
	}
	
	/**
	 * not empty String
	 *
	 * @param str
	 * @return
	 */
	public static boolean isNotEmpty(String str){
		return !isEmpty(str);
	}

	/**
	 * empty collection
	 *
	 * @param collection
	 * @return
	 */
	public static boolean isEmpty(Collection<?> collection) {
		return (collection == null) || (collection.isEmpty());
	}
	
	/**
	 * not empty collection
	 *
	 * @param collection
	 * @return
	 */
	public static boolean isNotEmpty(Collection<?> collection) {
		return !isEmpty(collection);
	}
	
	/**
	 * empty Iterator
	 *
	 * @param iterator
	 * @return
	 */
	public static boolean isEmpty(Iterator<?> iterator) {
		return (iterator == null) || (iterator.hasNext());
	}
	
	/**
	 * not empty Iterator
	 *
	 * @param iterator
	 * @return
	 */
	public static boolean isNotEmpty(Iterator<?> iterator) {
		return !isEmpty(iterator);
	}

	/**
	 * empty map
	 *
	 * @param map
	 * @return
	 */
	public static boolean isEmpty(Map<?, ?> map) {
		return (map == null) || (map.isEmpty());
	}
	
	/**
	 * not empty map
	 *
	 * @param map
	 * @return
	 */
	public static boolean isNotEmpty(Map<?, ?> map) {
		return !isEmpty(map);
	}
	
	/**
	 * empty Object[]
	 *
	 * @param objs
	 * @return
	 */
	public static boolean isEmpty(Object[] objs) {
		return (objs == null) || (objs.length == 0);
	}
	
	/**
	 * not empty Object[]
	 *
	 * @param objs
	 * @return
	 */
	public static boolean isNotEmpty(Object[] objs) {
		return !isEmpty(objs);
	}


	/**
	 * 将给定实例转化为JsonObject对象返回
	 * 	- 给定对象类型必须覆写getter和setter方法
	 * 	- 类变量不处理
	 *
	 * @param obj
	 * @param t
	 * @return
	 * @param <T>
	 */
	public static <T> JSONObject objInstanceToJsonObject(Object obj, T t){

		try{

			T object = (T) obj;
			Class<?> clz = Class.forName(object.getClass().getName());
			Field[] staticFields = clz.getFields();
			Field[] allFields = clz.getDeclaredFields();

			JSONObject jsonObject = new JSONObject();
			for (Field field:allFields){
				boolean flag = false;
				for (Field innerField:staticFields){
					if(innerField.getName().equals(field.getName())){
						flag=true;
						break;
					}
				}
				if(!flag){
					String name = field.getName();
					Method method = object.getClass().getMethod("get" + name.substring(0, 1).toUpperCase() + name.substring(1));

					char[] nameChars = name.toCharArray();
					StringBuffer bf = new StringBuffer();
					for(char c:nameChars){
						boolean cExists = charMapper.containsKey(c);
						if(cExists){
							bf.append(charMapper.get(c));
						}else {
							bf.append(c);
						}
					}
					String key = bf.toString();
					String value = String.valueOf(method.invoke(object));
					jsonObject.put(key,value);
				}

			}

			return jsonObject;
		}catch (Exception e){
			e.printStackTrace();
			return null;
		}

	}


}

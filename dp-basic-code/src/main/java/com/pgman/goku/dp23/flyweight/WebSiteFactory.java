package com.pgman.goku.dp23.flyweight;

import java.util.HashMap;

/**
 * 小型的外包项目，给客户A做一个产品展示网站，客户A的朋友感觉效果不错，也希望做这样的产品展示网站，但是要求都有些不同：
 * 1) 有客户要求以新闻的形式发布
 * 2) 有客户人要求以博客的形式发布
 * 3) 有客户希望以微信公众号的形式发布
 */
public class WebSiteFactory {

	private HashMap<String, ConcreteWebSite> pool = new HashMap<>();
	
	public WebSite getWebSiteCategory(String type) {
		if(!pool.containsKey(type)) {
			pool.put(type, new ConcreteWebSite(type));
		}
		
		return (WebSite)pool.get(type);
	}
	
	public int getWebSiteCount() {
		return pool.size();
	}

}

package com.pgman.goku.dp23.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 应用实例要求
 * 编写程序展示一个学校院系结构：需求是这样，要在一个页面中展示出学校的院系组成，一个学校有多个学院，一个学院有多个系。
 */
public class Client {

	public static void main(String[] args) {

		List<College> collegeList = new ArrayList<College>();
		
		ComputerCollege computerCollege = new ComputerCollege();
		InfoCollege infoCollege = new InfoCollege();
		
		collegeList.add(computerCollege);
		collegeList.add(infoCollege);

		OutPutImpl outPutImpl = new OutPutImpl(collegeList);
		outPutImpl.printCollege();

	}

}

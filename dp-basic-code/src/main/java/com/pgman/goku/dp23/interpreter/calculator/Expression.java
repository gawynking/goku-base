package com.pgman.goku.dp23.interpreter.calculator;

import java.util.HashMap;

/**
 * 说明：抽象类表达式，通过HashMap键值对管理，可以通过键获取对应的值
 */
public abstract class Expression {

	// 解释公式和数值,key就是公式（表达式） 参数【abc】，value就是参数值 {a=10,b=5}
	public abstract int interpreter(HashMap<String, Integer> var);

}

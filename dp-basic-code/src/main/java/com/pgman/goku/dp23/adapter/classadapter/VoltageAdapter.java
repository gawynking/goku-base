package com.pgman.goku.dp23.adapter.classadapter;

// 适配器类
public class VoltageAdapter extends Voltage220V implements IVoltage5V {

	@Override
	public int output5V() { // 返回5v电压
		// TODO Auto-generated method stub
		// 获取220v电压
		int srcV = output220V();
		int dstV = srcV / 44 ;
		return dstV;
	}

}

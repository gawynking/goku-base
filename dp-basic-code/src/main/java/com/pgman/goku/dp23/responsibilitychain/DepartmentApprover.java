package com.pgman.goku.dp23.responsibilitychain;

public class DepartmentApprover extends Approver {

	
	public DepartmentApprover(String name) {
		super(name);
	}
	
	@Override
	public void processRequest(PurchaseRequest purchaseRequest) {
		if(purchaseRequest.getPrice() <= 5000) {
			System.out.println("requst id = " + purchaseRequest.getId() + " by " + this.name + " process");
		}else {
			approver.processRequest(purchaseRequest);
		}
	}

}

package com.pgman.goku.dp23.responsibilitychain;

public class ViceSchoolMasterApprover extends Approver {

    public ViceSchoolMasterApprover(String name) {
        super(name);
    }

    @Override
    public void processRequest(PurchaseRequest purchaseRequest) {
        if (purchaseRequest.getPrice() < 10000 && purchaseRequest.getPrice() <= 30000) {
            System.out.println("requst id = " + purchaseRequest.getId() + " by " + this.name + " process");
        } else {
            approver.processRequest(purchaseRequest);
        }
    }
}

package com.pgman.goku.dp23.responsibilitychain;

public class SchoolMasterApprover extends Approver {

    public SchoolMasterApprover(String name) {
        super(name);
    }

    @Override
    public void processRequest(PurchaseRequest purchaseRequest) {
        if (purchaseRequest.getPrice() > 30000) {
            System.out.println("requst id = " + purchaseRequest.getId() + " by " + this.name + " process");
        } else {
            approver.processRequest(purchaseRequest);
        }
    }
}

package com.pgman.goku.dp23.responsibilitychain;

public class CollegeApprover extends Approver {

    public CollegeApprover(String name) {
        super(name);
    }

    @Override
    public void processRequest(PurchaseRequest purchaseRequest) {
        if (purchaseRequest.getPrice() < 5000 && purchaseRequest.getPrice() <= 10000) {
            System.out.println("requst id = " + purchaseRequest.getId() + " by " + this.name + " process");
        } else {
            approver.processRequest(purchaseRequest);
        }
    }
}

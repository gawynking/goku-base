package com.pgman.goku.dp23.responsibilitychain;

/**
 * 学校OA系统的采购审批项目：需求是
 * 1) 采购员采购教学器材
 * 2) 如果金额 小于等于5000, 由教学主任审批 （0<=x<=5000）
 * 3) 如果金额 小于等于10000, 由院长审批 (5000<x<=10000)
 * 4) 如果金额 小于等于30000, 由副校长审批 (10000<x<=30000)
 * 5) 如果金额 超过30000以上，有校长审批 ( 30000<x)
 * 请设计程序完成采购审批项目
 */
public class Client {

    public static void main(String[] args) {

        PurchaseRequest purchaseRequest = new PurchaseRequest(1, 31000, 1);

        DepartmentApprover departmentApprover = new DepartmentApprover("zhangsan");
        CollegeApprover collegeApprover = new CollegeApprover("lisi");
        ViceSchoolMasterApprover viceSchoolMasterApprover = new ViceSchoolMasterApprover("wangwu");
        SchoolMasterApprover schoolMasterApprover = new SchoolMasterApprover("zhaoliu");

        // 配置链依赖
        departmentApprover.setApprover(collegeApprover);
        collegeApprover.setApprover(viceSchoolMasterApprover);
        viceSchoolMasterApprover.setApprover(schoolMasterApprover);
        schoolMasterApprover.setApprover(departmentApprover);

        departmentApprover.processRequest(purchaseRequest);
        viceSchoolMasterApprover.processRequest(purchaseRequest);
    }

}

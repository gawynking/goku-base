package com.pgman.goku.nowcoder;

import com.pgman.goku.nowcoder.base.ListNode;

/**
 * 链表内指定区间反转
 *
 * 输入：{1,2,3,4,5},2,4
 * 返回值：{1,4,3,2,5}
 */
public class ALGReverseListBetween {

    /**
     * 链表内指定区间反转
     *
     * 输入：{1,2,3,4,5},2,4
     * 返回值：{1,4,3,2,5}
     *
     * @return
     */
    public ListNode reverseListBetween(ListNode head,int m,int n){

        ListNode res = new ListNode(-1);
        ListNode cur = head;
        ListNode pre = res;
        pre.next = cur;

        // 处理前m个输入
        for(int i=1;i<m;i++){
            pre = cur;
            cur = cur.next;
        }

        ListNode preM = pre; // 缓存上个node结果
        ListNode mNode = null;
        ListNode tmpPre = null;
        ListNode tmpCur = cur;

        // 处理m，n之间数据
        for(int i=m;i<=n;i++){
            if(i==m) mNode=cur;
            ListNode temp = tmpCur.next;
            tmpCur.next = tmpPre;
            tmpPre = tmpCur;
            tmpCur = temp;
        }

        preM.next = tmpPre;
        mNode.next = tmpCur;

        return res.next;
    }

    public static void main(String[] args) {

        // 构造输入列表
        ListNode node1 = new ListNode(1);
        ListNode node2 = new ListNode(2);
        ListNode node3 = new ListNode(3);
        ListNode node4 = new ListNode(4);
        ListNode node5 = new ListNode(5);
        ListNode node6 = new ListNode(6);
        ListNode node7 = new ListNode(7);
        ListNode node8 = new ListNode(8);

        node1.next = node2;
        node2.next = node3;
        node3.next = node4;
        node4.next = node5;
        node5.next = node6;
        node6.next = node7;
        node7.next = node8;



        System.out.println("输入：" + node1);

        ListNode reverseListBetween = new ALGReverseListBetween().reverseListBetween(node1,3,6);

        System.out.println("输出：" + reverseListBetween);
    }
}

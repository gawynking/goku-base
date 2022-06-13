package com.pgman.goku.nowcoder;

import com.pgman.goku.nowcoder.base.ListNode;

/**
 * 反转链表
 * 输入值：{1,2,3}
 * 返回值：{3,2,1}
 */
public class ALGReverseList {


    /**
     * 核心算法
     * @param head
     * @return
     */
    public ListNode reverseList(ListNode head){

        ListNode pre = null;
        ListNode cur = head;

        while (null != cur){
            ListNode next = cur.next;
            cur.next = pre;
            pre = cur;
            cur = next;
        }

        return pre;
    }

    /**
     * 测试
     *
     * @param args
     */
    public static void main(String[] args) {

        // 构造输入列表
        ListNode node1 = new ListNode(1);
        ListNode node2 = new ListNode(2);
        ListNode node3 = new ListNode(3);
        node1.next = node2;
        node2.next = node3;


        System.out.println(node1);

        ListNode reverseList = new ALGReverseList().reverseList(node1);

        System.out.println(reverseList);
    }

}

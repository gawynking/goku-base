package com.pgman.goku.nowcoder;

import com.pgman.goku.nowcoder.base.ListNode;

/**
 * 链表中的节点每k个一组翻转
 */
public class ALGReverseKGroup {

    /**
     * 链表中的节点每k个一组翻转
     *
     * 输入：{1,2,3,4,5},2
     * 返回值：{2,1,4,3,5}
     *
     * @param head
     * @param k
     * @return
     */
    public ListNode reverseKGroup(ListNode head,int k){
        //找到每次翻转的尾部
        ListNode tail = head;

        // 遍历k次到尾部
        for(int i = 0; i < k; i++){
            //如果不足k到了链表尾，直接返回，不翻转
            if(tail == null)
                return head;
            tail = tail.next;
        }

        //翻转时需要的前序和当前节点
        ListNode pre = null;
        ListNode cur = head;
        //在到达当前段尾节点前
        while(cur != tail){
            // 翻转
            ListNode temp = cur.next;
            cur.next = pre;
            pre = cur;
            cur = temp;
        }

        // 当前尾指向下一段要翻转的链表
        head.next = reverseKGroup(tail, k);
        return pre;

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

        ListNode reverseKGroup = new ALGReverseKGroup().reverseKGroup(node1,3);

        System.out.println("输出：" + reverseKGroup);
    }

}

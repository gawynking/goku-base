package com.pgman.goku.nowcoder;

import com.pgman.goku.nowcoder.base.ListNode;

public class ALGSortInList {

    // 合并两段有序链表
    ListNode merge(ListNode pHead1, ListNode pHead2) {

        // 一个已经为空了，直接返回另一个
        if (pHead1 == null)
            return pHead2;
        if (pHead2 == null)
            return pHead1;

        // 加一个表头
        ListNode head = new ListNode(-1);
        ListNode cur = head;

        // 两个链表都要不为空
        while (pHead1 != null && pHead2 != null) {
            // 取较小值的节点
            if (pHead1.val <= pHead2.val) {
                cur.next = pHead1;
                // 只移动取值的指针
                pHead1 = pHead1.next;
            } else {
                cur.next = pHead2;
                // 只移动取值的指针
                pHead2 = pHead2.next;
            }
            // 指针后移
            cur = cur.next;
        }

        // 哪个链表还有剩，直接连在后面
        if (pHead1 != null)
            cur.next = pHead1;
        else
            cur.next = pHead2;

        // 返回值去掉表头
        return head.next;

    }

    // 单链表排序
    public ListNode sortInList(ListNode head) {

        // 链表为空或者只有一个元素，直接就是有序的
        if (head == null || head.next == null)
            return head;

        ListNode left = head; // 步长0
        ListNode mid = head.next; // 步长1
        ListNode right = head.next.next; // 步长2

        // 右边的指针到达末尾时，中间的指针指向该段链表的中间
        while (right != null && right.next != null) {
            left = left.next;
            mid = mid.next;
            right = right.next.next;
        }

        // 左边指针指向左段的左右一个节点，从这里断开
        left.next = null;

        // 分成两段排序，合并排好序的两段
        return merge(sortInList(head), sortInList(mid));

    }


    /**
     * 基于冒泡排序实现
     *
     * @param head
     * @return
     */
    public ListNode sortList(ListNode head) {

        ListNode res = new ListNode(-1);
        res.next = head;
        ListNode outCur = head;
        ListNode innerCur = head;
        ListNode pre = res;

        while (null != outCur && null != outCur.next){
            while (null != innerCur && null != innerCur.next){
                if(innerCur.val > innerCur.next.val){
                    ListNode temp = innerCur.next;
                    innerCur.next = temp.next;
                    temp.next = innerCur;
                    pre.next = temp;
                    pre = innerCur;
                    innerCur = innerCur.next;
                }
            }
            outCur = outCur.next;
        }

        return res.next;

    }

    public static void main(String[] args) {
        // 构造输入列表
        ListNode node1 = new ListNode(3);
        ListNode node2 = new ListNode(1);
        ListNode node3 = new ListNode(2);
        ListNode node4 = new ListNode(5);
        node1.next = node2;
        node2.next = node3;
        node3.next = node4;


        System.out.println(node1);

        ListNode listNode = new ALGSortInList().sortList(node1);

        System.out.println(listNode);

    }

}

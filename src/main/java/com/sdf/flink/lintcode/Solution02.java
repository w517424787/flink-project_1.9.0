package com.sdf.flink.lintcode;

/**
 * Modify by wwg 2020-05-31
 * 给出两个 非空 的链表用来表示两个非负的整数。其中，它们各自的位数是按照逆序的方式存储的，并且它们的每个节点只能存储一位数字。
 * 如果，我们将这两个数相加起来，则会返回一个新的链表来表示它们的和。
 * 您可以假设除了数字 0之外，这两个数都不会以 0开头。
 * 示例：
 * 输入：(2 -> 4 -> 3) + (5 -> 6 -> 4)
 * 输出：7 -> 0 -> 8
 * 原因：342 + 465 = 807
 */
public class Solution02 {

    private static class ListNode {
        int val;
        ListNode next;

        ListNode(int data) {
            this.val = data;
        }
    }

    /**
     * 两个数相加
     *
     * @param l1
     * @param l2
     * @return
     */
    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        //进位
        int carried = 0;
        int data;
        //先构建一个初始值-1的链表
        ListNode node = new ListNode(-1);
        //记录初始位置
        ListNode newNode = node;
//        if (l1 == null) {
//            node.next = l2;
//        }
//        if (l2 == null) {
//            node.next = l1;
//        }

        ListNode p1 = l1;
        ListNode p2 = l2;
        //循环判断
        while (p1 != null || p2 != null) {
            //都不为空时
            if (p1 != null && p2 != null) {
                data = (carried + p1.val + p2.val) % 10;
                carried = (carried + p1.val + p2.val) / 10;

                p1 = p1.next;
                p2 = p2.next;

            } else if (p1 == null) {
                data = (carried + p2.val) % 10;
                carried = (carried + p2.val) / 10;

                p2 = p2.next;
            } else {
                data = (carried + p1.val) % 10;
                carried = (carried + p1.val) / 10;

                p1 = p1.next;
            }

            //将值添加到-1的链表后
            node.next = new ListNode(data);
            //重新对node重新赋值
            node = node.next;
        }

        //当进位值不等于0时，需要在添加1位
        //如：5+7 = 12，这是carried = 1，但是p1.next 和 p2.next都为null
        if (carried != 0) {
            node.next = new ListNode(carried);
        }

        //头部是值-1，需过滤
        return newNode.next;
    }

    public static void main(String[] args) {
        //(2 -> 4 -> 3) + (5 -> 6 -> 4)
        ListNode l1 = new ListNode(2);
        l1.next = new ListNode(4);
        l1.next.next = new ListNode(3);

        ListNode l2 = new ListNode(5);
        l2.next = new ListNode(6);
        l2.next.next = new ListNode(4);

        ListNode result = addTwoNumbers(l1, l2);
        while (result != null) {
            System.out.println(result.val);
            result = result.next;
        }
    }
}

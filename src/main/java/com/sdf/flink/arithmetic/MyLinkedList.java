package com.sdf.flink.arithmetic;

/**
 * Modify by wwg 2020-05-30
 * 模拟单链表增、删、改、查操作
 */

public class MyLinkedList {

    //定义链表节点Node
    private static class Node {
        int data;
        Node next;

        Node(int data) {
            this.data = data;
        }
    }

    //头节点指针
    private Node head;
    //尾节点指针
    private Node last;
    //链表长度
    private int size;

    /**
     * 链表中插入数据
     *
     * @param data  数据
     * @param index 位置
     */
    public void insert(int data, int index) throws Exception {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("超出链表节点范围！");
        }

        Node insertNode = new Node(data);

        //插入分三种情况：头部、中间、尾部
        //空链表
        if (size == 0) {
            head = insertNode;
            last = insertNode;
        } else if (index == 0) {
            //头部插入
            insertNode.next = head;
            head = insertNode;

        } else if (index == size) {
            //尾部插入
            last.next = insertNode;
            last = insertNode;

        } else {
            //中间插入
            Node preNode = getNode(index - 1);
            insertNode.next = preNode.next;
            preNode.next = insertNode;
        }

        //链表size+1
        size++;
    }

    /**
     * 删除指定链表节点
     *
     * @param index 删除的位置
     * @return
     * @throws Exception
     */
    public Node remove(int index) throws Exception {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("超出链表节点范围！");
        }

        Node removeNode = null;
        //删除头部节点
        if (index == 0) {
            removeNode = head;
            head = head.next;
        } else if (index == size - 1) {
            //删除尾部节点
            //先找出尾部节点的前一个节点
            Node preNode = getNode(index - 1);
            removeNode = preNode.next;
            preNode.next = null;
            last = preNode;
        } else {
            //删除中间节点
            Node preNode = getNode(index - 1);
            removeNode = preNode.next;
            //Node nextNode = preNode.next.next;
            preNode.next = preNode.next.next;
        }

        size--;
        return removeNode;
    }

    /**
     * 根据index去查找链表节点
     *
     * @param index
     * @return
     * @throws Exception
     */
    public Node getNode(int index) throws Exception {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("超出链表节点范围！");
        }

        //从头部开始查找
        Node temp = head;
        for (int i = 0; i < index; i++) {
            temp = temp.next;
        }
        return temp;
    }

    /**
     * 输出链表值
     */
    public void output() {
        Node temp = head;
        while (temp != null) {
            System.out.println(temp.data);
            temp = temp.next;
        }
    }

    public static void main(String[] args) throws Exception {
        MyLinkedList myLinkedList = new MyLinkedList();
        myLinkedList.insert(3, 0);
        myLinkedList.insert(4, 1);
        myLinkedList.insert(5, 2);
        myLinkedList.insert(6, 3);
        myLinkedList.insert(7, 1);
        myLinkedList.remove(2);
        myLinkedList.output();
    }
}

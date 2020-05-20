package com.jjw;

import java.util.ArrayList;
import java.util.List;

public class Test02 {

    public static void main(String[] args) {
        List list = new ArrayList();
        list.add(1);

        List list1 = list;
        System.out.println(list1.toString());

        list1 = new ArrayList();
        System.out.println(list1.toString());
        System.out.println(list.toString());
    }
}

package com.jjw.sparkCore.bean;

import java.io.Serializable;

public class Person implements Serializable{
    /**
     *
     */
    private static final long serialVersionUID = 5863629841241187531L;
    //	public static String isPerson = "yes";
    public static String isPerson = "";
    public String name;
    public String age ;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getAge() {
        return age;
    }
    public void setAge(String age) {
        this.age = age;
    }

}
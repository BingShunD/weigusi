package com.haizhi.weigusi.demo;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class EqualsDemo {
    private String name;
    private int age;

    @Override
    public int hashCode() {
        return Objects.hash(name,age);
    }

    @Override
    public boolean equals(Object obj) {
        EqualsDemo that = (EqualsDemo) obj;
        return name.equals(that.name) && age == that.age;
    }

    public EqualsDemo(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}

class TestEqualsDemo{
    public static void main(String[] args) {
        EqualsDemo person1 = new EqualsDemo("wulinfei",1);
        EqualsDemo person2 = new EqualsDemo("wulinfei",1);
        System.out.println("stu:" + person1.equals(person2));
        Set<EqualsDemo> set = new HashSet<>();
        set.add(person1);
        System.out.println("s1 hashCode:" + person1.hashCode());
        System.out.println("add s1 size:" + set.size());
        set.add(person2);
        System.out.println("s2 hashCode:" + person2.hashCode());
        System.out.println("add s2 size::" + set.size());
    }
}

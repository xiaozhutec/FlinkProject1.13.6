package com.johngo.domain;

/**
 * @author Johngo
 * @date 2022/4/8
 */

public class Person {
    public int id;
    public String name;
    public int age;
    public int sex;
    public String site;

    public Person() {
    }

    public Person(int id, String name, int age, int sex, String site) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.site = site;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", sex=" + sex +
                ", site='" + site + '\'' +
                '}';
    }
}

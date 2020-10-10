package cc.yuerblog.sql;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;

public class PersonBean implements Serializable {
    @JSONField(name="name")
    private String name;
    @JSONField(name="age")
    private int age;

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

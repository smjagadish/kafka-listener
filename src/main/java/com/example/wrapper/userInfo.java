package com.example.wrapper;

import com.example.mapinterface.objinf;
/*
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

 */

/*
Not used anymore
@Data
@AllArgsConstructor
@Builder
@NoArgsConstructor
 */
public class userInfo implements objinf {
    private int id;
    private String name;
    private String dept;

    public userInfo()
    {

    }
    public userInfo(int id, String name , String dept)
    {
        this.id = id;
        this.name = name;
        this.dept = dept;
    }
    @Override
    public int getid() {
        return this.id;
    }

    @Override
    public String getname() {
        return this.name;
    }

    @Override
    public String getdept() {
        return this.dept;
    }
}

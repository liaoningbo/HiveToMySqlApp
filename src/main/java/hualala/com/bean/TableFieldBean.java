package hualala.com.bean;

import java.io.Serializable;

/**
 * Creator:zhangpeng
 * Time:2018-09-30 9:21
 * Project: hiveToEsSpark
 * Description:
 */
public class TableFieldBean implements Serializable {
    private  String fieldName;
    private String fieldType;
    private String fieldComment="默认";
    private  boolean nullable;

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getFieldComment() {
        return fieldComment;
    }

    public void setFieldComment(String fieldComment) {
        this.fieldComment = fieldComment;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }
}

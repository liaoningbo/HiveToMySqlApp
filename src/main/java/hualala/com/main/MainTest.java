package hualala.com.main;

import hualala.com.bean.CommInsertBean;
import hualala.com.bean.TableFieldBean;
import hualala.com.dao.InsertCommDao;
import hualala.com.dao.impl.InsertCommDaoImpl;
import hualala.com.utils.Constants;
import hualala.com.utils.SqlUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Creator:Administrator
 * Time:2018-09-29 16:49
 * Project: hiveToEsSpark
 * Description:
 */
public class MainTest {

    //封装sql测试

    /**
     * `item_id` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '主键',
     `report_date` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '报表日期',
     `shop_id` bigint(20) DEFAULT NULL COMMENT '店铺编号',
     `shop_name` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '店铺名称'
     * @param args
     */
    public static void main(String[] args) {
        UpdateTable();





    }

    /**
     * [127782148_85807,
     2018-12-16 00:00:00,
     127782148,
     20181216,
     85807,
     0,
     4710772,
     RK201812160006,
     1081331,
     1,
     010101307,
     荷兰芹
     ,
     ,
     斤,
     1.00000000,
     10.00000000,
     9.31034483,
     null,
     0,
     100013272,
     周勇蔬菜,
     5058412,
     厨房部,
     10.00000000,
     10.00000000,
     10.00000000,
     0.07407407,
     null,
     无变化,
     0,
     6770858,
     仓库,
     201812]
     */
    public static   void UpdateTable(){
        List<CommInsertBean> insertList =new ArrayList<>();
        String fileds="item_id,report_date,detail_id,voucher_date,group_id,demand_type,demand_id,voucher_no,goods_id,voucher_type,goods_code,goods_name,goods_desc,standard_unit,goods_num,pre_tax_amount,pre_tax_avg,new_price,is_new_goods,supplier_id,supplier_name,house_id,house_name,tax_amount,tax_price,tax_avg,pre_tax_rate,new_tax_rate,action_status,bill_id,voucher_id,demand_name,pt";
        InsertCommDao dao=new InsertCommDaoImpl("mysql");
        CommInsertBean commInsertBean=new CommInsertBean();
        Object[] obj=new Object[33];
        obj[0]="127782148_85807";
        obj[1]="2018-12-16 00:00:00";
        obj[2]="127782148";
        obj[3]="20181216";
        obj[4]="85807";
        obj[5]="0";
        obj[6]="4710772";
        obj[7]="RK201812160006";
        obj[8]="1";
        obj[9]="010101307";
        obj[10]="0";
        obj[11]="荷兰芹";
        obj[12]="";
        obj[13]="斤";
        obj[14]="1.00000000";
        obj[15]="10.00000000";
        obj[16]="9.31034483";
        obj[17]="null";
        obj[18]="0";
        obj[19]="100013272";
        obj[20]="周勇蔬菜";
        obj[21]="5058412";
        obj[22]="厨房部";
        obj[23]="10.00000000";
        obj[24]="10.00000000";
        obj[25]="-0.04166667";
        obj[26]="-0.03947864";
        obj[27]="null";
        obj[28]="无变化";
        obj[29]="0";
        obj[30]="6770858";
        obj[31]="仓库";
        obj[32]="201812";
        commInsertBean.setObj(obj);
        insertList.add(commInsertBean);
        dao.batch(insertList,fileds,"mysql_aggr_supply_goods_price_day", Constants.DUPLICATE_SQL);
    }



    public  void createTable(){
        List<TableFieldBean> list=new ArrayList<>();
        TableFieldBean bean3=new TableFieldBean();
        bean3.setFieldName("item_id");
        bean3.setFieldType("string");
        bean3.setFieldComment("主键");
        list.add(bean3);
        TableFieldBean bean=new TableFieldBean();
        bean.setFieldName("report_date");
        bean.setFieldType("string");
        bean.setFieldComment("报表日期");
        list.add(bean);
        TableFieldBean bean1=new TableFieldBean();
        bean1.setFieldName("shop_id");
        bean1.setFieldType("bigint");
        bean1.setFieldComment("店铺编号");
        list.add(bean1);
        TableFieldBean bean2=new TableFieldBean();
        bean2.setFieldName("shop_name");
        bean2.setFieldType("string");
        bean2.setFieldComment("店铺名称");
        list.add(bean2);
       /* String createSql=SqlUtils.initCreateTableSql(list,"HbiTest","0","item_id");
        System.out.println(createSql);*/
        InsertCommDao dao=new InsertCommDaoImpl("mysql");
        dao.createTable(list,"HbiTest2","1","item_id");


    }


}

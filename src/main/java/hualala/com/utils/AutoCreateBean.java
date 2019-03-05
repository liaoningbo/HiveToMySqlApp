package hualala.com.utils;

import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Creator:Administrator
 * Time:2018-09-25 14:27
 * Project: supervisoryControl
 * Description:
 */
public class AutoCreateBean {
    //  mysql 驱动类
    private static final String DRIVER = "com.mysql.jdbc.Driver";

    //  数据库登录用户名
    private static final String USER = "root";

    //  数据库登录密码
    private static final String PASSWORD = "root";

    // 数据库连接地址
    private static final String URL = "jdbc:mysql://172.16.0.30:4000/hedw";

    private static String tablename;

    private String[] colnames; // 列名数组

    private String[] colTypes; // 列名类型数组

    private int[] colSizes; // 列名大小数组

    private boolean f_util = false; // 是否需要导入包java.util.*

    private boolean f_sql = false; // 是否需要导入包java.sql.*

    /**
     * 获取指定数据库中包含的表 TBlist
     *
     * @time 2016年3月4日下午5:54:52
     * @packageName com.util
     * @return 返回所有表名(将表名放到一个集合中)
     * @throws Exception
     */
    public List<String> TBlist() throws Exception {
        // 访问数据库 采用 JDBC方式
        Class.forName(DRIVER);

        Connection con = DriverManager.getConnection(URL, USER, PASSWORD);
        DatabaseMetaData md = con.getMetaData();

        List<String> list = null;

        ResultSet rs = md.getTables(null, null, null, null);
        if (rs != null) {
            list = new ArrayList<String>();
        }
        while (rs.next()) {
//            System.out.println("|表" + (i++) + ":" + rs.getString("TABLE_NAME"));
            String tableName = rs.getString("TABLE_NAME");
            list.add(tableName);
        }
        rs = null;
        md = null;
        con = null;
        return list;
    }

    public void GenEntity(List<String> TBlist, String packageName)throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSetMetaData rsmd = null;

        // 访问数据库 采用 JDBC方式
        Class.forName(DRIVER);
        conn = DriverManager.getConnection(URL, USER, PASSWORD);

        for (int k = 0; k < TBlist.size(); k++) {
            tablename = TBlist.get(k);
            String strsql = "select * from " + tablename;
            pstmt = conn.prepareStatement(strsql);
            rsmd = pstmt.getMetaData();
            int size = rsmd.getColumnCount();
            // 共有多少列
            colnames = new String[size];
            colTypes = new String[size];
            colSizes = new int[size];
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                colnames[i] = rsmd.getColumnName(i + 1);
                colTypes[i] = rsmd.getColumnTypeName(i + 1);
                if (colTypes[i].equalsIgnoreCase("datetime")) {
                    f_util = true;
                }
                if (colTypes[i].equalsIgnoreCase("image")
                        || colTypes[i].equalsIgnoreCase("text")) {
                    f_sql = true;
                }
                colSizes[i] = rsmd.getColumnDisplaySize(i + 1);
            }
            markerBean(initcap(tablename), parse(), packageName);
        }
        pstmt = null;
        rsmd = null;
        conn = null;
    }

    /**
     * 解析处理(生成实体类主体代码)
     */
    private String parse() {
        StringBuffer sb = new StringBuffer();
        if (f_util) {
            sb.append("import java.util.Date;\r\n");
        }
        if (f_sql) {
            sb.append("import java.sql.*;\r\n\r\n\r\n");
        }
        sb.append("public class " + initcap(tablename) + " {\r\n");
        processAllAttrs(sb);
        processAllMethod(sb);
        sb.append("}\r\n");

        return sb.toString();

    }

    /**
     * 创建java 文件 将生成的属性 get/set 方法 保存到 文件中 markerBean
     *
     * @time 2015年9月29日下午4:15:22
     * @packageName fanshe
     * @param className
     *            类名称
     * @param content
     *            类内容 包括属性 getset 方法
     */
    public void markerBean(String className, String content, String packageName) {
        String folder = System.getProperty("user.dir") + "/src/" + packageName + "/";

        File file = new File(folder);
        if (!file.exists()) {
            file.mkdirs();
        }
        String fileName = folder + className + ".java";

        try {
            File newdao = new File(fileName);
            FileWriter fw = new FileWriter(newdao);
            fw.write("package\t" + packageName.replace("/", ".") + ";\r\n");
            fw.write(content);
            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 生成所有的方法
     *
     * @param sb
     */
    private void processAllMethod(StringBuffer sb) {
        for (int i = 0; i < colnames.length; i++) {
            sb.append("\tpublic void set" + initcap(colnames[i]) + "("
                    + sqlType2JavaType(colTypes[i]) + " " + colnames[i]
                    + "){\r\n");
            sb.append("\t\tthis." + colnames[i] + " = " + colnames[i] + ";\r\n");
            sb.append("\t}\r\n");

            sb.append("\tpublic " + sqlType2JavaType(colTypes[i]) + " get"
                    + initcap(colnames[i]) + "(){\r\n");
            sb.append("\t\treturn " + colnames[i] + ";\r\n");
            sb.append("\t}\r\n");
        }
    }

    /**
     * 解析输出属性
     *
     * @return
     */
    private void processAllAttrs(StringBuffer sb) {
        for (int i = 0; i < colnames.length; i++) {
            sb.append("\tprivate " + sqlType2JavaType(colTypes[i]) + " " + colnames[i] + ";\r\n");

        }
    }

    /**
     * 把输入字符串的首字母改成大写
     *
     * @param str
     * @return
     */
    private String initcap(String str) {
        char[] ch = str.toCharArray();
        if (ch[0] >= 'a' && ch[0] <= 'z') {
            ch[0] = (char) (ch[0] - 32);
        }
        return new String(ch);
    }

    private String sqlType2JavaType(String sqlType) {
        if (sqlType.equalsIgnoreCase("bit")) {
            return "bool";
        } else if (sqlType.equalsIgnoreCase("tinyint")) {
            return "byte";
        } else if (sqlType.equalsIgnoreCase("smallint")) {
            return "short";
        } else if (sqlType.equalsIgnoreCase("int")) {
            return "int";
        } else if (sqlType.equalsIgnoreCase("bigint")) {
            return "long";
        } else if (sqlType.equalsIgnoreCase("float")) {
            return "float";
        } else if (sqlType.equalsIgnoreCase("decimal")
                || sqlType.equalsIgnoreCase("numeric")
                || sqlType.equalsIgnoreCase("real")) {
            return "double";
        } else if (sqlType.equalsIgnoreCase("money")
                || sqlType.equalsIgnoreCase("smallmoney")) {
            return "double";
        } else if (sqlType.equalsIgnoreCase("varchar")
                || sqlType.equalsIgnoreCase("char")
                || sqlType.equalsIgnoreCase("nvarchar")
                || sqlType.equalsIgnoreCase("nchar")) {
            return "String";
        } else if (sqlType.equalsIgnoreCase("datetime")
                ||sqlType.equalsIgnoreCase("date")){
            return "Date";
        }

        else if (sqlType.equalsIgnoreCase("image")) {
            return "Blob";
        } else if (sqlType.equalsIgnoreCase("text")) {
            return "Clob";
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        AutoCreateBean auto = new AutoCreateBean();
        List<String> list = auto.TBlist();
        auto.GenEntity(list, "main/java/hualala/com/bean");

    }


}

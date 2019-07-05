package com.kgc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JavaHbaseAPI {

    /**
     * 创建表 !admin.isTableAvailable(tableName)
     * new HTableDescriptor(tableName)
     * addFamily(new HColumnDescriptor("name"));
     * admin.createTable(hTableDescriptor);
     */

    public void createTable() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hbase-site.xml"));
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("people");
            if (!admin.isTableAvailable(tableName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                hTableDescriptor.addFamily(new HColumnDescriptor("name"));
                hTableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
                hTableDescriptor.addFamily(new HColumnDescriptor("personalinfo"));
                admin.createTable(hTableDescriptor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 删除表 disableTable(tableName)
     * deleteTable(tableName)
     */
    public void deleteTable() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hbase-site.xml"));
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("people");
            if (admin.isTableAvailable(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("删除成功!");
            } else {
                System.out.println("并无此表");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 查询库表列表  HTableDescriptor[] hTableDescriptors = admin.listTables();
     * hTableDescriptors[i].getTableName()
     */
    public void listTables() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hbase-site.xml"));
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            HTableDescriptor[] hTableDescriptors = admin.listTables();
            for (int i = 0; i < hTableDescriptors.length; i++) {
                System.out.println(hTableDescriptors[i].getTableName());
                System.out.println(hTableDescriptors[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 插入数据   String[][] people={{"1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26"},...}
     * Put put = new Put(Bytes.toBytes(people[i][0]));
     * put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("frist"), Bytes.toBytes(people[i][1]));
     * ...
     * put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("frist"), Bytes.toBytes(people[i][n]));
     * table.put(put);
     */
    public void insertResouce() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hbase-site.xml"));
        String[][] people = {
                {"1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26"},
                {"2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24"},
                {"3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27"},
                {"4", "Rae", "Schroeder", "rae@xyz.com", "F", "31"},
                {"5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25"},
                {"6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24"}};
        Connection connection = null;
        Admin admin = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            TableName tableName = TableName.valueOf("people");
            admin = connection.getAdmin();
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(tableName);
                for (int i = 0; i < people.length; i++) {
                    Put put = new Put(Bytes.toBytes(people[i][0]));
                    put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("frist"), Bytes.toBytes(people[i][1]));
                    put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(people[i][2]));
                    put.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
                    put.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("sex"), Bytes.toBytes(people[i][4]));
                        put.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"), Bytes.toBytes(people[i][5]));
                    table.put(put);
                }
            } else {
                System.out.println("并无此表");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 获取某个Rowkey的数据  Get get = new Get(Bytes.toBytes("列值"));
     * result = table.get(get);
     * byte[] firstname = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("frist"));
     * String firstName = Bytes.toString(firstname);
     */
    public void getResource() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hbase-site.xml"));
        Connection connection = null;
        Admin admin = null;
        Result result = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("people");
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(tableName);
                Get get = new Get(Bytes.toBytes("3"));
                result = table.get(get);
                byte[] firstname = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("frist"));
                String firstName = Bytes.toString(firstname);
                System.out.println(firstName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 删除数据  List<Delete> list = new ArrayList<>();
     * Delete delete = new Delete(Bytes.toBytes(people[i][0]));
     * list.add(delete);
     * table.delete(list);
     */
    public void deleteData() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hbase-site.xml"));
        String[][] people = {
                {"1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26"},
                {"2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24"},
                {"3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27"},
                {"4", "Rae", "Schroeder", "rae@xyz.com", "F", "31"},
                {"5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25"},
                {"6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24"}};
        Connection connection = null;
        Admin admin = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();

            TableName tableName = TableName.valueOf("people");
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(tableName);
                List<Delete> list = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    Delete delete = new Delete(Bytes.toBytes(people[i][0]));
                    list.add(delete);
                }
                table.delete(list);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    /**
     * 显示表数据   Scan scan = new Scan();
     * FilterList filterList= new FilterList();
     * SingleColumnValueFilter singleColumnValueFilter1= new
     * SingleColumnValueFilter(Bytes.toBytes("personalinfo"),
     * Bytes.toBytes("sex"), CompareFilter.CompareOp.EQUAL,
     * Bytes.toBytes("F"));     --筛选条件
     * scan.setFilter(filterList);
     * resultScanner=table.getScanner(scan);
     * Result result= resultScanner.next();
     * byte[] firstname = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("frist"));
     * System.out.println("姓名:"+Bytes.toString(firstname))
     */
    public void scanData() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("core-site.xml"));
        configuration.addResource(new Path("hbase-site.xml"));
        Connection connection = null;
        Admin admin = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("people");
            if (admin.isTableAvailable(tableName)) {
                table = connection.getTable(tableName);
                Scan scan = new Scan();
                FilterList filterList = new FilterList();
                SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("personalinfo"), Bytes.toBytes("sex"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("F"));
                SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("25"));
                filterList.addFilter(singleColumnValueFilter1);
                filterList.addFilter(singleColumnValueFilter2);
                scan.setFilter(filterList);
                resultScanner = table.getScanner(scan);
            }
            Result result = resultScanner.next();
            while (result != null) {
                byte[] firstname = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("frist"));
                byte[] lastname = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("last"));
                byte[] contactinfo_email = result.getValue(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
                byte[] personalinfo_sex = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("sex"));
                byte[] personalinfo_age = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));
                System.out.println("姓名:" + Bytes.toString(firstname) + " " + Bytes.toString(lastname)
                        + "  邮箱为:" + Bytes.toString(contactinfo_email) + "  性别: " + Bytes.toString(personalinfo_sex) +
                        " 年龄:" + Bytes.toString(personalinfo_age));
                result = resultScanner.next();

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}



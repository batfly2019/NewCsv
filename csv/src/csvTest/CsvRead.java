package csvTest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.csvreader.CsvReader;

public class CsvRead {
	private final static String path = "D:\\mydata\\collect_data\\";
	private final static String prefix = "custom";
	private final static String suffix = ".csv";
	private final static String paths = path+prefix+suffix;
	
	private final static String cfn = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private final static String url = "jdbc:sqlserver://localhost:1433;DatabaseName=collect_test";
	
	Connection con = null;
	PreparedStatement pre = null;
	int rs = 0;
	
	public static void main(String[] args) {
		CsvRead csv = new CsvRead();
		csv.readCsv();
	}
	
	public void insert(List<String[]> list) {
		try {
			Class.forName(cfn);
			con = DriverManager.getConnection(url, "sa" , "123456");
			String sql = "INSERT INTO [collect_test].[dbo].[2006x]([序号],[月度],[进出口],[商品代码],[商品名称],[金额],[数量],[单位],[企业代码],[企业名称],[地址],[电话],[传真],[邮政编码],[电子邮件],[联系人],[消费地区(进口)/生产地区(出口)],[企业类型],[原产国(进口)/目的国(出口)],[海关口岸],[贸易方式],[运输方式],[中转国]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
			pre = con.prepareStatement(sql);
			for(int row=0;row<list.size();row++) {
				int length = list.get(row).length;
				System.out.println(list.get(row)[0]);
				for(int i=0;i<length;i++) {
					pre.setString(i+1, list.get(row)[i]);
				}
				rs = pre.executeUpdate();
			}
			if(rs != 0) {
				System.out.println("插入成功！");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void readCsv() {
		List<String[]> list = new ArrayList<String[]>();
		CsvReader reader;
		try {
			reader = new CsvReader(paths, ',', Charset.forName("gb2312"));
			reader.readHeaders();
			while(reader.readRecord()) {
				list.add(reader.getValues());
				if(list.size() % 10000 == 0) {
					insert(list);
					list.clear();
				}
				//insert(list);
				//list.clear();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

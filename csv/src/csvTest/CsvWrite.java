package csvTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.csvreader.CsvWriter;

public class CsvWrite {
	/*
	 * 数据库连接相关参数也可设置成常量
	 */
	private final static String cfn = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private final static String url = "jdbc:sqlserver://localhost:1433;DatabaseName=collect_test";
	
	/*
	 * 文件路劲名分割设置成常量
	 * 为可能设置多个输出文件做准备
	 */
	private final static String path = "D:\\mydata\\collect_data\\";
	private final static String prefix = "HaiGuan";
	private final static String suffix = ".csv";
	
	/*
	 * 初始化jdbc连接数据库过程中的对象
	 */
	Connection con = null;
	PreparedStatement pre = null;
	ResultSet rs = null;
	
	/*
	 * 这里只设置一个csv文件即可将文件路径名设置为常量
	 */
	private final static String paths = path+prefix+suffix;
	
	public static void main(String[] args) {
		Date d1 = new Date();
		System.out.println("开始时间为："+d1.getTime());
		
		CsvWrite csv = new CsvWrite();
		csv.createFile();
		csv.connectOut();
		
		Date d2 = new Date();
		System.out.println("结束时间为："+d2.getTime());
		System.out.println("总耗时："+(d2.getTime()-d1.getTime()));
	}
	
	/**
	 * 连接数据库并输出到csv
	 */
	public void connectOut() {
		//数组链组成的数组方便查询
		List<String[]> list = new ArrayList<String[]>();
		try {
			//加载数据库驱动
			Class.forName(cfn);
			//实例化连接对象--连接指定数据库
			con = DriverManager.getConnection(url, "sa", "123456");
			System.out.println("连接指定数据库成功！");
			//创建查询数据总量的sql
			String sql1 = "SELECT count(*) FROM [HaiGuan].[dbo].[Content];";
			//执行查询
			pre = con.prepareStatement(sql1);
			rs = pre.executeQuery();
			//遍历结果集
			rs.next();
			//获取结果集的数据
			int count = rs.getInt(1);
			System.out.println("数据总条数为："+count);
			//分页数
			int index = count/10000 + 1;
			//时间初始化--方便计算每次操作的时间
			Date d1 = null;
			Date d2 = null;
			//输入文件头
			CsvWriter wr = new CsvWriter(paths,',', Charset.forName("gb2312"));
			String[] header = {"Id","商品编码","商品名称","贸易伙伴编码","贸易伙伴名称","贸易方式编码","贸易方式名称","注册地编码","注册地名称","第一数量","第一计量单位","第二数量","第二计量单位","美元","创建时间","year","startMonth","endMonth"};
			wr.writeRecord(header);
			/*
			 * 遍历每页
			 */
			for(int i = 0; i < index;i++) {
				
				//初步设计份文件存储--但有问题，先注释掉
				/*//设计每二十次查询得到的数据放在一个csv文件
				String paths = path+prefix+(i/30+1)+suffix;
				//创建文件
				createFile();
				//实例化CsvWriter
				CsvWriter wr = new CsvWriter(paths,',', Charset.forName("gb2312"));
				//创建文件后输入表头
				if(i%30 == 0) {
					String[] header = {"序号","月度","进出口","商品代码","商品名称","金额","数量","单位","企业代码","企业名称","地址","电话","传真","邮政编码","电子邮件","联系人","消费地区(进口)/生产地区(出口)","企业类型","原产国(进口)/目的国(出口)","海关口岸","贸易方式","运输方式","中转国"};
					wr.writeRecord(header);
				}*/
				
				//开始时间
				d1 = new Date();
				//创建分页查询的sql--10000一分
				String sql = "SELECT [Id]" + 
						",[商品编码]" + 
						",[商品名称]" + 
						",[贸易伙伴编码]" + 
						",[贸易伙伴名称]" + 
						",[贸易方式编码]" + 
						",[贸易方式名称]" + 
						",[注册地编码]" + 
						",[注册地名称]" + 
						",[第一数量]" + 
						",[第一计量单位]" + 
						",[第二数量]" + 
						",[第二计量单位]" + 
						",[美元]" + 
						",[创建时间]" + 
						",[year]" + 
						",[startMonth]" + 
						",[endMonth]" + 
						"FROM [HaiGuan].[dbo].[Content] WHERE [Id] " + 
						"BETWEEN " + 
						(1+i*10000) + 
						"AND " + 
						(1+i)*10000 +
						"ORDER BY [Id];";
				//实例化预编译对象
				pre = con.prepareStatement(sql);
				//执行sql
				rs = pre.executeQuery();
				//调用遍历结果集获取数据的方法
				select(rs, list, wr);
				
				d2 = new Date();
				System.out.println("第"+(i+1)+"查询并输出消耗时间为：" + (d2.getTime()-d1.getTime()) + "ms!");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 遍历结果集获取数据
	 * @param rs
	 * @param list
	 * @param wr
	 */
	public void select(ResultSet rs,List<String[]> list,CsvWriter wr) {
		//初始化数组对象s
		String[] s = null;
		try {
			//遍历结果集
			while(rs.next()) {
				//每次创建新的数组对象来存储数据
				s = new String[18];
				s[0] = rs.getString("Id");
				s[1] = rs.getString("商品编码");
				s[2] = rs.getString("商品名称");
				s[3] = rs.getString("贸易伙伴编码");
				s[4] = rs.getString("贸易伙伴名称");
				s[5] = rs.getString("贸易方式编码");
				s[6] = rs.getString("贸易方式名称");
				s[7] = rs.getString("注册地编码");
				s[8] = rs.getString("注册地名称");
				s[9] = rs.getString("第一数量");
				s[10] = rs.getString("第一计量单位");
				s[11] = rs.getString("第二数量");
				s[12] = rs.getString("第二计量单位");
				s[13] = rs.getString("美元");
				s[14] = rs.getString("创建时间");
				s[15] = rs.getString("year");
				s[16] = rs.getString("startMonth");
				s[17] = rs.getString("endMonth");
				//将每行数据的数组添加到集合
				list.add(s);
			}
			//遍历输出csv文件
		    for(int i=0;i<list.size();i++) {
		    	String[] Data= list.get(i);
		    	wr.writeRecord(Data);
		    }
		    //清空集合
		    list.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建文件
	 */
	public void createFile() {
		File file = new File(paths);
		if(!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("文件已创建！");
		}else {
			System.out.println("文件已存在！");
		}
	}
}

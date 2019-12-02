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
import java.util.Date;

import com.csvreader.CsvWriter;

public class Csv {
	/*
	 * 数据库连接相关参数也可设置成常量
	 */
	private final static String cfn = "com.mysql.jdbc.Driver";
	private final static String url = "jdbc:mysql://localhost:3306/custom?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=convertToNull";
	
	/*
	 * 文件路劲名分割设置成常量
	 * 为可能设置多个输出文件做准备
	 */
	private final static String path = "D:\\mydata\\collect_data\\min_custom\\";
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
	private String paths = null;
	
	String[] header = {"Id","商品编码","商品名称","贸易伙伴编码","贸易伙伴名称","贸易方式编码","贸易方式名称","注册地编码","注册地名称","第一数量","第一计量单位","第二数量","第二计量单位","美元","创建时间","year","startMonth","endMonth"};
	
	CsvWriter wr = null;
	
	String sql = "SELECT Id" + 
			",商品编码" + 
			",商品名称" + 
			",贸易伙伴编码" + 
			",贸易伙伴名称" + 
			",贸易方式编码" + 
			",贸易方式名称" + 
			",注册地编码" + 
			",注册地名称" + 
			",第一数量" + 
			",第一计量单位" + 
			",第二数量" + 
			",第二计量单位" + 
			",美元" + 
			",创建时间" + 
			",year" + 
			",startMonth" + 
			",endMonth" + 
			" FROM haiguan WHERE Id BETWEEN ? AND ? ORDER BY Id;";
	
	public static void main(String[] args) throws Exception {
		Date d1 = new Date();
		System.out.println("开始时间为："+d1.getTime());
		
		Csv csv = new Csv();
		csv.connectOut();
		
		Date d2 = new Date();
		System.out.println("结束时间为："+d2.getTime());
		System.out.println("总耗时："+(d2.getTime()-d1.getTime()));
	}
	
	/**
	 * 连接数据库并输出到csv
	 * @throws Exception 
	 */
	public void connectOut() throws Exception {
		//数组链组成的数组方便查询
		try {
			//加载数据库驱动
			Class.forName(cfn);
			//实例化连接对象--连接指定数据库
			con = DriverManager.getConnection(url, "root", "zlj960101");
			System.out.println("连接指定数据库成功！");
			//创建查询数据总量的sql
			String sql1 = "SELECT count(*) FROM haiguan;";
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
			/*
			 * 遍历每页
			 */
			for(int i = 0; i < index;i++) {
				//设计每十次查询得到的数据放在一个csv文件
				if(i%10 == 0) {
					paths = path+prefix+(i/10+1)+suffix;
					if(wr != null){
						wr.close();
					}
					if(i % 100 ==0) {
						System.gc();
					}
					//创建文件
					createFile(paths);
					//实例化CsvWriter
					wr = new CsvWriter(paths,',', Charset.forName("gb2312"));
					//创建文件后输入表头
					wr.writeRecord(header);
				}
				//实例化预编译对象
				pre = con.prepareStatement(sql);
				pre.setInt(1, (1+i*10000));
				pre.setInt(2, (1+i)*10000);
				//执行sql
				rs = pre.executeQuery();
				
				//调用遍历结果集获取数据的方法
				select(rs, wr);
				closeConn(pre, rs);
				//wr.close();
//				d2 = new Date();
//				System.out.println("第"+(i+1)+"查询并输出消耗时间为：" + (d2.getTime()-d1.getTime()) + "ms!");
				System.out.println("第"+(i+1)+"查询并输出!");
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
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(pre != null) {
					pre.close();
				}
				if(con != null) {
					con.close();
				}
				if(wr != null) {
					wr.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 将预编译对象和结果集对象关闭---避免太多对象的存留
	 * @param pre
	 * @param rs
	 * @throws Exception
	 */
	public void closeConn(PreparedStatement pre,ResultSet rs) throws Exception{
		if(rs != null) {
			rs.close();
		}
		if(pre != null) {
			pre.close();
		}
	}
	
	/**
	 * 遍历结果集获取数据
	 * @param rs
	 * @param list
	 * @param wr
	 */
	public void select(ResultSet rs,CsvWriter wr) {
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
				s[14] = rs.getTimestamp("创建时间")==null? "":rs.getTimestamp("创建时间").toString();
				s[15] = rs.getString("year");
				s[16] = rs.getString("startMonth");
				s[17] = rs.getString("endMonth");
				//将每行数据的数组添加到集合
				wr.writeRecord(s);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建文件
	 */
	public void createFile(String paths) {
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
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
	 * ���ݿ�������ز���Ҳ�����óɳ���
	 */
	private final static String cfn = "com.mysql.jdbc.Driver";
	private final static String url = "jdbc:mysql://localhost:3306/custom?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=convertToNull";
	
	/*
	 * �ļ�·�����ָ����óɳ���
	 * Ϊ�������ö������ļ���׼��
	 */
	private final static String path = "D:\\mydata\\collect_data\\min_custom\\";
	private final static String prefix = "HaiGuan";
	private final static String suffix = ".csv";
	
	/*
	 * ��ʼ��jdbc�������ݿ�����еĶ���
	 */
	Connection con = null;
	PreparedStatement pre = null;
	ResultSet rs = null;
	
	/*
	 * ����ֻ����һ��csv�ļ����ɽ��ļ�·��������Ϊ����
	 */
	private String paths = null;
	
	String[] header = {"Id","��Ʒ����","��Ʒ����","ó�׻�����","ó�׻������","ó�׷�ʽ����","ó�׷�ʽ����","ע��ر���","ע�������","��һ����","��һ������λ","�ڶ�����","�ڶ�������λ","��Ԫ","����ʱ��","year","startMonth","endMonth"};
	
	CsvWriter wr = null;
	
	String sql = "SELECT Id" + 
			",��Ʒ����" + 
			",��Ʒ����" + 
			",ó�׻�����" + 
			",ó�׻������" + 
			",ó�׷�ʽ����" + 
			",ó�׷�ʽ����" + 
			",ע��ر���" + 
			",ע�������" + 
			",��һ����" + 
			",��һ������λ" + 
			",�ڶ�����" + 
			",�ڶ�������λ" + 
			",��Ԫ" + 
			",����ʱ��" + 
			",year" + 
			",startMonth" + 
			",endMonth" + 
			" FROM haiguan WHERE Id BETWEEN ? AND ? ORDER BY Id;";
	
	public static void main(String[] args) throws Exception {
		Date d1 = new Date();
		System.out.println("��ʼʱ��Ϊ��"+d1.getTime());
		
		Csv csv = new Csv();
		csv.connectOut();
		
		Date d2 = new Date();
		System.out.println("����ʱ��Ϊ��"+d2.getTime());
		System.out.println("�ܺ�ʱ��"+(d2.getTime()-d1.getTime()));
	}
	
	/**
	 * �������ݿⲢ�����csv
	 * @throws Exception 
	 */
	public void connectOut() throws Exception {
		//��������ɵ����鷽���ѯ
		try {
			//�������ݿ�����
			Class.forName(cfn);
			//ʵ�������Ӷ���--����ָ�����ݿ�
			con = DriverManager.getConnection(url, "root", "zlj960101");
			System.out.println("����ָ�����ݿ�ɹ���");
			//������ѯ����������sql
			String sql1 = "SELECT count(*) FROM haiguan;";
			//ִ�в�ѯ
			pre = con.prepareStatement(sql1);
			rs = pre.executeQuery();
			//���������
			rs.next();
			//��ȡ�����������
			int count = rs.getInt(1);
			System.out.println("����������Ϊ��"+count);
			//��ҳ��
			int index = count/10000 + 1;
			/*
			 * ����ÿҳ
			 */
			for(int i = 0; i < index;i++) {
				//���ÿʮ�β�ѯ�õ������ݷ���һ��csv�ļ�
				if(i%10 == 0) {
					paths = path+prefix+(i/10+1)+suffix;
					if(wr != null){
						wr.close();
					}
					if(i % 100 ==0) {
						System.gc();
					}
					//�����ļ�
					createFile(paths);
					//ʵ����CsvWriter
					wr = new CsvWriter(paths,',', Charset.forName("gb2312"));
					//�����ļ��������ͷ
					wr.writeRecord(header);
				}
				//ʵ����Ԥ�������
				pre = con.prepareStatement(sql);
				pre.setInt(1, (1+i*10000));
				pre.setInt(2, (1+i)*10000);
				//ִ��sql
				rs = pre.executeQuery();
				
				//���ñ����������ȡ���ݵķ���
				select(rs, wr);
				closeConn(pre, rs);
				//wr.close();
//				d2 = new Date();
//				System.out.println("��"+(i+1)+"��ѯ���������ʱ��Ϊ��" + (d2.getTime()-d1.getTime()) + "ms!");
				System.out.println("��"+(i+1)+"��ѯ�����!");
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
	 * ��Ԥ�������ͽ��������ر�---����̫�����Ĵ���
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
	 * �����������ȡ����
	 * @param rs
	 * @param list
	 * @param wr
	 */
	public void select(ResultSet rs,CsvWriter wr) {
		//��ʼ���������s
		String[] s = null;
		try {
			//���������
			while(rs.next()) {
				//ÿ�δ����µ�����������洢����
				s = new String[18];
				s[0] = rs.getString("Id");
				s[1] = rs.getString("��Ʒ����");
				s[2] = rs.getString("��Ʒ����");
				s[3] = rs.getString("ó�׻�����");
				s[4] = rs.getString("ó�׻������");
				s[5] = rs.getString("ó�׷�ʽ����");
				s[6] = rs.getString("ó�׷�ʽ����");
				s[7] = rs.getString("ע��ر���");
				s[8] = rs.getString("ע�������");
				s[9] = rs.getString("��һ����");
				s[10] = rs.getString("��һ������λ");
				s[11] = rs.getString("�ڶ�����");
				s[12] = rs.getString("�ڶ�������λ");
				s[13] = rs.getString("��Ԫ");
				s[14] = rs.getTimestamp("����ʱ��")==null? "":rs.getTimestamp("����ʱ��").toString();
				s[15] = rs.getString("year");
				s[16] = rs.getString("startMonth");
				s[17] = rs.getString("endMonth");
				//��ÿ�����ݵ�������ӵ�����
				wr.writeRecord(s);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * �����ļ�
	 */
	public void createFile(String paths) {
		File file = new File(paths);
		if(!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("�ļ��Ѵ�����");
		}else {
			System.out.println("�ļ��Ѵ��ڣ�");
		}
	}
}
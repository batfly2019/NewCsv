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
	 * ���ݿ�������ز���Ҳ�����óɳ���
	 */
	private final static String cfn = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private final static String url = "jdbc:sqlserver://localhost:1433;DatabaseName=collect_test";
	
	/*
	 * �ļ�·�����ָ����óɳ���
	 * Ϊ�������ö������ļ���׼��
	 */
	private final static String path = "D:\\mydata\\collect_data\\";
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
	private final static String paths = path+prefix+suffix;
	
	public static void main(String[] args) {
		Date d1 = new Date();
		System.out.println("��ʼʱ��Ϊ��"+d1.getTime());
		
		CsvWrite csv = new CsvWrite();
		csv.createFile();
		csv.connectOut();
		
		Date d2 = new Date();
		System.out.println("����ʱ��Ϊ��"+d2.getTime());
		System.out.println("�ܺ�ʱ��"+(d2.getTime()-d1.getTime()));
	}
	
	/**
	 * �������ݿⲢ�����csv
	 */
	public void connectOut() {
		//��������ɵ����鷽���ѯ
		List<String[]> list = new ArrayList<String[]>();
		try {
			//�������ݿ�����
			Class.forName(cfn);
			//ʵ�������Ӷ���--����ָ�����ݿ�
			con = DriverManager.getConnection(url, "sa", "123456");
			System.out.println("����ָ�����ݿ�ɹ���");
			//������ѯ����������sql
			String sql1 = "SELECT count(*) FROM [HaiGuan].[dbo].[Content];";
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
			//ʱ���ʼ��--�������ÿ�β�����ʱ��
			Date d1 = null;
			Date d2 = null;
			//�����ļ�ͷ
			CsvWriter wr = new CsvWriter(paths,',', Charset.forName("gb2312"));
			String[] header = {"Id","��Ʒ����","��Ʒ����","ó�׻�����","ó�׻������","ó�׷�ʽ����","ó�׷�ʽ����","ע��ر���","ע�������","��һ����","��һ������λ","�ڶ�����","�ڶ�������λ","��Ԫ","����ʱ��","year","startMonth","endMonth"};
			wr.writeRecord(header);
			/*
			 * ����ÿҳ
			 */
			for(int i = 0; i < index;i++) {
				
				//������Ʒ��ļ��洢--�������⣬��ע�͵�
				/*//���ÿ��ʮ�β�ѯ�õ������ݷ���һ��csv�ļ�
				String paths = path+prefix+(i/30+1)+suffix;
				//�����ļ�
				createFile();
				//ʵ����CsvWriter
				CsvWriter wr = new CsvWriter(paths,',', Charset.forName("gb2312"));
				//�����ļ��������ͷ
				if(i%30 == 0) {
					String[] header = {"���","�¶�","������","��Ʒ����","��Ʒ����","���","����","��λ","��ҵ����","��ҵ����","��ַ","�绰","����","��������","�����ʼ�","��ϵ��","���ѵ���(����)/��������(����)","��ҵ����","ԭ����(����)/Ŀ�Ĺ�(����)","���ؿڰ�","ó�׷�ʽ","���䷽ʽ","��ת��"};
					wr.writeRecord(header);
				}*/
				
				//��ʼʱ��
				d1 = new Date();
				//������ҳ��ѯ��sql--10000һ��
				String sql = "SELECT [Id]" + 
						",[��Ʒ����]" + 
						",[��Ʒ����]" + 
						",[ó�׻�����]" + 
						",[ó�׻������]" + 
						",[ó�׷�ʽ����]" + 
						",[ó�׷�ʽ����]" + 
						",[ע��ر���]" + 
						",[ע�������]" + 
						",[��һ����]" + 
						",[��һ������λ]" + 
						",[�ڶ�����]" + 
						",[�ڶ�������λ]" + 
						",[��Ԫ]" + 
						",[����ʱ��]" + 
						",[year]" + 
						",[startMonth]" + 
						",[endMonth]" + 
						"FROM [HaiGuan].[dbo].[Content] WHERE [Id] " + 
						"BETWEEN " + 
						(1+i*10000) + 
						"AND " + 
						(1+i)*10000 +
						"ORDER BY [Id];";
				//ʵ����Ԥ�������
				pre = con.prepareStatement(sql);
				//ִ��sql
				rs = pre.executeQuery();
				//���ñ����������ȡ���ݵķ���
				select(rs, list, wr);
				
				d2 = new Date();
				System.out.println("��"+(i+1)+"��ѯ���������ʱ��Ϊ��" + (d2.getTime()-d1.getTime()) + "ms!");
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
	 * �����������ȡ����
	 * @param rs
	 * @param list
	 * @param wr
	 */
	public void select(ResultSet rs,List<String[]> list,CsvWriter wr) {
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
				s[14] = rs.getString("����ʱ��");
				s[15] = rs.getString("year");
				s[16] = rs.getString("startMonth");
				s[17] = rs.getString("endMonth");
				//��ÿ�����ݵ�������ӵ�����
				list.add(s);
			}
			//�������csv�ļ�
		    for(int i=0;i<list.size();i++) {
		    	String[] Data= list.get(i);
		    	wr.writeRecord(Data);
		    }
		    //��ռ���
		    list.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * �����ļ�
	 */
	public void createFile() {
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

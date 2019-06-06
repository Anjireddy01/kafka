import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


public class deptinfo {
    public static void main(String args[]){
            long tmill=System.currentTimeMillis();
            String FILENAME = "/home/oracle-odi/kafka/confluent-5.0.1/abc/dept/test/test.json";
                System.out.println(FILENAME);
                testdb dbData = new testdb();
                List<Dept> deptList = dbData.getDeptDetails();
            try {

                FileWriter fw = new FileWriter(FILENAME, false);
                BufferedWriter bw = new BufferedWriter(fw);
                String header ="{\"value_schema\": \"{\\\"type\\\": \\\"record\\\",\\\"name\\\": \\\"deptinfo\\\",\\\"fields\\\" : [{\\\"name\\\": \\\"deptno\\\", \\\"type\\\": \\\"long\\\"},{\\\"name\\\": \\\"dname\\\", \\\"type\\\": \\\"string\\\"},{\\\"name\\\": \\\"loc\\\", \\\"type\\\": \\\"string\\\"},{\\\"name\\\": \\\"date\\\", \\\"type\\\": \\\"string\\\"}]}\", \"records\": [";
                //String header = "{\\\"value_schema\\\": \\\"{\\\"type\\\": \\\"record\\\",\\\"name\\\": \\\"emp\\\",\\\"fields\\\" : [{\\\"name\\\": \\\"empid\\\", \\\"type\\\": \\\"long\\\"},{\\\"name\\\": \\\"empname\\\", \\\"type\\\": \\\"string\\\"},{\\\"name\\\": \\\"gender\\\", \\\"type\\\": \\\"string\\\"},{\\\"name\\\": \\\"dob\\\", \\\"type\\\": \\\"string\\\"},{\\\"name\\\": \\\"salary\\\", \\\"type\\\": \\\"double\\\"},{\\\"name\\\": \\\"worklocation\\\",\\\"type\\\":\\\"string\\\",\\\"default\\\":\\\"null\\\"}]}\", \"records\": [";
                bw.write(header);
                int count = 0;
                //bw.newLine();
                //System.out.println(header);
                for (Dept dept : deptList) {
                    String content = null;
                    String loc=dept.getLoc();
                    String dname=dept.getDname();
                    String date=dept.getDate();
                    int deptno=dept.getDeptno();
                                        if(count==0)
                                        {
                                                 content = "{\"value\": {\"deptno\": "+deptno+",\"dname\":\""+dname+"\",\"loc\":\""+loc+"\",\"date\":\""+date+"\"}}";
                                                 count++;
                                        }
                                        else
                                        {
                                                content = ",{\"value\": {\"deptno\": "+deptno+",\"dname\":\""+dname+"\",\"loc\":\""+loc+"\",\"date\":\""+date+"\"}}";
                                        }
                   // System.out.println(content+"},");
                    bw.write(content);
                   // bw.newLine();
                }
                bw.write("]}");
                //System.out.println("}]}");
                //System.out.println("Done");
                                    if (bw != null)
                                          bw.close();
                                    if (fw != null)
                          fw.close();

                }
                 catch (IOException e)
                        {
                        e.printStackTrace();
                }

                        File f1=new File(FILENAME);
                        f1.setExecutable(true);
                        f1.setReadable(true);
                        f1.setWritable(true);

                    ProcessBuilder processBuilder = new ProcessBuilder();
                    processBuilder.command(FILENAME);

                           try
                             {
                                 Process process = processBuilder.start();
                                 StringBuilder output = new StringBuilder();
                                 BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                                 String line;

                                while ((line = reader.readLine()) != null)
                                {
                                    output.append(line + "\n");
                                        System.out.println(line);
                                }
                                   int exitVal = process.waitFor();
                                   if (exitVal == 0)
                                        {
                                           System.out.println("Success!");
                                           String result = "{["+output+"]}";
                                           System.out.println(result);
                                           System.exit(0);
                                   }
                                        else
                                        {
                                           //abnormal...
                                   }

                           } catch (IOException e) {
                                   e.printStackTrace();
                           } catch (InterruptedException e) {
                                   e.printStackTrace();
                           }

        }




}

class Dept {


        public String getDname() {
                return dname;
        }
        public void setDname(String dname) {
                this.dname = dname;
        }
        public String getLoc() {
                return loc;
        }
        public void setLoc(String loc) {
                this.loc = loc;
        }
        public String getDate() {
                return date;
        }
        public void setDate(String date) {
                this.date = date;
        }
        public Integer getDeptno() {
                return deptno;
        }
        public void setDeptno(Integer deptno) {
                this.deptno = deptno;
        }
        Integer deptno;
        String dname;
        String loc;
        String date;

                }

class testdb {
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
   static final String DB_URL = "jdbc:oracle:thin:@192.168.0.120:1522:orcl9";

   //  Database credentials
   static final String USER = "project";
   static final String PASS = "Welcome123";
   List <Dept> deptList = new ArrayList <Dept>();

   public List<Dept> getDeptDetails() {
   Connection conn = null;
   Statement stmt = null;
   try{
      //STEP 2: Register JDBC driver
      Class.forName("oracle.jdbc.driver.OracleDriver");

      //STEP 3: Open a connection
      System.out.println("Connecting to a selected database...");
      conn = DriverManager.getConnection(DB_URL, USER, PASS);
      System.out.println("Connected database successfully...");

      //STEP 4: Execute a query
      System.out.println("Creating statement...");
      stmt = conn.createStatement();
      String sql = "SELECT * FROM deptinfo";
      ResultSet rs = stmt.executeQuery(sql);
      //STEP 5: Extract data from result set
      while(rs.next()){
         //Retrieve by column name
                                 int deptno  = rs.getInt("deptno");
                                 String dname = rs.getString("dname");
                                 String loc = rs.getString("loc");
                 String date = rs.getString("date");
                 Dept dept = new Dept();
                 dept.setDeptno(deptno);
                 dept.setDname(dname);
                 dept.setLoc(loc);
                 dept.setDate(date);
                 deptList.add(dept);
      }
      rs.close();
   }catch(SQLException se){
      //Handle errors for JDBC
      se.printStackTrace();
   }catch(Exception e){
      //Handle errors for Class.forName
      e.printStackTrace();
   }finally{
      //finally block used to close resources
      try{
         if(stmt!=null)
            conn.close();
      }catch(SQLException se){
      }// do nothing
      try{
         if(conn!=null)
            conn.close();
      }catch(SQLException se){
         se.printStackTrace();
      }//end finally try
   }//end try
    return deptList;
}



}


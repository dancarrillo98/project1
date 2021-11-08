package project1;

import scala.io.StdIn.readLine
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io._;
import scala.io.StdIn.readLine
import java.io.IOException
import scala.util.Try
import java.io.PrintWriter
import java.sql.SQLException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.DriverManager
import java.io.File
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import scala.collection.mutable.ListBuffer

/**
 * helpful notes 
 * scp -P 2222 .\hello-world_2.13-1.0.jar maria_dev@myip:/home/maria_dev
 * spark-submit --class project1.Main ./hello-world_2.11-1.0.jar
 */
object Main {
  def getContent(url: String): String = {
    val httpClient = new DefaultHttpClient()
    val httpResponse = httpClient.execute(new HttpGet(url))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    httpClient.getConnectionManager().shutdown()
    return content
  }

  def saveData(data: String) {
    val fullFileName = "/tmp/data.json" 
    val writer = new PrintWriter(new File(fullFileName))
    writer.write(data)
    writer.close()
    println(s"File creation success!")


    
    var con: java.sql.Connection = null;
    try {
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        stmt.execute("CREATE TABLE IF NOT EXISTS articles(json String)")
        stmt.execute("LOAD DATA LOCAL INPATH '" + fullFileName +"' INTO TABLE articles");
        stmt.executeQuery("SELECT * FROM articles");
    } catch {
        case ex : Throwable=> {
        ex.printStackTrace();
        throw new Exception (s"${ex.getMessage}")
        }
    } finally{
        try {
            if (con != null){
                con.close()
            }
        } catch {
            case ex : Throwable => {
            ex.printStackTrace();
            throw new Exception (s"${ex.getMessage}")
            } 
        }
    }
  }

  // 1
  def topTitles() = {
    var con: java.sql.Connection = null;
    try {
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        for (i <- 0 to 9){
          var results = stmt.executeQuery("SELECT get_json_object(json, '$.articles.title[" + i + "]') FROM articles")
          while (results.next()){
              println(s"${i + 1}: ${results.getString(1)}")
          }
        }

    } catch {
        case ex : Throwable => {
        ex.printStackTrace();
        throw new Exception (s"${ex.getMessage}")
        }
    } finally{
        try {
            if (con != null){
                con.close()
            }
        } catch {
            case ex : Throwable => {
            ex.printStackTrace();
            throw new Exception (s"${ex.getMessage}")
            } 
        }
    }
  }

  // 2
  def topSources() = {
    var con: java.sql.Connection = null;
    try {
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        for (i <- 0 to 9){
          var results = stmt.executeQuery("SELECT get_json_object(json, '$.articles.source[" + i + "]') FROM articles")
          while (results.next()){
              println(s"${i + 1}: ${results.getString(1)}")
          }
        }

    } catch {
        case ex : Throwable => {
        ex.printStackTrace();
        throw new Exception (s"${ex.getMessage}")
        }
    } finally{
        try {
            if (con != null){
                con.close()
            }
        } catch {
            case ex : Throwable => {
            ex.printStackTrace();
            throw new Exception (s"${ex.getMessage}")
            } 
        }
    }
  }

  // 3
  def topAuthors() = {
    var con: java.sql.Connection = null;
    try {
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        for (i <- 0 to 5){
          var results = stmt.executeQuery("SELECT get_json_object(json, '$.articles.author[" + i + "]') FROM articles")
          while (results.next()){
              println(s"${i + 1}: ${results.getString(1)}")
          }
        }

    } catch {
        case ex : Throwable => {
        ex.printStackTrace();
        throw new Exception (s"${ex.getMessage}")
        }
    } finally{
        try {
            if (con != null){
                con.close()
            }
        } catch {
            case ex : Throwable => {
            ex.printStackTrace();
            throw new Exception (s"${ex.getMessage}")
            } 
        }
    }
  }

  // 4
  def whenWereThey() = {
    var con: java.sql.Connection = null;
    try {
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        for (i <- 0 to 9){
          var results = stmt.executeQuery("SELECT get_json_object(json, '$.articles.publishedAt[" + i + "]') FROM articles")
          while (results.next()){
              println(s"${i + 1}: ${results.getString(1)}")
          }
        }

    } catch {
        case ex : Throwable => {
        ex.printStackTrace();
        throw new Exception (s"${ex.getMessage}")
        }
    } finally{
        try {
            if (con != null){
                con.close()
            }
        } catch {
            case ex : Throwable => {
            ex.printStackTrace();
            throw new Exception (s"${ex.getMessage}")
            } 
        }
    }
  }

  // 5
  def numArticles() = {
    var con: java.sql.Connection = null;
    try {
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        var results = stmt.executeQuery("SELECT get_json_object(json, '$.totalResults') FROM articles")
          while (results.next()){
              println(s"${results.getString(1)}")
        }

    } catch {
        case ex : Throwable => {
        ex.printStackTrace();
        throw new Exception (s"${ex.getMessage}")
        }
    } finally{
        try {
            if (con != null){
                con.close()
            }
        } catch {
            case ex : Throwable => {
            ex.printStackTrace();
            throw new Exception (s"${ex.getMessage}")
            } 
        }
    }
  }

  // 6
  def topWord() = {
    var con: java.sql.Connection = null;
    try {
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        var results = stmt.executeQuery("SELECT WORD, COUNT(*) AS Total FROM articles LATERAL VIEW EXPLODE(SPLIT(json, ' ')) lTable AS WORD GROUP BY WORD ORDER BY Total DESC LIMIT 1")
        while (results.next()){
              println(s"${results.getString(1)}")
        }

    } catch {
        case ex : Throwable => {
        ex.printStackTrace();
        throw new Exception (s"${ex.getMessage}")
        }
    } finally{
        try {
            if (con != null){
                con.close()
            }
        } catch {
            case ex : Throwable => {
            ex.printStackTrace();
            throw new Exception (s"${ex.getMessage}")
            } 
        }
    }
  }



  

 
  def main(args: Array[String]) { 
    // log in 
    println("Welcome\nPlease enter your username")
    var user : String = scala.io.StdIn.readLine()

    println("Password:")
    var pass: String = scala.io.StdIn.readLine()

    // verify
    var username = "user"
    var adusername = "admin"
    var password = "user"
    var adpassword = "admin"
    var response : Int = 0
    if (user.equals(username) && pass.equals(password)) {
      // nonadmin menu
      do {
        println("Logged in successfully! Please pick a commandline option\n" +
          "1 See top titles in politics\n" +
          "2 See top sources in politics\n" +
          "3 See top authors in politics\n" +
          "4 See when the top articles in politics right now were first published\n" +
          "5 See how many politics articles are in the database \n" +
          "6 See the top word in politics\n" +
          "7 Exit")
        response = scala.io.StdIn.readInt()
        response match {
          case 1 => topTitles()
          case 2 => topSources()
          case 3 => topAuthors()
          case 4 => whenWereThey()
          case 5 => numArticles()
          case 6 => topWord()
          case 7 => println("Fairwell")
          case _ => println("Please pick a valid option")
        }
      } while (response != 7)

    }
    else if (user.equals(adusername) && pass.equals(adpassword)) {
      // admin menu
      do {
        println("Please pick a commandline option\n" +
          "0 Fetch political data from the API \n" +
          "1 See top titles in politics\n" +
          "2 See top sources in politics\n" +
          "3 See top authors in politics\n" +
          "4 See when the top articles in politics right now were first published \n" +
          "5 See how many politics articles are in the database \n" +
          "6 See the top word in politics\n" +
          "7 Exit")
        response = scala.io.StdIn.readInt()
        response match {
          case 0 => saveData(getContent("https://newsapi.org/v2/everything?q=Politics&from=2021-11-07&sortBy=popularity&apiKey=373bcc37e1da47b9b6950de6dd41c8ed"))
          case 1 => topTitles()
          case 2 => topSources()
          case 3 => topAuthors()
          case 4 => whenWereThey()
          case 5 => numArticles()
          case 6 => topWord()
          case 7 => println("Fairwell")
          case _ => println("Please pick a valid option")
        }
      } while (response != 7)
    }
    else {
      println("Sorry there is no user/password with that combo.")
    }
  }
}



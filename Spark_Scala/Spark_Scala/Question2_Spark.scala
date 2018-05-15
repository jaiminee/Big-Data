
import org.apache.spark.SparkContext._

val sRDD = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val userData  = sc.textFile("/FileStore/tables/userdata.txt")
val dataFormat = sRDD.map(x=>x.split("\t")).filter(x => x.length == 2).map(x=>(x(0),x(1).split(",")))
val splitedUserData = userData.map(line => line.split(","))
val parsedUserData = splitedUserData.map(line => (line(0).toString,(line(1).toString,line(2).toString,line(3).toString,line(4).toString,line(5).toString,line(6).toString,line(7).toString,line(8).toString,line(9).toString)))
var mapData = dataFormat.flatMap(	
	line => line._2.map( 
      y=>(
            //key
            (
            if(y.toInt < line._1.toInt){
				(y.toInt,line._1.toInt)
			}
            else
            { 
				(line._1.toInt,y.toInt)
			}
            )
          //value
        -> line._2
        )
)).reduceByKey((x, y) => x.intersect(y)) //finding mutual friends

//(key as a user pair and count as value)
val d = mapData.map(x=>(x._1,x._2.size)) 

//finding top 10 user pairs based on count
val sortedFriends = d.sortBy{ case(x,y) => -y }.take(10) 

//1. join by 1st value in the user pair, 2. join by 2nd value in the user pair
val sortedRDDwithDetails = sc.parallelize(sortedFriends).map(x => (x._1._1.toString, (x._1._2, x._2.toString))).join(parsedUserData).map(x=>
      (x._2._1._1.toString, (x._1, x._2._1._2, x._2._2))).join(parsedUserData) 

//formating output <total number of common Friends><Two white space><First Name of User A><TAB><Last Name of User A><Two white space><Address of User A><First Name of User B><TAB><Last Name of User B><Two white space><Address of User B>
val finalresultRDD = sortedRDDwithDetails.map(x => (x._2._1._2+"  "+x._2._1._3._1+"\t"+x._2._1._3._2+","+x._2._1._3._3
      +","+x._2._1._3._4+","+x._2._1._3._5+","+x._2._1._3._6+","+x._2._1._3._7+","+x._2._1._3._8+","+x._2._1._3._9
      +"  "+x._2._2._1+"\t"+x._2._2._2+","+x._2._2._3+","+x._2._2._4+","+x._2._2._5+","+x._2._2._6+","+x._2._2._7
      +","+x._2._2._8+","+x._2._2._9))

//collecting output
finalresultRDD.collect().foreach(println)

//save resultRDD
finalresultRDD.coalesce(1).saveAsTextFile("/FileStore/tables/Question2/OutputSpark1")




// COMMAND ----------



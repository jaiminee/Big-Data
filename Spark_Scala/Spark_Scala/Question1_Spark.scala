
import org.apache.spark.SparkContext._

val sRDD = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
//val userData  = sc.textFile("/FileStore/tables/userdata.txt")

sRDD.take(1)

val dataFormat = sRDD.map(x=>x.split("\t")).filter(x => x.length == 2).map(x=>(x(0),x(1).split(",")))
//val userDataSplited = userData.map(x=>(x.split(",")(0),x.split(",").slice(1,9)))
var userDataMap = dataFormat.take(1)

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
)).reduceByKey((x, y) => x.intersect(y))

//val d = mapData.map(x=>(x._1,x._2.size))
//d.take(1)
//val sortedFriends = d.sortBy{ case(x,y) => -y }.take(10)
var f1 = mapData.filter{case(x,y)=> x._1 == 0 && x._2==4}.collect()
var f2 = mapData.filter{case(x,y)=> x._1 == 20 && x._2==22939}.collect()
var f3 = mapData.filter{case(x,y)=> x._1 == 1 && x._2==29826}.collect()
var f4 = mapData.filter{case(x,y)=> x._1 ==6222  && x._2==19272}.collect()
var f5 = mapData.filter{case(x,y)=> x._1 == 28041 && x._2==28056}.collect()




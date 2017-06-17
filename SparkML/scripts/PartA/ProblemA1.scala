import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

//Load the matrix and seperate the movie id and put it in a vector
val itemusermat_data = sc.textFile("../../data/itemusermat")
val classify_data = itemusermat_data.map(s => Vectors.dense(s.split(' ')
							    .drop(1)
							    .map(_.toDouble)
 							  )).cache()
//run the k-means on the matrix data
val number_of_clusters = 10
val number_of_iterations = 150
val clusters = KMeans.train(classify_data, number_of_clusters, number_of_iterations)

//map each movie id to the cluster by running the predict for each movie
val prediction = itemusermat_data.map{ line =>(line.split(' ')(0),clusters.predict(Vectors.dense(line.split(' ').drop(1).map(_.toDouble))))}
//load the movie file and join with prediction
val movie = sc.textFile("../../data/movies.dat")
val  movies_data = movie.map( line=>(line.split("::"))).map(field=>(field(0),(field(1)+" , "+field(2))))

//join movie data and prediction and group by cluster id(key)
val joinedResult = prediction.join(movies_data)
val groupedData = joinedResult.map(record=>(record._2._1,(record._1,record._2._2))).groupByKey()
val result = groupedData.map(p=>(p._1,p._2.toList))

//Take top 5 for each cluster if available and print
val output = result.map(p=>(p._1,p._2.take(5)))//.sortBy(p._1)
println("Cluster Id , List of first 5 Movies in cluster")
output.foreach(p=>println("Cluster Id "+p._1+"\n"+p._2.mkString("\n")))

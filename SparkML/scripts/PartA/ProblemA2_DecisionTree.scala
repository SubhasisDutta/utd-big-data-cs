import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

//Load the glass.data file
val data = sc.textFile("../../data/glass.data")
//mark the type of each data
val classify_data = data.map { line =>
  val fields = line.split(',')
  LabeledPoint(fields(10).toDouble, Vectors.dense(fields(0).toDouble,fields(1).toDouble,fields(2).toDouble,fields(3).toDouble,fields(4).toDouble,fields(5).toDouble,fields(6).toDouble,fields(7).toDouble,fields(8).toDouble,fields(9).toDouble))
}

//split the data into 60 and 40 for training and test
val splits = classify_data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training_data = splits(0)
val test_data = splits(1)

//Run the decision tree to train
val numClasses = 8
val categorical_features = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 150
val model = DecisionTree.trainClassifier(training_data, numClasses, categorical_features,impurity, maxDepth, maxBins)

//find the level for test data
val label_prediction = test_data.map { record =>
					val prediction = model.predict(record.features)
					(record.label, prediction)
					}

//get accuracy
val accuracy =  100.0 *label_prediction.filter(r => r._1 == r._2).count.toDouble / test_data.count
println("Accuracy of Decision Tree is = " + accuracy+"%")

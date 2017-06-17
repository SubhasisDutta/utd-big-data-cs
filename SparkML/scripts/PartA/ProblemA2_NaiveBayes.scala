import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

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

//run the naive bayse
val model = NaiveBayes.train(training_data, lambda = 1.0)

//predict level for test data
val prediction_label = test_data.map(p => (model.predict(p.features), p.label))

//check accuracy and report
val accuracy = 100.0 * prediction_label.filter(x => x._1 == x._2).count() / test_data.count()
println("Accuracy of Naive Bayes is = " + accuracy+"%")

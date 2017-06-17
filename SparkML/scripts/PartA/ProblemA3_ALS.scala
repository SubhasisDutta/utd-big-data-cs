import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating


// Load and parse the data
val data = sc.textFile("../../data/ratings.dat")
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training_data = splits(0)
val test_data = splits(1)
	
//load the data into Rating entity
val train_ratings = training_data.map(_.split("::") match { case Array(user, item, rate, timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })
val test_ratings = test_data.map(_.split("::") match { case Array(user, item, rate, timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

// Build the recommendation model using ALS
val rank = 8
val iterations = 20
val model = ALS.train(train_ratings, rank, iterations, 0.1)

// Evaluate the model on rating data by creating (user,product) key
val users_products_test = test_ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

//Predict for test data using the trained model
val predictions = model.predict(users_products_test).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

val rates_predictions = test_ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

//Find Mean Squared Error
val MSE = rates_predictions.map { case ((user, product), (rate1, rate2)) =>
      val err = (rate1 - rate2)
      err * err
    }.mean()
println("Mean Squared Error = " + MSE)

//Find Error and accuracy
val error = rates_predictions.map { case ((user, product), (rate1, rate2)) =>
      val err = (rate1 - rate2)
      err
    }.mean()
val accuracy = 100 * (1-error)
println("Accuracy of ALS Model is = " + accuracy+"%")

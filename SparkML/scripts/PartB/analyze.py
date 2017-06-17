import naive_bayes_classifier

algorithm = 'naivebayes'

if(algorithm == 'naivebayes'):
    print "Start..."
    trainingDataFile = 'data/full_training_dataset.csv'
    classifierDumpFile = 'data/model/naivebayes_trained_model.pickle'
    trainingRequired = 1
    nb = naive_bayes_classifier.NaiveBayesClassifier(trainingDataFile, classifierDumpFile, trainingRequired)

print 'Done'

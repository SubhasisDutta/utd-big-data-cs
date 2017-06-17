import naive_bayes_classifier

class TwitterSentiment(object):
    def __init__(self):
        trainingDataFile = 'data/full_training_dataset.csv'
        nb_classifierDumpFile = 'data/model/naivebayes_trained_model.pickle'
        self.nb = naive_bayes_classifier.NaiveBayesClassifier(trainingDataFile, nb_classifierDumpFile)

    def classifyTweet(self, tweet, method):
        if (method != 'naivebayes'):
            return "Method ", method, " NOT SUPPORTED."
        tweets = []
        tweets.append(tweet)
        if (tweets):
            if (method == 'naivebayes'):
                response = self.nb.classify(tweets)
                return response
        else:
            return "NO tweets to classify"


if __name__ == '__main__':
    print "Test"
    testTweet = 'Congrats @subhasis, i heard you wrote a new tech post on sentiment analysis'
    t1 = 'President Donald #trump approaches his first big test this week from a position of unusual weakness.'
    t2 = '#trump has the lowest standing in public opinion of any new president in modern history.'
    t3 = 'Trump has displayed little interest in the policy itself, casting it as a thankless chore to ' \
         'be done before getting to tax-cut legislation he values more.'
    ts = TwitterSentiment()

    print ts.classifyTweet(testTweet,'naivebayes')
    print ts.classifyTweet(t1,'naivebayes')
    print ts.classifyTweet(t2, 'naivebayes')
    print ts.classifyTweet(t3, 'naivebayes')


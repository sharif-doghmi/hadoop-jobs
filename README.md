# hadoop-jobs

Hadoop jobs I wrote

## ngrams package

An n-gram is a contiguous sequence of n words in a text. They are used in text mining and machine learning from text data.
A bigram would be two words that appear next to each other in a text, with the order preserved. Similarly a trigram is a similar sequence of three words.
Frequent n-grams are n-grams that appear more than a certain number of times in a text.

- **BigramFinder.java** contains a Hadoop job that outputs bigrams that appear more than 50 times in a text, and their frequency.
- **TrigramFinder.java** contains a Hadoop job that outputs trigrams that appear more than 10 times in a text, and their frequency.

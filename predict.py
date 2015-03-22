import Orange, sys

if len(sys.argv) < 4:
    print "Usage: " + sys.argv[0] + " <train_set> <predict_set> <output>"
    sys.exit()

print "Importing..."
train = Orange.data.Table(sys.argv[1])
test = Orange.data.Table(sys.argv[2])
print "Training..."
tree = Orange.regression.tree.TreeLearner(train)

print "Classifying..."
error = 0.0
for nb_line in range(len(test)):
    real = test[nb_line][-1]
    prediction = tree(test[nb_line])
    test[nb_line][-1] = prediction
    error += (real - prediction) ** 2
print "Mean squarred error: ", error/len(test)
test.save(sys.argv[3])

print "Done!"


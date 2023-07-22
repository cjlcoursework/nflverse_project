Conclusion
==========
Although we were able to achieve some learning, and had a few predictions that looked promising, 
creating a successful model is not out of the question but would require more effort than we had time for in this POC. 

We would likely be looking for additional data or more intense pre-processing than we had time for in this POC.

In addition, it's unlikely that getting a 'quick-hit' by experimenting in an AutoML will produce easy results.  

We are limited by the amount of data available.  There are only so many seasons and games, and the data will become stale the older it gets.

Even given better data, we'll likely need more complex models and additional effort to experiment with different models and hyperparameters.  

We would need to decide whether to invest in this effort. experimentation and perhaps more complex models to acheive better results.



### Experiment one:


[Run using AWS Sagmaker Studio](https://us-west-2.console.aws.amazon.com/sagemaker/home?region=us-west-2#/notebook-instances/nfl-project-2021-10-04-22-01-01-001)


##### Objective:

Predict the outcome of play calls (e.g. pass, rush, punt, field goal) based on the down, distance, and field position.  
The data would be at the most granular level of a single play.  
We'll use a simple model such as logistic regression to see if we can predict the yards gained.  
We'll also use an AutoML ensemble model to see if we can get better results.

##### Results:

Although the data looked good, a full sweep of AutoML ensemble models did not produce any results better than a random guess.
We were not able to predict the yards gained due to team stats and any particular play call, and could not think of any other objective target.


##### Analysis:

The data as we assembled it did not provide any predictive power.  
The way we formatted the data or constructed the model could be a problem.
We could have spent more time to see if we could find a better model, but we decided to move on to the next experiment 
to see if there was a quick hit using a classification model

### Experiment two:

[Experiment 2 notebook](../../notebooks/nfl_win_loss_classification_experiment2.ipynb)

##### Objective:

Aggregate the data to the game level and see if we can predict wins and losses based on the features we identified in the feature engineering phase.'

##### Results:

We were able to predict wins and losses with 82%+ accuracy using a simple neural network binary classification model (1 for wins and 0 for ties and losses).  

##### Analysis:

Although the validation loss, accuracy, f1 score, confusion matrix and ROC scores all look good, the SHAP explainer showed that we were learning on features that did not make intuitive sense.
In addition, although the loss decreased normally, the validation accuracy started out high rended at a lower rate than the training accuracy.
It's likely that despite the good scores, the model was over-fitting .



### Experiment three:

[Experiment 3 notebook](../../notebooks/nfl_win_loss_classification_experiment3.ipynb)

##### Objective:

Aggregate the data to the game level and see if we can predict wins and losses based on the features we identified in the feature engineering phase.
However, since we suspect that the model was over-fitting, we'll split out season 2022 and use seasons 2016 - 2021 to predict the 2022 season.

##### Results:

We were again able to predict wins and losses with 82%+ accuracy using a simple neural network binary classification model (1 for wins and 0 for ties and losses).

##### Analysis:

This time the precision was down to 65% and the confusion matrix confirms that this was due to (too) many false positives.


TODO:

# Shuffle X and y together (keeping their alignment)
indices = np.arange(X.shape[0])
np.random.shuffle(indices)

X_shuffled = X[indices]
y_shuffled = y[indices]


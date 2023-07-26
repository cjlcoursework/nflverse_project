<div style="background-color: teal; padding: 10px;">
    <h3 style="color: white;">Conclusion</h3>
</div>
<div style="border: 1px solid rgba(13, 74, 102, 0.4); margin: 1px 0;"></div>

< [Back](main.md)

We were able to achieve some success using nflverse data for machine learning.  Using the data for machine learning is therefore feasible. Going one step further to create a more interesting model is not out of the question but would require more effort than we had time for in this POC. 

<br>

<div style="background-color: teal; padding: 10px;">
    <h3 style="color: white;">Supporting Evidense</h3>
</div>


<div style="border: 1px solid rgba(13, 74, 102, 0.4); margin: 1px 0;"></div>

<br>

### <font color=teal>Experiment one:</font>


[Run using AWS Sagmaker Studio](https://us-west-2.console.aws.amazon.com/sagemaker/home?region=us-west-2#/notebook-instances/nfl-project-2021-10-04-22-01-01-001)


##### <font color="#0d4a66">Objective:</font>

Predict the outcome of play calls (e.g. pass, rush, punt, field goal) based on the down, distance, and field position, with yards_gained and points_gained as the target columns.  
The data would be at the most granular level of a single play.  

##### <font color="#0d4a66">Steps:</font>
Start with a simple model such as logistic regression to see if we can predict the yards gained based on the play call in a give situation (e.g. 4th down and 1 yard to go, 3rd down and 10 yards to go, etc.) 
We'll also use an AutoML ensemble model to see if we can get better results.


##### <font color="#0d4a66">Results:</font>

Although the data looked good, a full sweep of AutoML ensemble models did not produce any results better than a random guess.
We were not able to predict the yards gained due to team stats and any particular play call, and could not think of any other objective target.  


##### <font color="#0d4a66">Analysis:</font>

In retrospect, it's possible that we curated the data too well and that, as in experiment 2 we should curate less and let the network figure out how different features contributed to the outcome. 
We could have spent more time to see if we could create better data and find a better model, but we decided to move on to the next experiment 
to see if there was a quick hit using a classification model

<br>

<div style="border: 1px solid rgba(13, 74, 102, 0.4); margin: 1px 0;"></div>

### <font color=teal>Experiment two:</font>

[Experiment 2 notebook](../../notebooks/nfl_win_loss_classification_experiment2.ipynb)

##### <font color="#0d4a66">Objective:</font>

Predict simple win/loss based on the features we identified in the feature engineering phase.


##### <font color="#0d4a66">Steps:</font>
Aggregate the data to the game level and see if we can predict wins and losses.   
We use seasons **2016** - **2021** to predict the **2022** season.

- Initially we had a multi-class classification model with 3 classes (1 for wins, 0 for ties, and -1 for losses), but we found that the model was not learning the -1 class. We also had curated the data too much: if we had a defensive stat and an offense stat, we might take the difference to achieve one feature that was neither defense or offense.  
This produced a model that had very high recall but 65% precision.


- We then removed a lot of the data curation and let the model figure out how to use the features.  
This produced a model with 87% precision and 85% recall.

##### <font color="#0d4a66">Results:</font>

We were able to predict wins and losses with 85%+ accuracy using a simple neural network binary classification model (1 for wins and ties, and 0 for losses).  

##### <font color="#0d4a66">Analysis:</font>

Although the validation loss, accuracy, f1 score, confusion matrix and ROC scores all look good, the SHAP explainer showed that we were learning on features that did not make complete intuitive sense.
It's likely that the nflverse features that produced the best model are confounded with other features that we did not have in the data.


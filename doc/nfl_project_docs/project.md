
<div style="background-color: teal; padding: 10px;">
    <h3 style="color: white;">Project Description</h3>
</div>

< [Back](main.md)

<br>
<h3 style="color: purple;">Problem Statement</h3>
<div style="border: 1px solid rgba(147, 112, 219, 0.4); margin: 1px 0;"></div>
<br>



### <font color=teal>Motivation</font>

  - We want to decide whether to kick off a project that uses readily avialable NFL data to apply machine learning to predict NFL game outcomes

### <font color=teal>Context</font>
- The NFLVerse site has some good NFL play-by-play data. 
- We'd like to understand the difficulty and feasibility of using this data as input to a machine learning projects.  
- We'd like to understand whether we can improve on these models using machine learning.

### <font color=teal>Criteria for Success</font>
- Determine a rough level-of-effort to predict NFL game outcomes using machine learning.
    Create a proof-of-concept to show how data can be formatted as input to a machine learning model.   
    Show that the model can be trained and used to predict NFL game outcomes.

### <font color=teal>Scope of solution</font>
- We don't need to predict games accurately.  We just need to show that we can format the data and train a model to predict NFL game outcomes.


### <font color=teal>Constraints</font>
- Because we have 2 weeks to decide whether to green-light a project to predict NFL game outcomes, we will not try to perfect either the model or the data.  
    We'll just see if we can get something working with simple classification and regression models.
    We do not have access to the NFL API, which may provide better data.

<br>
<h1 style="color: purple;">Approach</h1>
<div style="border: 1px solid rgba(147, 112, 219, 0.4); margin: 1px 0;"></div>
<br>

### <font color=teal>Dimension the data</font>
- The nflverse data is delivered in wide column monolithic blocks. 

- The cardinality of the data is different for many of the columns, which is great for keeping it simple, but leads to very sparse data.
- For example, a column containing the "rushing player" does not make sense for a pass play, so this data wil be null for anything but a rushing play, and there's no simple way to impute that information 
    when it is taken along with other columns with different cardinality such as game drive and downs.
    The best way to do that is to separate the data into dimensions and facts, then join them together in different ways to see what works.


### <font color=teal>Experiment one</font>
- Our first experiment will be to see if we can predict the outcome of play-calling based on the down, distance, and field position.  
- The data would be at the most granular level of a single play.  
- We'll use a simple model such as logistic regression to see if we can predict the yards gained.
- We'll also use an AutoML ensemble model to see if we can get better results.

### <font color=teal>Experiment two</font>
- We have only two weeks, so if the play-call model does not provide immediate joy (we have two weeks) we won't spend too much more time to tweak the model.  Then we'll shift to aggregate the data to the game level and see if we can predict wins and losses.
- This is not as interesting, because we don't really believe that we'll be able to predict even as well as a statistical approach, but we will still be able to see whether a model can diminish loss and increase accuracy. 


### <font color=teal>Deliverables</font>
- A proof-of-concept that shows how to format the data as input to a machine learning model.
- A proof-of-concept that shows how to train a model to predict NFL game outcomes.
- A report that shows the level-of-effort to predict NFL game outcomes using machine learning.
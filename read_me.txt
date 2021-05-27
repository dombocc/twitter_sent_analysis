This repository is a set of codes that pulls and classifies tweets regarding 9 different tech companies/ tech topics, ie. Apple and Linux. 

#PullTweets.py
It pulls new tweets based on the id of the most recently pulled tweet from the last run.
Then, cleans them up and removes generally any non-English verbiage. 
It then store them in database. 

#AssignSentiment.py
From there, a model has been made to classify the tweets as positive or negative regard the companies. 

#CheckTables.py
This code runs after sentiment is assigned and checks each tables counts to make sure that every record has been carried all the through 
    the process and none have fallen out or been duplicated.

#CreateSentimentModel.ipynb 
This was written to create the model. 


Next steps if continued through the project would be to link the output to a data visualization suite like Tableau or PowerBi 
    to visualize the data over time as well as collectively analyzing how people are speaking about these companies/topics
    
    Another step would be to do some model tuning and possible incorporate some continual learning to have a better trained model.

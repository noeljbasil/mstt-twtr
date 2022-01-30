# Stop The Traffik Twitter Listening Dashboard maintenance repo

This repository is used to provide the code to Heroku app that perform scrapping of twitter data, apply necessary transformations and upload it to a location that can be connected to by the Tableau dashboard: https://public.tableau.com/app/profile/merkle.stt/viz/TwitterListeningdashboard_containercp12/OverallTrend

File info:

1. Procfile              : This file is used to set up a custom clock process so that only necessary free dyno hours are consumed
2. nltk.txt: This file is used to download nltk modules that required to perform the NLP operations in the main code
3. requirements.txt: THis file informs the Heroku app what all python libraries are requried to be installed
4. runtime.txt: This file defines the version of python to be used
5. Scripts/scheduler.py: This file sets the schedule on which Scripts/stt_twtr.py file is to be run
6. Scripts/stt_twtr.py: The main program file
7. Scripts/stt_twtr_manual_run.py: File that can be used to run manually and hence can be used for quick debugs

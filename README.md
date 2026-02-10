# openpowerlifting-pipeline
This pipeline should supply up-to date information from openpowerlifting.org to a Tableau Public dashboard which showcases all the powerlifters totaling more than 1000 kgs.
The pipeline is run every day at 6 am CET.
It gets the full dataset from openpowerlifting (the bigger file, not just IPF numbers).
The data is filtered for only lifts which total more than 1000 kgs.
The filtered dataset is then uploaded to a neon.tech serverless database and also to a Google Sheet, because Tableau Public does not handle any other type of live connection.

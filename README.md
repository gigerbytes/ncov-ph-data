# ncov-ph-data

Gathering Data about NCOV in the Philippines from the DOH & converting it into a usable format for data analysis.

## Files

`main.py`
Set of functions used to query DOH API used by the DOH dashboard https://ncovtracker.doh.gov.ph/
Saves data into a mongo database

`pdf_aggregator.py`
Uses camelot to read the daily PDF released by DOH about cases, as it has some data not (yet) accessible through the api. Saves parsed data into CSV

You can see pdf examples in /pdfs

`combine_data.py`
Combine API and PDF data to holisting csv `cases_ph.csv` for use & upload to KAGGLE

Kaggle link here : https://www.kaggle.com/gigbytes/novel-coronavirus-philippine-dataset

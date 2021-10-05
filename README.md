-----Algo-Trading-------

db_handler.py  -----> manually updates the databases used in the program by assigning default values to every field

entry.py   ---------> only checks for entry ( web socket based deployment )....incomplete

main.py  -----------> the part of the program that runs every 15 minutes....pretty much complete....except for the historical api to websocket conversion

main.csv -----------> stock list and respective fields

open_positions.csv && shortlist.csv ---------> tables that will be created in the running market whenever a stock is shortlisted and / or a trade is initiated

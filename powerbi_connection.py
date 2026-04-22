import duckdb
import pandas as pd

db_path = r'\\wsl.localhost\Ubuntu\home\michauj\Life-In-Data\data\final\life_in_data.duckdb'
con = duckdb.connect(db_path, read_only=True)

# Pobieramy wszystkie trzy tabele
fact_stayfree = con.execute("SELECT * FROM fact_stayfree").df()
fact_habits = con.execute("SELECT * FROM fact_habits").df()
dim_calendar = con.execute("SELECT * FROM dim_calendar").df()

con.close()
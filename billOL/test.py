import pandas as pd


# start_date = pd.Timestamp.today() - pd.DateOffset(months=2)

# start_date = pd.Timestamp.today() - pd.DateOffset(months=2)
# start_date = start_date.normalize()

# start_date = pd.Timestamp(year=2024, month=1, day=1)

end_date = pd.Timestamp.today()
start_date = (end_date - pd.DateOffset(months=2)).replace(day=1)

print(start_date.date())

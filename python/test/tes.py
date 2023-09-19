import pandas as pd
df = pd.DataFrame(columns=['A'])
df = df.append({'A': 1}, ignore_index=True)
print(df)
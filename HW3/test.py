import pandas as pd

# create a sample DataFrame
data = {'fruit': [ 'orange', 'banana', 'orange']}
df = pd.DataFrame(data)

# use value_counts() to count occurrences of 'apple'
count = df['fruit'].value_counts()['apple']

print(f"The number of apples is: {count}")
import pandas as pd
import main
file_name = 'C:\\Users\\cuixi\\Downloads\\2020-01-13T19_31_19_1-4\\2020-01-13T19_31_19_2.csv'
names = ['citing', 'cited']
chunks = pd.read_csv(file_name,usecols=names,chunksize=10**5,iterator=True)
for chunk in chunks:
    i = 0
    for i in range(len(chunk)):
        main.searchTest(chunk['citing'][i],chunk['cited'][i])




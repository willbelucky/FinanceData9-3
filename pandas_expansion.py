def difference(df1, df2):
    return df1.loc[df1.index.difference(df2.index).values]

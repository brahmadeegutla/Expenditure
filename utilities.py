def scd1(incoming_df, master_df):

    m = master_df
    i = incoming_df

    headers = ["Transaction_date", "Description", "Reference_number"]

    delisted_df = m.alias('m').join(i.alias('i'),headers,"left_outer").filter("i.Transaction_date is NULL").select('m.*')
    print("dfs")
    m.show()
    i.show()
    delisted_df.show()

    FINAL_DF = delisted_df.union(incoming_df)
    return FINAL_DF


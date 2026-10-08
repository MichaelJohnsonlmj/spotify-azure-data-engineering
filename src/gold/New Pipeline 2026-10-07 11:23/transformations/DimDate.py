import dlt

# ============================================================
# 1. Silver DimUser -> Streaming staging table
# ============================================================

@dlt.table(
    name="dimdate_stg",
    comment="Staging stream reading the Silver DimDate table."
)
def dimdate_stg():

    return spark.readStream.table(
        "spotify_catalog.silver.DimDate"
    )


# ============================================================
# 2. Define the Gold streaming target
# ============================================================

dlt.create_streaming_table(
    name="dimdate",
    comment="Gold Dimdate dimension with SCD Type 2 history."
)


# ============================================================
# 3. Auto CDC -> Gold DimUser
# ============================================================

dlt.create_auto_cdc_flow(
    target="dimdate",
    source="dimdate_stg",
    keys=["date_key"],
    sequence_by="date",
    stored_as_scd_type=2,
    track_history_except_column_list=None,
    name="dimdate_cdc_flow",
    once=False
)
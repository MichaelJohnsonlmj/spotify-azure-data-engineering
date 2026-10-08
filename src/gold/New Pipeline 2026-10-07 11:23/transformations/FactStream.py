import dlt

# ============================================================
# 1. Silver DimUser -> Streaming staging table
# ============================================================

@dlt.table(
    name="factstream_stg",
    comment="Staging factstream reading the Silver factstream table."
)
def factstream_stg():

    return spark.readStream.table(
        "spotify_catalog.silver.factstream"
    )


# ============================================================
# 2. Define the Gold streaming target
# ============================================================

dlt.create_streaming_table(
    name="factstream",
    comment="Gold factstream dimension with SCD Type 2 history."
)


# ============================================================
# 3. Auto CDC -> Gold DimUser
# ============================================================

dlt.create_auto_cdc_flow(
    target="factstream",
    source="factstream_stg",
    keys=["stream_id"],
    sequence_by="stream_timestamp",
    stored_as_scd_type=2,
    track_history_except_column_list=None,
    name="factstream_cdc_flow",
    once=False
)
import dlt

# ============================================================
# 1. Silver DimUser -> Streaming staging table
# ============================================================

@dlt.table(
    name="dimtrack_stg",
    comment="Staging stream reading the Silver DimTrack table."
)
def dimuser_stg():

    return spark.readStream.table(
        "spotify_catalog.silver.DimTrack"
    )


# ============================================================
# 2. Define the Gold streaming target
# ============================================================

dlt.create_streaming_table(
    name="dimtrack",
    comment="Gold DimTrack dimension with SCD Type 2 history."
)


# ============================================================
# 3. Auto CDC -> Gold DimUser
# ============================================================

dlt.create_auto_cdc_flow(
    target="dimtrack",
    source="dimtrack_stg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_except_column_list=None,
    name="dimtrack_cdc_flow",
    once=False
)
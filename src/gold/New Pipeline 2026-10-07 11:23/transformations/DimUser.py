```python
import dlt


# ============================================================
# 1. Data quality expectations
# ============================================================

expectations = {
    "rule1": "user_id IS NOT NULL"
}


# ============================================================
# 2. Silver DimUser -> Streaming staging table
# ============================================================

@dlt.table(
    name="dimuser_stg",
    comment="Staging stream reading the Silver DimUser table."
)
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():

    return spark.readStream.table(
        "spotify_catalog.silver.DimUser"
    )


# ============================================================
# 3. Define Gold streaming target
# ============================================================

dlt.create_streaming_table(
    name="dimuser",
    expect_all_or_drop=expectations,
    comment="Gold DimUser dimension with SCD Type 2 history."
)


# ============================================================
# 4. Auto CDC -> Gold DimUser
# ============================================================

dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_except_column_list=None,
    name="dimuser_cdc_flow",
    once=False
)
```

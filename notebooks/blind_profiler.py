from databricks.sdk.runtime import *
from utility import init_logger
from pyspark.sql import functions as F


class BlindProfiler:
    def __init__(self):
        self.logger = init_logger()

    def setup_widgets(self):
        # Remove old widgets
        dbutils.widgets.removeAll()

        dbutils.widgets.text("CATALOG_NAME", "roo_bricks", "Catalog Name")
        dbutils.widgets.text("SCHEMA_NAME", "mockaroo_data", "Schema Name")
        dbutils.widgets.text("TABLE_NAMES", "*", "Table Names (CSV; * = all)")

    def profile(self):
        catalog_name = dbutils.widgets.get("CATALOG_NAME")
        schema_name = dbutils.widgets.get("SCHEMA_NAME")
        table_names = dbutils.widgets.get("TABLE_NAMES")

        # Get list of tables to profile
        if table_names == "*":
            tables_query = f"SHOW TABLES IN {catalog_name}.{schema_name}"
            tables_df = spark.sql(tables_query)
            tables_to_profile = [
                row.tableName
                for row in tables_df.collect()
                if row.tableName != "profile_results"
            ]
        else:
            tables_to_profile = [t.strip() for t in table_names.split(",")]

        self.logger.info(
            f"Profiling {len(tables_to_profile)} tables in {catalog_name}.{schema_name}"
        )

        # Collect all profile results
        all_profile_data = []

        # Profile each table
        for table_name in tables_to_profile:
            self.logger.info(f"Profiling table: {table_name}")
            table_profile_data = self._profile_table(
                catalog_name, schema_name, table_name
            )
            if table_profile_data:
                all_profile_data.extend(table_profile_data)

        # Save all results to single consolidated table
        if all_profile_data:
            profile_df = (
                spark.createDataFrame(all_profile_data)
                .select(
                    "Table",
                    "Column",
                    "Min",
                    "Max",
                    "Mean",
                    "Mode",
                    "ModeFreq",
                    "Variance",
                    "StdDev",
                    "P5",
                    "P25",
                    "P50",
                    "P75",
                    "P95",
                    "Skewness",
                    "Kurtosis",
                    "Range",
                    "IQR",
                    "OutlierCount",
                    "DuplicateCount",
                    "NullCount",
                    "RecordCount",
                )
                .orderBy("Table", "Column")
            )
            prof_table_name = f"{catalog_name}.{schema_name}.profile_results"
            profile_df.write.mode("overwrite").option(
                "overwriteSchema", True
            ).saveAsTable(prof_table_name)

            self.logger.info(f"All profile results saved to table: {prof_table_name}")
        else:
            self.logger.info("No profile data collected from any tables")

    def _profile_table(self, catalog_name, schema_name, table_name):
        try:
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

            # Get table schema to identify numeric and date columns
            df = spark.table(full_table_name)
            numeric_date_columns = []

            for field in df.schema.fields:
                # Ignore metadata columns (those starting with __m_)
                if field.name.startswith("__m_"):
                    continue

                if field.dataType.typeName() in [
                    "integer",
                    "long",
                    "float",
                    "double",
                    "decimal",
                ] or field.dataType.typeName() in ["date", "timestamp"]:
                    numeric_date_columns.append((field.name, field.dataType.typeName()))

            if not numeric_date_columns:
                self.logger.info(f"No numeric or date columns found in {table_name}")
                return None

            self.logger.info(
                f"Found {len(numeric_date_columns)} numeric/date columns in {table_name}: {[col[0] for col in numeric_date_columns]}"
            )

            # Get total record count for the table
            total_records = df.count()

            # Process each column individually to create row-based results
            profile_data = []

            for col_name, col_type in numeric_date_columns:
                if col_type in ["date", "timestamp"]:
                    # For date/timestamp columns - batch all aggregations
                    date_aggs = df.agg(
                        F.date_format(F.min(col_name), "yyyy-MM-dd'T'HH:mm:ss").alias(
                            "min_val"
                        ),
                        F.date_format(F.max(col_name), "yyyy-MM-dd'T'HH:mm:ss").alias(
                            "max_val"
                        ),
                        F.date_format(
                            F.from_unixtime(F.avg(F.unix_timestamp(col_name))),
                            "yyyy-MM-dd'T'HH:mm:ss",
                        ).alias("mean_val"),
                        F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(
                            "null_count"
                        ),
                    ).collect()[0]

                    min_val = date_aggs["min_val"]
                    max_val = date_aggs["max_val"]
                    mean_val = date_aggs["mean_val"]
                    null_count = str(date_aggs["null_count"])

                    # For date/timestamp, variance, stddev, skewness, kurtosis, range, IQR, and outliers don't make sense
                    variance_val = "N/A"
                    stddev_val = "N/A"
                    skewness_val = "N/A"
                    kurtosis_val = "N/A"
                    range_val = "N/A"
                    iqr_val = "N/A"
                    outlier_count = "N/A"

                    # Calculate percentiles for date/timestamp columns
                    df_with_ts = df.withColumn("temp_ts", F.unix_timestamp(col_name))
                    percentiles_ts = df_with_ts.approxQuantile(
                        "temp_ts", [0.05, 0.25, 0.5, 0.75, 0.95], 0.01
                    )

                    # Batch convert percentiles back to date format
                    if any(p is not None for p in percentiles_ts):
                        percentile_conversions = df.select(
                            (
                                F.date_format(
                                    F.from_unixtime(F.lit(percentiles_ts[0])),
                                    "yyyy-MM-dd'T'HH:mm:ss",
                                ).alias("p5")
                                if percentiles_ts[0] is not None
                                else F.lit("N/A").alias("p5")
                            ),
                            (
                                F.date_format(
                                    F.from_unixtime(F.lit(percentiles_ts[1])),
                                    "yyyy-MM-dd'T'HH:mm:ss",
                                ).alias("p25")
                                if percentiles_ts[1] is not None
                                else F.lit("N/A").alias("p25")
                            ),
                            (
                                F.date_format(
                                    F.from_unixtime(F.lit(percentiles_ts[2])),
                                    "yyyy-MM-dd'T'HH:mm:ss",
                                ).alias("p50")
                                if percentiles_ts[2] is not None
                                else F.lit("N/A").alias("p50")
                            ),
                            (
                                F.date_format(
                                    F.from_unixtime(F.lit(percentiles_ts[3])),
                                    "yyyy-MM-dd'T'HH:mm:ss",
                                ).alias("p75")
                                if percentiles_ts[3] is not None
                                else F.lit("N/A").alias("p75")
                            ),
                            (
                                F.date_format(
                                    F.from_unixtime(F.lit(percentiles_ts[4])),
                                    "yyyy-MM-dd'T'HH:mm:ss",
                                ).alias("p95")
                                if percentiles_ts[4] is not None
                                else F.lit("N/A").alias("p95")
                            ),
                        ).collect()[0]

                        p5_val = percentile_conversions["p5"]
                        p25_val = percentile_conversions["p25"]
                        p50_val = percentile_conversions["p50"]
                        p75_val = percentile_conversions["p75"]
                        p95_val = percentile_conversions["p95"]
                    else:
                        p5_val = p25_val = p50_val = p75_val = p95_val = "N/A"
                else:
                    # For numeric columns - batch all aggregations
                    numeric_aggs = df.agg(
                        F.min(col_name).alias("min_val"),
                        F.max(col_name).alias("max_val"),
                        F.avg(col_name).alias("mean_val"),
                        F.variance(col_name).alias("variance_val"),
                        F.stddev(col_name).alias("stddev_val"),
                        F.skewness(col_name).alias("skewness_val"),
                        F.kurtosis(col_name).alias("kurtosis_val"),
                        F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(
                            "null_count"
                        ),
                    ).collect()[0]

                    min_val = str(round(numeric_aggs["min_val"], 2))
                    max_val = str(round(numeric_aggs["max_val"], 2))
                    mean_val = str(round(numeric_aggs["mean_val"], 2))
                    variance_val = str(round(numeric_aggs["variance_val"], 2))
                    stddev_val = str(round(numeric_aggs["stddev_val"], 2))
                    skewness_val = str(round(numeric_aggs["skewness_val"], 2)) if numeric_aggs["skewness_val"] is not None else "N/A"
                    kurtosis_val = str(round(numeric_aggs["kurtosis_val"], 2)) if numeric_aggs["kurtosis_val"] is not None else "N/A"
                    null_count = str(numeric_aggs["null_count"])

                    # Calculate percentiles
                    percentiles = df.approxQuantile(
                        col_name, [0.05, 0.25, 0.5, 0.75, 0.95], 0.01
                    )
                    p5_val = (
                        str(round(percentiles[0], 2))
                        if percentiles[0] is not None
                        else "N/A"
                    )
                    p25_val = (
                        str(round(percentiles[1], 2))
                        if percentiles[1] is not None
                        else "N/A"
                    )
                    p50_val = (
                        str(round(percentiles[2], 2))
                        if percentiles[2] is not None
                        else "N/A"
                    )
                    p75_val = (
                        str(round(percentiles[3], 2))
                        if percentiles[3] is not None
                        else "N/A"
                    )
                    p95_val = (
                        str(round(percentiles[4], 2))
                        if percentiles[4] is not None
                        else "N/A"
                    )

                    # Calculate range, IQR, and outlier count for numeric columns
                    if percentiles[1] is not None and percentiles[3] is not None:
                        # Calculate range (max - min)
                        range_val = str(round(numeric_aggs["max_val"] - numeric_aggs["min_val"], 2))

                        # Calculate IQR (Q3 - Q1)
                        iqr_val = str(round(percentiles[3] - percentiles[1], 2))

                        # Calculate outlier bounds (Q1 - 1.5*IQR, Q3 + 1.5*IQR)
                        q1 = percentiles[1]
                        q3 = percentiles[3]
                        iqr = q3 - q1
                        lower_bound = q1 - 1.5 * iqr
                        upper_bound = q3 + 1.5 * iqr

                        # Count outliers
                        outlier_count_result = df.filter(
                            (F.col(col_name) < lower_bound) | (F.col(col_name) > upper_bound)
                        ).count()
                        outlier_count = str(outlier_count_result)
                    else:
                        range_val = "N/A"
                        iqr_val = "N/A"
                        outlier_count = "N/A"

                # Calculate mode and distinct count together
                mode_and_distinct = (
                    df.filter(F.col(col_name).isNotNull())
                    .agg(F.countDistinct(col_name).alias("distinct_count"))
                    .collect()[0]
                )

                distinct_count = mode_and_distinct["distinct_count"]
                duplicate_count_int = total_records - distinct_count
                duplicate_count = str(duplicate_count_int)

                # Calculate mode (most frequent value) - still needs separate operation
                mode_result = (
                    df.filter(F.col(col_name).isNotNull())
                    .groupBy(col_name)
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(1)
                    .collect()
                )

                if mode_result:
                    mode_val = str(mode_result[0][col_name])
                    mode_freq = str(mode_result[0]["count"])
                else:
                    mode_val = "N/A"
                    mode_freq = "0"

                # Create row for column metrics
                profile_data.extend(
                    [
                        {
                            "Table": table_name,
                            "Column": col_name,
                            "Min": min_val,
                            "Max": max_val,
                            "Mean": mean_val,
                            "Mode": mode_val,
                            "ModeFreq": mode_freq,
                            "Variance": variance_val,
                            "StdDev": stddev_val,
                            "P5": p5_val,
                            "P25": p25_val,
                            "P50": p50_val,
                            "P75": p75_val,
                            "P95": p95_val,
                            "Skewness": skewness_val,
                            "Kurtosis": kurtosis_val,
                            "Range": range_val,
                            "IQR": iqr_val,
                            "OutlierCount": outlier_count,
                            "DuplicateCount": duplicate_count,
                            "NullCount": null_count,
                            "RecordCount": str(total_records),
                        },
                    ]
                )

                # For date/timestamp columns, add day gap analysis
                if col_type in ["date", "timestamp"]:
                    try:
                        # Get unique dates sorted
                        unique_dates_df = (
                            df.select(col_name)
                            .filter(F.col(col_name).isNotNull())
                            .distinct()
                            .orderBy(col_name)
                        )

                        # Add row numbers to calculate gaps
                        from pyspark.sql.window import Window

                        # Use row_number instead of lag to avoid window partitioning issues
                        window_spec = Window.orderBy(col_name)

                        dates_with_row_num = unique_dates_df.withColumn(
                            "row_num", F.row_number().over(window_spec)
                        )

                        # Self-join to calculate gaps more efficiently
                        gaps_df = (
                            dates_with_row_num.alias("curr")
                            .join(
                                dates_with_row_num.alias("prev"),
                                F.col("curr.row_num") == F.col("prev.row_num") + 1,
                                "inner",
                            )
                            .select(
                                F.col(f"curr.{col_name}").alias("current_date"),
                                F.col(f"prev.{col_name}").alias("prev_date"),
                                F.datediff(
                                    F.col(f"curr.{col_name}"), F.col(f"prev.{col_name}")
                                ).alias("day_gap"),
                            )
                            .filter(F.col("day_gap").isNotNull())
                        )

                        if gaps_df.count() > 0:
                            # Calculate gap statistics - batch all aggregations
                            gap_stats = gaps_df.agg(
                                F.min("day_gap").alias("gap_min"),
                                F.max("day_gap").alias("gap_max"),
                                F.avg("day_gap").alias("gap_mean"),
                                F.variance("day_gap").alias("gap_variance"),
                                F.stddev("day_gap").alias("gap_stddev"),
                            ).collect()[0]

                            gap_min = gap_stats["gap_min"]
                            gap_max = gap_stats["gap_max"]
                            gap_mean = gap_stats["gap_mean"]
                            gap_variance = gap_stats["gap_variance"]
                            gap_stddev = gap_stats["gap_stddev"]

                            # Calculate gap percentiles
                            gap_percentiles = gaps_df.approxQuantile(
                                "day_gap", [0.05, 0.25, 0.5, 0.75, 0.95], 0.01
                            )
                            gap_p5 = (
                                str(gap_percentiles[0])
                                if gap_percentiles[0] is not None
                                else "N/A"
                            )
                            gap_p25 = (
                                str(gap_percentiles[1])
                                if gap_percentiles[1] is not None
                                else "N/A"
                            )
                            gap_p50 = (
                                str(gap_percentiles[2])
                                if gap_percentiles[2] is not None
                                else "N/A"
                            )
                            gap_p75 = (
                                str(gap_percentiles[3])
                                if gap_percentiles[3] is not None
                                else "N/A"
                            )
                            gap_p95 = (
                                str(gap_percentiles[4])
                                if gap_percentiles[4] is not None
                                else "N/A"
                            )

                            # Get mode for gaps
                            gap_mode_result = (
                                gaps_df.groupBy("day_gap")
                                .count()
                                .orderBy(F.desc("count"))
                                .limit(1)
                                .collect()
                            )

                            if gap_mode_result:
                                gap_mode = str(gap_mode_result[0]["day_gap"])
                                gap_mode_freq = str(gap_mode_result[0]["count"])
                            else:
                                gap_mode = "N/A"
                                gap_mode_freq = "0"

                            # Calculate gap duplicates
                            gap_total = gaps_df.count()
                            gap_distinct = gaps_df.select("day_gap").distinct().count()
                            gap_duplicates = gap_total - gap_distinct

                            # Add gap analysis row
                            profile_data.extend(
                                [
                                    {
                                        "Table": table_name,
                                        "Column": f"{col_name} (Day Gaps)",
                                        "Min": str(gap_min),
                                        "Max": str(gap_max),
                                        "Mean": str(round(gap_mean, 2)),
                                        "Mode": gap_mode,
                                        "ModeFreq": gap_mode_freq,
                                        "Variance": (
                                            str(round(gap_variance, 2))
                                            if gap_variance
                                            else "0.0"
                                        ),
                                        "StdDev": (
                                            str(round(gap_stddev, 2))
                                            if gap_stddev
                                            else "0.0"
                                        ),
                                        "P5": gap_p5,
                                        "P25": gap_p25,
                                        "P50": gap_p50,
                                        "P75": gap_p75,
                                        "P95": gap_p95,
                                        "Skewness": "N/A",
                                        "Kurtosis": "N/A",
                                        "Range": "N/A",
                                        "IQR": "N/A",
                                        "OutlierCount": "N/A",
                                        "DuplicateCount": str(gap_duplicates),
                                        "NullCount": "0",
                                        "RecordCount": str(gap_total),
                                    },
                                ]
                            )
                    except Exception as e:
                        self.logger.warning(
                            f"Could not calculate day gaps for {col_name}: {str(e)}"
                        )

            if profile_data:
                self.logger.info(f"Profile results for {table_name} completed")

            return profile_data

        except Exception as e:
            self.logger.error(f"Error profiling table {table_name}: {str(e)}")
            return None

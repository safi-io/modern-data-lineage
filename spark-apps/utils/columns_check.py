def columns_check(spark, logger, s3_paths):
    expected_columns = {"_serial", "_raw"}

    for s3_path in s3_paths:

        header_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(s3_path)
        )

        actual_columns = set(header_df.columns)

        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns

        if missing:
            raise ValueError(
                f"{s3_path}: Missing columns: {missing}"
            )

        if extra:
            logger.warning(
                f"{s3_path}: Extra columns (ignored): {extra}"
            )

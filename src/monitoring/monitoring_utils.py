from datetime import datetime


def calculate_quality_score(total_checks, failed_checks):
    """
    Calculate the percentage of successful data quality checks.
    """

    if total_checks == 0:
        return 0.0

    passed_checks = total_checks - failed_checks

    quality_score = (
        passed_checks / total_checks
    ) * 100

    return round(quality_score, 2)


def create_metrics_record(
    pipeline_name,
    rows_processed,
    runtime_seconds,
    failed_checks,
    quality_score
):
    """
    Create a monitoring metrics record ready to be
    converted into a Spark DataFrame.
    """

    return [
        (
            pipeline_name,
            rows_processed,
            runtime_seconds,
            failed_checks,
            quality_score,
            datetime.now()
        )
    ]

def pipeline_health_status(quality_score):
    """
    Determine overall pipeline health
    based on quality score.
    """

    if quality_score >= 95:
        return "HEALTHY"

    elif quality_score >= 80:
        return "WARNING"

    else:
        return "CRITICAL"


def pipeline_summary(
    pipeline_name,
    quality_score,
    failed_checks
):
    """
    Create a pipeline summary record.
    """

    return {
        "pipeline_name": pipeline_name,
        "quality_score": quality_score,
        "failed_checks": failed_checks,
        "health_status": pipeline_health_status(
            quality_score
        )
    }

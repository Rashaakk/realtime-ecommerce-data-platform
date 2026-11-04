
from pyspark.sql import DataFrame

from member3_quality_storage.config import (
    VALIDATED_DIR,
    REJECTED_DIR,
)


def save_quality_results(
    valid_df: DataFrame,
    invalid_df: DataFrame,
    mode: str = "append",
) -> None:
    """
    Save valid and invalid records as separate Parquet datasets.
    """
    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)

    valid_df.write.mode(mode).parquet(str(VALIDATED_DIR))
    invalid_df.write.mode(mode).parquet(str(REJECTED_DIR))

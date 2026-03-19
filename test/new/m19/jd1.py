import re
from pyspark.sql import SparkSession

class sourceTargetCompare():

    def _wrap_query(self, query: str) -> str:
        q = query.strip().rstrip(";")

        # Already wrapped → return as-is
        if re.match(r"^\(.*\)\s+AS\s+\w+\s*$", q, re.DOTALL | re.IGNORECASE):
            return q

        # Plain table name → return as-is
        if re.match(r"^[\w\.\[\]]+$", q):
            return q

        # Any other SQL → wrap it
        return f"({q}) AS spark_jdbc_wrapper"

    def handler(self):
        log.info('INFO ==========> connecting the source ===========>')

        source_df = spark.read.format("jdbc") \
            .option("url", self.SOURCE_URL) \
            .option("dbtable", self._wrap_query(self.SOURCE_QUERY)) \
            .option("user", self.SOURCE_USER) \
            .option("password", self.SOURCE_PSWD) \
            .option("numPartitions", 4) \
            .option("fetchsize", 100000) \
            .option("driver", driver) \
            .load()

        log.info('INFO ==========> data extracted successfully from the source ===========>')

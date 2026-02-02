import duckdb
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List
from db.dao.athena_dao import AthenaDaoInterface
from models.stream_execution_model import GetStreamStatisticFlowguideReq, GetStreamStatisticNoteReq, SearchDomainsModelReq
from utils.common import format_keyword
from utils.constants import (
    ATHENA_WHERE_CLAUSE_SEPARATOR,
    ATHENA_WHERE_CLAUSE_OR_SEPARATOR,
)
from utils.enums import ErrorValidateMsgEnum, FlowGuideStatisticTypeEnum, FlowGuidePlayStatusEnum
from utils.validate import validate_and_split_date, format_date_slash
from aws_lambda_powertools import Logger

logger = Logger()

# Module-level singleton instance
_duckdb_instance = None
_tables_registered = False
class DuckDbDao(AthenaDaoInterface):
    """
    DuckDB implementation of AthenaDaoInterface
    Used for local / CI environments
    """
    def __new__(cls):
        """Singleton pattern - return same instance if already created"""
        global _duckdb_instance
        if _duckdb_instance is None:
            _duckdb_instance = super(DuckDbDao, cls).__new__(cls)
        return _duckdb_instance

    def __init__(self):
         # Only initialize once
        if hasattr(self, '_initialized'):
            return
        self._initialized = True
        self.data_root = "/tmp/data"
        
        os.makedirs(self.data_root, exist_ok=True)

        # Persistent DB file
        db_path = f"{self.data_root}/analytics.duckdb"
        self.athena_flowguide_table = "flowguide_events"
        self.athena_note_table = "note_events"
        self.con = duckdb.connect(database=db_path)

        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")
        self.con.execute("SET s3_region='us-east-1';")
        self.con.execute("SET s3_endpoint='192.168.1.88:4566';")
        self.con.execute("SET s3_use_ssl=false;") 
        self.con.execute("SET s3_url_style='path';")
        self.con.execute("SET s3_access_key_id='test';")
        self.con.execute("SET s3_secret_access_key='test';")
        self._register_tables()

    def _register_tables(self):
        """
        Register external parquet datasets as DuckDB views
        """
        # Real Lambda: read from S3

        flowguide_bucket = "athena-flowguide-data"
        note_bucket = "athena-note-data"
        
        flowguide_path = f"s3://{flowguide_bucket}/**/*.parquet" if flowguide_bucket else None
        note_path = f"s3://{note_bucket}/**/*.parquet" if note_bucket else None


        logger.info(f"Registering flowguide parquet: {flowguide_path}")
        logger.info(f"Registering note parquet: {note_path}")

        # Drop existing views if they exist
        self.con.execute(f"DROP VIEW IF EXISTS {self.athena_flowguide_table}")
        self.con.execute(f"DROP VIEW IF EXISTS {self.athena_note_table}")

        self.con.execute(f"""
            CREATE VIEW {self.athena_flowguide_table} AS
            SELECT * FROM read_parquet('{flowguide_path}', hive_partitioning=1)
        """)

        self.con.execute(f"""
            CREATE VIEW {self.athena_note_table} AS
            SELECT * FROM read_parquet('{note_path}', hive_partitioning=1)
        """)

    def _rewrite_sql(self, query: str) -> str:
        """
        Convert Athena SQL to DuckDB-compatible SQL
        
        Changes:
        1. Remove quoted database.table references -> use view names directly
        2. Replace Athena-specific functions with DuckDB equivalents
        3. Handle URL extraction functions
        """
        if not query or query.strip() == "":
            raise ValueError("Query cannot be empty or None")
        
        # Replace quoted database.table references with just table/view names
        # Pattern: "database"."table" -> table
        # Be more specific to avoid false matches
        query = re.sub(r'"\w+"\s*\.\s*"(\w+)"', r'\1', query)
        
        # Replace url_extract_protocol with DuckDB equivalent
        # DuckDB doesn't have url_extract_protocol, use regex instead
        query = re.sub(
            r'url_extract_protocol\(([^)]+)\)',
            r"regexp_extract(\1, '^([^:]+):', 1)",
            query
        )
        
        # Replace url_extract_host with DuckDB equivalent
        query = re.sub(
            r'url_extract_host\(([^)]+)\)',
            r"regexp_extract(\1, '://([^/]+)', 1)",
            query
        )
    
        return query

    def execute_query(self, query: str, output_location: str) -> Dict:
        """
        DuckDB version of execute_query
        - synchronous
        - ignores output_location (kept for interface compatibility)
        """
        try:
            rewritten_query = self._rewrite_sql(query)
            logger.debug(f"Original query: {query}")
            logger.debug(f"Rewritten query: {rewritten_query}")
            
            df = self.con.execute(rewritten_query).fetchdf()

            # Convert to list of dicts
            data = df.to_dict("records")
            
            # Convert numpy types to Python native types
            for row in data:
                for key, value in row.items():
                    if hasattr(value, 'item'):  # numpy scalar
                        row[key] = value.item()
                    elif hasattr(value, 'isoformat'):  # datetime
                        row[key] = value.isoformat()

            return {
                "success": True,
                "data": data
            }

        except Exception as e:
            logger.error("DuckDB query failed", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "error_code": "DUCKDB_EXECUTION_ERROR"
            }

    def _build_athena_date_filters(
        self,
        from_date: str | None,
        to_date: str | None,
        *,
        year_col: str = "year",
        month_col: str = "month",
        day_col: str = "day",
    ) -> list[str]:
        """
        Build date filters for partitioned columns (same as Athena version)
        Cast columns to INTEGER since parquet stores partition columns as VARCHAR
        """
        conditions: list[str] = []

        if from_date:
            y, m, d = validate_and_split_date(from_date)
            conditions.append(
                f"(CAST({year_col} AS INTEGER) > {y} OR (CAST({year_col} AS INTEGER) = {y} AND CAST({month_col} AS INTEGER) > {m}) "
                f"OR (CAST({year_col} AS INTEGER) = {y} AND CAST({month_col} AS INTEGER) = {m} AND CAST({day_col} AS INTEGER) >= {d}))"
            )

        if to_date:
            y, m, d = validate_and_split_date(to_date)
            conditions.append(
                f"(CAST({year_col} AS INTEGER) < {y} OR (CAST({year_col} AS INTEGER) = {y} AND CAST({month_col} AS INTEGER) < {m}) "
                f"OR (CAST({year_col} AS INTEGER) = {y} AND CAST({month_col} AS INTEGER) = {m} AND CAST({day_col} AS INTEGER) <= {d}))"
            )

        return conditions

    def _build_domain_filter(self, domains: list[str]) -> str:
        """
        Build LIKE filter for domains (same as Athena version)
        """
        if not domains:
            return ""

        conditions = ATHENA_WHERE_CLAUSE_OR_SEPARATOR.join(
            [f"page_url LIKE '{domain}%'" for domain in domains]
        )
        return f"({conditions})"

    def _build_flowguide_summary_cte(self, where_clause: str) -> str:
        """
        Build flowguide_summary CTE (same as Athena version)
        """
        return f"""flowguide_summary AS (
            SELECT
                flow_guide_id,
                COUNT(CASE WHEN play_status = {FlowGuidePlayStatusEnum.PLAYING} THEN 1 END) AS raw_play_count,
                COUNT(CASE WHEN play_status = {FlowGuidePlayStatusEnum.END} THEN 1 END) AS completed_count
            FROM {self.athena_flowguide_table}
            WHERE {where_clause}
            GROUP BY flow_guide_id
            HAVING COUNT(CASE WHEN play_status = {FlowGuidePlayStatusEnum.PLAYING} THEN 1 END) > 0
            OR COUNT(CASE WHEN play_status = {FlowGuidePlayStatusEnum.END} THEN 1 END) > 0
        )"""

    def get_flowguide_total_count(
            self,
            output_location: str,
            request: GetStreamStatisticFlowguideReq,
            flow_guide_ids_str: str
        ) -> int:
        """Same implementation as Athena, but uses execute_query"""
        if not flow_guide_ids_str:
            return 0

        where_conditions = []
        where_conditions.append(f"flow_guide_id IN ({flow_guide_ids_str})")

        date_conditions = self._build_athena_date_filters(request.from_date, request.to_date)
        where_conditions.extend(date_conditions)
        
        domain_filter = self._build_domain_filter(request.domains)
        if domain_filter:
            where_conditions.append(domain_filter)
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        flowguide_summary_cte = self._build_flowguide_summary_cte(where_clause)

        if request.type == FlowGuideStatisticTypeEnum.RANKING:
            query = f"""
            WITH {flowguide_summary_cte}
            SELECT COALESCE(SUM(GREATEST(raw_play_count, completed_count)), 0) AS total_count
            FROM flowguide_summary
            """
        elif request.type == FlowGuideStatisticTypeEnum.COMPLETION_RATE:
            query = f"""
            SELECT COUNT(*) AS total_count
            FROM {self.athena_flowguide_table}
            WHERE {where_clause} AND play_status = {FlowGuidePlayStatusEnum.END}
            """
        elif request.type == FlowGuideStatisticTypeEnum.INTERRUPTION_RATE:
            query = f"""
            WITH {flowguide_summary_cte},
            adjusted_summary AS (
                SELECT
                    flow_guide_id,
                    GREATEST(raw_play_count, completed_count) AS adjusted_play_count,
                    completed_count
                FROM flowguide_summary
            )
            SELECT COALESCE(SUM(adjusted_play_count - completed_count), 0) AS total_count
            FROM adjusted_summary
            """
        else:
            query = f"""
            SELECT COUNT(*) AS total_count
            FROM {self.athena_flowguide_table}
            WHERE {where_clause}
            """
        
        result = self.execute_query(query, output_location)
        total_count = int(result["data"][0]["total_count"]) if result["success"] and result["data"] else 0

        # Add query for distinct flow_guide_id count
        query = f"""
        SELECT COUNT(DISTINCT flow_guide_id) AS total_flow_count
        FROM {self.athena_flowguide_table}
        WHERE {where_clause}
        """
        result = self.execute_query(query, output_location)
        total_flow_count = int(result["data"][0]["total_flow_count"]) if result["success"] else 0

        return total_count, total_flow_count  # Return tuple

    def get_note_total_count(
            self,
            output_location: str,
            request: GetStreamStatisticNoteReq,
            note_ids_str: str
        ) -> int:
        """Same implementation as Athena"""
        if not note_ids_str:
            return 0

        where_conditions = []
        date_conditions = self._build_athena_date_filters(request.from_date, request.to_date)
        where_conditions.extend(date_conditions)
        where_conditions.append(f"note_id IN ({note_ids_str})")

        if where_conditions:
            where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
            query = f"""
            SELECT COUNT(*) AS total_count
            FROM {self.athena_note_table}
            WHERE {where_clause}
            """
        else:
            query = f"""
            SELECT COUNT(*) AS total_count
            FROM {self.athena_note_table}
            """
        
        result = self.execute_query(query, output_location)
        total_count = int(result["data"][0]["total_count"]) if result["success"] else 0

        # Build query for distinct note_id count
        query = f"""
        SELECT COUNT(DISTINCT note_id) AS total_note_count
        FROM "{self.athena_note_table}"
        WHERE {where_clause}
        """
        result = self.execute_query(query, output_location)
        total_note_count = int(result["data"][0]["total_note_count"]) if result["success"] else 0

        return total_count, total_note_count

    def get_flowguide_ranking_data(
            self,
            page: int,
            page_size: int,
            output_location: str,
            request: GetStreamStatisticFlowguideReq,
            flow_guide_ids_str: str
        ) -> Dict:
        """Same implementation as Athena"""
        start_rank = (page - 1) * page_size + 1
        end_rank = page * page_size
        
        if not flow_guide_ids_str:
            return {
                "success": True,
                "data": []
            }
        
        where_conditions = []
        where_conditions.append(f"flow_guide_id IN ({flow_guide_ids_str})")
        date_conditions = self._build_athena_date_filters(request.from_date, request.to_date)
        where_conditions.extend(date_conditions)
        domain_filter = self._build_domain_filter(request.domains)
        if domain_filter:
            where_conditions.append(domain_filter)
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        flowguide_summary_cte = self._build_flowguide_summary_cte(where_clause)
        
        query = f"""
        WITH {flowguide_summary_cte},
        adjusted_counts AS (
            SELECT
                flow_guide_id,
                GREATEST(raw_play_count, completed_count) AS play_count
            FROM flowguide_summary
        ),
        ranked_data AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY play_count DESC, flow_guide_id DESC) AS row_number,
                RANK() OVER (ORDER BY play_count DESC) AS ranking,
                flow_guide_id,
                play_count
            FROM adjusted_counts
        )
        SELECT 
            ranking,
            flow_guide_id,
            play_count
        FROM ranked_data
        WHERE row_number BETWEEN {start_rank} AND {end_rank}
        ORDER BY ranking, flow_guide_id DESC
        """
        return self.execute_query(query, output_location)

    def get_flowguide_completion_rate_data(
            self,
            page: int,
            page_size: int,
            output_location: str,
            request: GetStreamStatisticFlowguideReq,
            flow_guide_ids_str: str
        ) -> Dict:
        """Same implementation as Athena"""
        start_rank = (page - 1) * page_size + 1
        end_rank = page * page_size
        
        if not flow_guide_ids_str:
            return {
                "success": True,
                "data": []
            }
        
        where_conditions = []
        where_conditions.append(f"flow_guide_id IN ({flow_guide_ids_str})")
        date_conditions = self._build_athena_date_filters(request.from_date, request.to_date)
        where_conditions.extend(date_conditions)
        domain_filter = self._build_domain_filter(request.domains)
        if domain_filter:
            where_conditions.append(domain_filter)
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        flowguide_summary_cte = self._build_flowguide_summary_cte(where_clause)
        
        query = f"""
        WITH {flowguide_summary_cte},
        adjusted_summary AS (
            SELECT
                flow_guide_id,
                GREATEST(raw_play_count, completed_count) AS play_count,
                completed_count
            FROM flowguide_summary
        ),
        ranked_data AS (
            SELECT
                adjusted_summary.flow_guide_id,
                adjusted_summary.play_count,
                adjusted_summary.completed_count,
                ROUND(
                    COALESCE(adjusted_summary.completed_count * 100.0 / NULLIF(adjusted_summary.play_count, 0), 0), 2
                ) AS completion_rate,
                ROW_NUMBER() OVER (
                    ORDER BY 
                        COALESCE(adjusted_summary.completed_count * 1.0 / NULLIF(adjusted_summary.play_count, 0), 0) DESC,
                        adjusted_summary.flow_guide_id DESC
                ) AS row_number,
                RANK() OVER (
                    ORDER BY 
                        COALESCE(adjusted_summary.completed_count * 1.0 / NULLIF(adjusted_summary.play_count, 0), 0) DESC
                ) AS ranking,
                COUNT(*) OVER() AS total_data_count
            FROM adjusted_summary
        )
        SELECT
            ranked_data.ranking,
            ranked_data.flow_guide_id,
            ranked_data.play_count,
            ranked_data.completed_count,
            ranked_data.completion_rate,
            ranked_data.total_data_count
        FROM ranked_data
        WHERE ranked_data.row_number BETWEEN {start_rank} AND {end_rank}
        ORDER BY ranked_data.ranking, ranked_data.play_count DESC, ranked_data.flow_guide_id DESC
        """
        return self.execute_query(query, output_location)

    def get_flowguide_interruption_rate_data(
            self,
            page: int,
            page_size: int,
            output_location: str,
            request: GetStreamStatisticFlowguideReq,
            flow_guide_ids_str: str
        ) -> Dict:
        """Same implementation as Athena"""
        start_rank = (page - 1) * page_size + 1
        end_rank = page * page_size
        
        if not flow_guide_ids_str:
            return {
                "success": True,
                "data": []
            }
        
        where_conditions = []
        where_conditions.append(f"flow_guide_id IN ({flow_guide_ids_str})")
        date_conditions = self._build_athena_date_filters(request.from_date, request.to_date)
        where_conditions.extend(date_conditions)
        domain_filter = self._build_domain_filter(request.domains)
        if domain_filter:
            where_conditions.append(domain_filter)
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        flowguide_summary_cte = self._build_flowguide_summary_cte(where_clause)
        
        query = f"""
        WITH {flowguide_summary_cte},
        adjusted_summary AS (
            SELECT
                flow_guide_id,
                GREATEST(raw_play_count, completed_count) AS play_count,
                completed_count
            FROM flowguide_summary
        ),
        calculated_summary AS (
            SELECT
                flow_guide_id,
                play_count,
                completed_count,
                (play_count - completed_count) AS interrupted_count
            FROM adjusted_summary
        ),
        ranked_data AS (
            SELECT
                calculated_summary.flow_guide_id,
                calculated_summary.play_count,
                calculated_summary.interrupted_count,
                ROUND(
                    COALESCE(calculated_summary.interrupted_count * 100.0 / NULLIF(calculated_summary.play_count, 0), 0), 2
                ) AS interruption_rate,
                ROW_NUMBER() OVER (
                    ORDER BY 
                        COALESCE(calculated_summary.interrupted_count * 1.0 / NULLIF(calculated_summary.play_count, 0), 0) DESC,
                        calculated_summary.flow_guide_id DESC
                ) AS row_number,
                RANK() OVER (
                    ORDER BY 
                        COALESCE(calculated_summary.interrupted_count * 1.0 / NULLIF(calculated_summary.play_count, 0), 0) DESC
                ) AS ranking,
                COUNT(*) OVER() AS total_data_count
            FROM calculated_summary
        )
        SELECT
            ranked_data.ranking,
            ranked_data.flow_guide_id,
            ranked_data.play_count,
            ranked_data.interrupted_count,
            ranked_data.interruption_rate,
            ranked_data.total_data_count
        FROM ranked_data
        WHERE ranked_data.row_number BETWEEN {start_rank} AND {end_rank}
        ORDER BY ranked_data.ranking, ranked_data.play_count DESC, ranked_data.flow_guide_id DESC
        """
        return self.execute_query(query, output_location)

    def get_note_ranking_data(
            self,
            page: int,
            page_size: int,
            output_location: str,
            request: GetStreamStatisticNoteReq,
            note_ids_str: str
        ) -> Dict:
        """Same implementation as Athena"""
        start_rank = (page - 1) * page_size + 1
        end_rank = page * page_size
        
        where_conditions = []
        
        if not note_ids_str:
            return {
                "success": True,
                "data": []
            }
        
        where_conditions.append(f"note_id IN ({note_ids_str})")
        date_conditions = self._build_athena_date_filters(request.from_date, request.to_date)
        where_conditions.extend(date_conditions)
        domain_filter = self._build_domain_filter(request.domains)
        if domain_filter:
            where_conditions.append(domain_filter)
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        
        query = f"""
        WITH display_counts AS (
            SELECT 
                note_id,
                COUNT(*) AS display_count
            FROM {self.athena_note_table}
            WHERE {where_clause}
            GROUP BY note_id
        ),
        ranked_data AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY display_count DESC, note_id DESC) AS row_number,
                RANK() OVER (ORDER BY display_count DESC) AS ranking,
                note_id,
                display_count,
                COUNT(*) OVER() AS total_data_count
            FROM display_counts
        )
        SELECT 
            ranking,
            note_id,
            display_count,
            total_data_count
        FROM ranked_data
        WHERE row_number BETWEEN {start_rank} AND {end_rank}
        ORDER BY ranking, note_id DESC
        """
        return self.execute_query(query, output_location)

    def get_recently_used_flowguide_data(
        self,
        offset: int,
        page_size: int,
        output_location: str,
        request: GetStreamStatisticFlowguideReq,
        flow_guide_ids_str: str
    ) -> Dict:
        """
        Get recently used flowguide data ordered by most recent timestamp
        
        Returns flow guides ordered by their latest usage timestamp (most recent first)
        with pagination support.
        """
        if not flow_guide_ids_str:
            return {
                "success": True,
                "data": []
            }
        
        # Build WHERE conditions
        where_conditions = []
        where_conditions.append(f"flow_guide_id IN ({flow_guide_ids_str})")
        
        # Add date filters
        date_conditions = self._build_athena_date_filters(request.from_date, request.to_date)
        where_conditions.extend(date_conditions)

        # status filter
        where_conditions.append(f"play_status = {FlowGuidePlayStatusEnum.PLAYING}")

        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        
        # Query to get most recent flow_guide_ids ordered by latest timestamp
        # Get distinct flow_guide_id with their latest timestamp, then order by timestamp DESC
        query = f"""
            WITH latest_events AS (
                SELECT 
                    flow_guide_id,
                    MAX(timestamp) AS latest_timestamp
                FROM {self.athena_flowguide_table}
                WHERE {where_clause}
                GROUP BY flow_guide_id
            )
            SELECT 
                flow_guide_id,
                latest_timestamp
            FROM latest_events
            ORDER BY latest_timestamp DESC
            LIMIT {page_size} OFFSET {offset}
        """
        
        return self.execute_query(query, output_location)

    def get_recently_used_note_data(
        self,
        offset: int,
        page_size: int,
        output_location: str,
        request: GetStreamStatisticNoteReq,
        note_ids_str: str
    ) -> Dict:
        """
        Get recently used note data ordered by most recent timestamp
        
        Returns notes ordered by their latest usage timestamp (most recent first)
        with pagination support.
        """
        if not note_ids_str:
            return {
                "success": True,
                "data": []
            }
        
        # Build WHERE conditions
        where_conditions = []
        where_conditions.append(f"note_id IN ({note_ids_str})")
        
        # Add date filters
        date_conditions = self._build_athena_date_filters(request.from_date, request.to_date)
        where_conditions.extend(date_conditions)

        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        
        # Query to get most recent note_ids ordered by latest timestamp
        # Get distinct note_id with their latest timestamp, then order by timestamp DESC
        query = f"""
            WITH latest_events AS (
                SELECT 
                    note_id,
                    MAX(timestamp) AS latest_timestamp
                FROM {self.athena_note_table}
                WHERE {where_clause}
                GROUP BY note_id
            )
            SELECT 
                note_id,
                latest_timestamp
            FROM latest_events
            ORDER BY latest_timestamp DESC
            LIMIT {page_size} OFFSET {offset}
        """
        
        return self.execute_query(query, output_location)

    def _get_domain_query(
        self,
        table_name: str | None,
        id_column: str,
        ids: List[int],
        request: SearchDomainsModelReq,
        output_location: str
    ) -> Dict:
        """Same implementation as Athena, but uses DuckDB views"""
        # For DuckDB, use view names directly instead of Athena table names
        # Handle None table_name BEFORE building the query - this prevents None from appearing in SQL
        if table_name is None:
            # Map based on id_column to determine which view to use
            if id_column == "flow_guide_id":
                table_name = self.athena_flowguide_table
            elif id_column == "note_id":
                table_name = self.athena_note_table
            else:
                raise ValueError(f"Unknown id_column: {id_column}, cannot determine table name. table_name was None.")
        
        where_conditions = []

        if ids:
            ids_str = ','.join(map(str, ids))
            where_conditions.append(f"{id_column} IN ({ids_str})")

        if request and request.keyword:
            keyword = request.keyword.strip()
            if keyword:
                keyword = format_keyword(keyword).lower()
                where_conditions.append(f"LOWER(page_url) LIKE '%{keyword}%'")

        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)

        # Now table_name is guaranteed to be a valid string, not None
        query = f"""
            SELECT DISTINCT
                CASE
                    WHEN page_url LIKE '%://%'
                        THEN CONCAT(regexp_extract(page_url, '^([^:]+):', 1), '://', regexp_extract(page_url, '://([^/]+)', 1))
                    ELSE page_url
                END AS domain
            FROM {table_name}
            {f"WHERE {where_clause}" if where_clause else ""}
        """

        return self.execute_query(query, output_location)

    def get_ranked_notes_count(self, output_location: str, note_ids_str: str) -> int:
        """
        Get count of ranked notes maximum 10
        
        Returns the count of top 10 ranked notes by display_count.
        Maximum return value is 10.
        """
        if not note_ids_str:
            return 0

        from_date = datetime.now() - timedelta(days=30)
        to_date = datetime.now()
        # Convert datetime to string format expected by _build_athena_date_filters
        from_date_str = format_date_slash(from_date)
        to_date_str = format_date_slash(to_date)
        
        # Build WHERE conditions
        where_conditions = []
        where_conditions.append(f"note_id IN ({note_ids_str})")
        
        # Add date filters if provided
        date_conditions = self._build_athena_date_filters(from_date_str, to_date_str)
        where_conditions.extend(date_conditions)
        
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        
        # Query to get top 10 ranked notes by display_count
        query = f"""
            WITH display_counts AS (
                SELECT 
                    note_id,
                    COUNT(*) AS display_count
                FROM {self.athena_note_table}
                WHERE {where_clause}
                GROUP BY note_id
            ),
            ranked_data AS (
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY display_count DESC, note_id DESC) AS row_number,
                    note_id
                FROM display_counts
            )
            SELECT COUNT(*) AS count
            FROM ranked_data
            WHERE row_number <= 10
        """
        result = self.execute_query(query, output_location)
        if result.get("success") and result.get("data"):
            return int(result["data"][0]["count"])
        return 0

    def get_recently_used_notes_count(self, output_location: str, note_ids_str: str) -> int:
        """
        Get count of recently used notes in the last 30 days
        
        Returns the count of distinct notes that were used within the date range
        specified in the request (caller already specifies 30 days).
        """
        if not note_ids_str:
            return 0

        from_date = datetime.now() - timedelta(days=30)
        to_date = datetime.now()
        # Convert datetime to string format expected by _build_athena_date_filters
        from_date_str = format_date_slash(from_date)
        to_date_str = format_date_slash(to_date)

        # Build WHERE conditions
        where_conditions = []
        where_conditions.append(f"note_id IN ({note_ids_str})")
        
        date_conditions = self._build_athena_date_filters(from_date_str, to_date_str)
        where_conditions.extend(date_conditions)
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        
        # Query to get distinct note_ids from the specified date range
        query = f"""
            WITH latest_events AS (
                SELECT 
                    note_id,
                    MAX(timestamp) AS latest_timestamp
                FROM {self.athena_note_table}
                WHERE {where_clause}
                GROUP BY note_id
            )
            SELECT COUNT(DISTINCT note_id) AS count
            FROM latest_events
        """
        
        result = self.execute_query(query, output_location)
        if result.get("success") and result.get("data"):
            return int(result["data"][0]["count"])
        return 0

    def get_ranked_flowguides_count(self, output_location: str, flowguide_ids_str: str) -> int:
        """
        Get count of ranked flowguides maximum 10
        
        Returns the count of top 10 ranked flowguides by play_count.
        Maximum return value is 10.
        Uses the same ranking logic as get_flowguide_ranking_data.
        """
        if not flowguide_ids_str:
            return 0

        from_date = datetime.now() - timedelta(days=30)
        to_date = datetime.now()
        # Convert datetime to string format expected by _build_athena_date_filters
        from_date_str = format_date_slash(from_date)
        to_date_str = format_date_slash(to_date)
        
        # Build WHERE conditions
        where_conditions = []
        where_conditions.append(f"flow_guide_id IN ({flowguide_ids_str})")
        
        # Add date filters if provided
        date_conditions = self._build_athena_date_filters(from_date_str, to_date_str)
        where_conditions.extend(date_conditions)
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        flowguide_summary_cte = self._build_flowguide_summary_cte(where_clause)
        
        # Query to get top 10 ranked flowguides by play_count (same logic as get_flowguide_ranking_data)
        query = f"""
            WITH {flowguide_summary_cte},
            adjusted_counts AS (
                SELECT
                    flow_guide_id,
                    GREATEST(raw_play_count, completed_count) AS play_count
                FROM flowguide_summary
            ),
            ranked_data AS (
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY play_count DESC, flow_guide_id DESC) AS row_number,
                    flow_guide_id
                FROM adjusted_counts
            )
            SELECT COUNT(*) AS count
            FROM ranked_data
            WHERE row_number <= 10
        """
        result = self.execute_query(query, output_location)
        if result.get("success") and result.get("data"):
            return int(result["data"][0]["count"])
        return 0

    def get_recently_used_flowguides_count(self, output_location: str, flowguide_ids_str: str) -> int:
        """
        Get count of recently used flowguides in the last 30 days
        
        Returns the count of distinct flowguides that were used within the date range
        specified in the request (caller already specifies 30 days).
        """
        if not flowguide_ids_str:
            return 0

        from_date = datetime.now() - timedelta(days=30)
        to_date = datetime.now()
        # Convert datetime to string format expected by _build_athena_date_filters
        from_date_str = format_date_slash(from_date)
        to_date_str = format_date_slash(to_date)

        # Build WHERE conditions
        where_conditions = []
        where_conditions.append(f"flow_guide_id IN ({flowguide_ids_str})")
        
        date_conditions = self._build_athena_date_filters(from_date_str, to_date_str)
        where_conditions.extend(date_conditions)
        
        where_clause = ATHENA_WHERE_CLAUSE_SEPARATOR.join(where_conditions)
        
        # Query to get distinct flow_guide_ids from the specified date range
        query = f"""
            WITH latest_events AS (
                SELECT 
                    flow_guide_id,
                    MAX(timestamp) AS latest_timestamp
                FROM {self.athena_flowguide_table}
                WHERE {where_clause}
                GROUP BY flow_guide_id
            )
            SELECT COUNT(DISTINCT flow_guide_id) AS count
            FROM latest_events
        """
        
        result = self.execute_query(query, output_location)
        if result.get("success") and result.get("data"):
            return int(result["data"][0]["count"])
        return 0
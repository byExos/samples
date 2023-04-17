import sqlite3
import time
from typing import Iterable, Dict, Any, Union, Optional
from uuid import UUID
import uuid

from azure.eventhub._eventprocessor.checkpoint_store import CheckpointStore


class SqliteCheckpointStore(CheckpointStore):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ownership (
                fully_qualified_namespace TEXT,
                eventhub_name TEXT,
                consumer_group TEXT,
                partition_id TEXT,
                owner_id TEXT,
                last_modified_time TEXT,
                etag TEXT,
                PRIMARY KEY (fully_qualified_namespace, eventhub_name, consumer_group, partition_id)
            )
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoint (
                fully_qualified_namespace TEXT,
                eventhub_name TEXT,
                consumer_group TEXT,
                partition_id TEXT,
                sequence_number INTEGER,
                offset TEXT,
                PRIMARY KEY (fully_qualified_namespace, eventhub_name, consumer_group, partition_id)
            )
            """
        )
        self.conn.commit()

    async def list_ownership(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        **kwargs: Any
    ) -> Iterable[Dict[str, Any]]:
        self.cursor.execute(
            """
            SELECT fully_qualified_namespace, eventhub_name, consumer_group, partition_id, owner_id,
                last_modified_time, etag
            FROM ownership
            WHERE fully_qualified_namespace=? AND eventhub_name=? AND consumer_group=?
            """,
            (fully_qualified_namespace, eventhub_name, consumer_group)
        )
        rows = self.cursor.fetchall()
        return [
            {
                "fully_qualified_namespace": row[0],
                "eventhub_name": row[1],
                "consumer_group": row[2],
                "partition_id": row[3],
                "owner_id": UUID(row[4]),
                "last_modified_time": float(row[5]),
                "etag": row[6],
            }
            for row in rows
        ]

    async def claim_ownership(
        self,
        ownership_list: Iterable[Dict[str, Any]],
        **kwargs: Any
    ) -> Iterable[Dict[str, Any]]:
        claimed_ownerships = []
        for ownership in ownership_list:
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO ownership
                    (fully_qualified_namespace, eventhub_name, consumer_group, partition_id, owner_id, last_modified_time, etag)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ownership["fully_qualified_namespace"],
                    ownership["eventhub_name"],
                    ownership["consumer_group"],
                    ownership["partition_id"],
                    str(ownership["owner_id"]),
                    str(time.time()),
                    str(uuid.uuid4()),
                ),
            )
            claimed_ownerships.append(ownership)
        self.conn.commit()
        return claimed_ownerships

    async def update_checkpoint(self, checkpoint: Dict[str, Optional[Union[str, int]]], **kwargs: Any) -> None:
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO checkpoint (
                fully_qualified_namespace,
                eventhub_name,
                consumer_group,
                partition_id,
                sequence_number,
                offset
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                checkpoint["fully_qualified_namespace"],
                checkpoint["eventhub_name"],
                checkpoint["consumer_group"],
                checkpoint["partition_id"],
                checkpoint["sequence_number"],
                checkpoint["offset"],
            ),
        )
        self.conn.commit()

    async def list_checkpoints(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        **kwargs: Any
    ) -> Iterable[Dict[str, Any]]:
        self.cursor.execute(
            """
            SELECT partition_id, sequence_number, offset
            FROM checkpoint
            WHERE fully_qualified_namespace = ? AND
                eventhub_name = ? AND
                consumer_group = ?
            """,
            (fully_qualified_namespace, eventhub_name, consumer_group),
        )
        return [
            {
                "fully_qualified_namespace": fully_qualified_namespace,
                "eventhub_name": eventhub_name,
                "consumer_group": consumer_group,
                "partition_id": row[0],
                "sequence_number": row[1],
                "offset": row[2],
            } 
            for row in self.cursor.fetchall()]

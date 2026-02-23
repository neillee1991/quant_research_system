"""
Database Migration Management Tool

This tool manages database schema migrations with version control,
rollback support, and migration history tracking.
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2 import sql

# Add parent directory to path to import db_client
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from store.postgres_client import db_client


class MigrationManager:
    """Manages database migrations with version control"""

    def __init__(self):
        self.migrations_dir = Path(__file__).parent
        self.db = db_client
        self._ensure_migration_table()

    def _ensure_migration_table(self):
        """Create migration history table if it doesn't exist"""
        sql_create = """
        CREATE TABLE IF NOT EXISTS migration_history (
            id SERIAL PRIMARY KEY,
            migration_name VARCHAR(255) NOT NULL UNIQUE,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            execution_time_ms INTEGER,
            status VARCHAR(20) NOT NULL DEFAULT 'success',
            error_message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_migration_history_applied_at
        ON migration_history(applied_at DESC);
        """
        self.db.execute(sql_create)
        print("✓ Migration history table ready")

    def get_applied_migrations(self):
        """Get list of applied migrations"""
        df = self.db.query(
            "SELECT migration_name FROM migration_history WHERE status = 'success' ORDER BY applied_at"
        )
        return set(df['migration_name'].to_list()) if not df.is_empty() else set()

    def get_pending_migrations(self):
        """Get list of pending migrations"""
        applied = self.get_applied_migrations()
        all_migrations = sorted([
            f.stem for f in self.migrations_dir.glob("*.sql")
            if f.stem != 'init'  # Skip init.sql
        ])
        return [m for m in all_migrations if m not in applied]

    def apply_migration(self, migration_name: str):
        """Apply a single migration"""
        migration_file = self.migrations_dir / f"{migration_name}.sql"

        if not migration_file.exists():
            raise FileNotFoundError(f"Migration file not found: {migration_file}")

        print(f"\n{'='*60}")
        print(f"Applying migration: {migration_name}")
        print(f"{'='*60}")

        # Read migration SQL
        with open(migration_file, 'r', encoding='utf-8') as f:
            migration_sql = f.read()

        start_time = datetime.now()

        try:
            # Execute migration without transaction (for CONCURRENTLY support)
            # Get a raw connection and set autocommit
            conn = self.db._pool.getconn()
            try:
                old_isolation_level = conn.isolation_level
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                with conn.cursor() as cur:
                    cur.execute(migration_sql)

                conn.set_isolation_level(old_isolation_level)
            finally:
                self.db._pool.putconn(conn)

            # Calculate execution time
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)

            # Record success
            self.db.execute(
                """
                INSERT INTO migration_history (migration_name, execution_time_ms, status)
                VALUES (%s, %s, 'success')
                """,
                (migration_name, execution_time)
            )

            print(f"✓ Migration applied successfully in {execution_time}ms")
            return True

        except Exception as e:
            # Record failure
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            error_msg = str(e)

            try:
                self.db.execute(
                    """
                    INSERT INTO migration_history (migration_name, execution_time_ms, status, error_message)
                    VALUES (%s, %s, 'failed', %s)
                    """,
                    (migration_name, execution_time, error_msg)
                )
            except:
                pass  # If we can't record the error, just continue

            print(f"✗ Migration failed: {error_msg}")
            raise

    def apply_all_pending(self):
        """Apply all pending migrations"""
        pending = self.get_pending_migrations()

        if not pending:
            print("\n✓ No pending migrations")
            return

        print(f"\nFound {len(pending)} pending migration(s):")
        for m in pending:
            print(f"  - {m}")

        for migration in pending:
            self.apply_migration(migration)

        print(f"\n{'='*60}")
        print(f"✓ All migrations applied successfully")
        print(f"{'='*60}")

    def show_status(self):
        """Show migration status"""
        applied = self.get_applied_migrations()
        pending = self.get_pending_migrations()

        print("\n" + "="*60)
        print("MIGRATION STATUS")
        print("="*60)

        print(f"\nApplied migrations: {len(applied)}")
        if applied:
            df = self.db.query(
                """
                SELECT migration_name, applied_at, execution_time_ms, status
                FROM migration_history
                ORDER BY applied_at DESC
                LIMIT 10
                """
            )
            print("\nRecent migrations:")
            for row in df.to_dicts():
                status_icon = "✓" if row['status'] == 'success' else "✗"
                print(f"  {status_icon} {row['migration_name']} "
                      f"({row['execution_time_ms']}ms) - {row['applied_at']}")

        print(f"\nPending migrations: {len(pending)}")
        if pending:
            for m in pending:
                print(f"  - {m}")

        print("\n" + "="*60)

    def rollback_last(self):
        """Rollback the last migration (if rollback script exists)"""
        df = self.db.query(
            """
            SELECT migration_name FROM migration_history
            WHERE status = 'success'
            ORDER BY applied_at DESC
            LIMIT 1
            """
        )

        if df.is_empty():
            print("No migrations to rollback")
            return

        last_migration = df['migration_name'][0]
        rollback_file = self.migrations_dir / f"{last_migration}_rollback.sql"

        if not rollback_file.exists():
            print(f"✗ No rollback script found for: {last_migration}")
            print(f"  Expected: {rollback_file}")
            return

        print(f"\nRolling back migration: {last_migration}")

        with open(rollback_file, 'r', encoding='utf-8') as f:
            rollback_sql = f.read()

        try:
            self.db.execute(rollback_sql)
            self.db.execute(
                "DELETE FROM migration_history WHERE migration_name = %s",
                (last_migration,)
            )
            print(f"✓ Migration rolled back successfully")
        except Exception as e:
            print(f"✗ Rollback failed: {e}")
            raise


def main():
    """Main entry point"""
    manager = MigrationManager()

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python apply_migrations.py status    - Show migration status")
        print("  python apply_migrations.py migrate   - Apply all pending migrations")
        print("  python apply_migrations.py rollback  - Rollback last migration")
        sys.exit(1)

    command = sys.argv[1]

    try:
        if command == "status":
            manager.show_status()
        elif command == "migrate":
            manager.apply_all_pending()
        elif command == "rollback":
            manager.rollback_last()
        else:
            print(f"Unknown command: {command}")
            sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

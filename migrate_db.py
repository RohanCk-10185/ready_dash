#!/usr/bin/env python3
"""
Database migration script to fix the unique constraint issue.
This script will recreate the database with the correct schema.
"""

import os
import sqlite3
from datetime import datetime

def migrate_database():
    """Migrate the database to fix the unique constraint issue"""
    
    db_path = os.getenv('DATABASE_URL', 'sqlite:///./eks_dashboard.db').replace('sqlite:///', '')
    
    if not os.path.exists(db_path):
        print("Database doesn't exist yet, no migration needed.")
        return
    
    # Backup the existing database
    backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"Creating backup: {backup_path}")
    
    # Copy the database file
    import shutil
    shutil.copy2(db_path, backup_path)
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if the unique constraint exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='index' AND name='unique_cluster_per_region'
        """)
        
        if cursor.fetchone():
            print("Migration already applied.")
            return
        
        # Drop the old unique constraint on name
        cursor.execute("""
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name='cluster_registry'
        """)
        
        table_sql = cursor.fetchone()[0]
        print("Current table schema:", table_sql)
        
        # Create new table with correct constraints
        cursor.execute("DROP TABLE IF EXISTS cluster_registry_new")
        
        cursor.execute("""
            CREATE TABLE cluster_registry_new (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR NOT NULL,
                account_id VARCHAR NOT NULL,
                region VARCHAR NOT NULL,
                arn VARCHAR UNIQUE,
                version VARCHAR,
                platform_version VARCHAR,
                endpoint VARCHAR,
                status VARCHAR,
                created_at DATETIME,
                role_arn VARCHAR,
                tags JSON,
                health_issues JSON,
                eks_auto_mode VARCHAR,
                certificate_authority_data TEXT,
                networking JSON,
                oidc_provider_url VARCHAR,
                security_insights JSON,
                workloads JSON,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                table_name VARCHAR NOT NULL UNIQUE,
                CONSTRAINT unique_cluster_per_region UNIQUE (name, account_id, region)
            )
        """)
        
        # Copy data from old table to new table
        cursor.execute("""
            INSERT INTO cluster_registry_new 
            SELECT * FROM cluster_registry
        """)
        
        # Drop old table and rename new table
        cursor.execute("DROP TABLE cluster_registry")
        cursor.execute("ALTER TABLE cluster_registry_new RENAME TO cluster_registry")
        
        # Create indexes
        cursor.execute("CREATE INDEX ix_cluster_registry_id ON cluster_registry (id)")
        cursor.execute("CREATE INDEX ix_cluster_registry_name ON cluster_registry (name)")
        cursor.execute("CREATE INDEX ix_cluster_registry_account_id ON cluster_registry (account_id)")
        cursor.execute("CREATE INDEX ix_cluster_registry_region ON cluster_registry (region)")
        
        conn.commit()
        print("Migration completed successfully!")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()

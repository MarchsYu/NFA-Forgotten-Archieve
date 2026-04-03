"""
Migration script to add legend_members table.

Run this after init_db.py or use it to update an existing database.
"""
from sqlalchemy import text

from src.db.session import engine


def migrate():
    with engine.connect() as conn:
        # Check if table exists
        result = conn.execute(text(
            "SELECT EXISTS (SELECT FROM information_schema.tables "
            "WHERE table_name = 'legend_members')"
        ))
        exists = result.scalar()
        
        if exists:
            print("legend_members table already exists, skipping migration")
            return
        
        print("Creating legend_members table...")
        
        conn.execute(text("""
            CREATE TABLE legend_members (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                member_id UUID NOT NULL UNIQUE REFERENCES members(id) ON DELETE CASCADE,
                archive_status VARCHAR(32) NOT NULL,
                simulation_enabled BOOLEAN NOT NULL DEFAULT false,
                archived_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                restored_at TIMESTAMP WITH TIME ZONE,
                CONSTRAINT ck_legend_member_archive_status 
                    CHECK (archive_status IN ('archived', 'restored')),
                CONSTRAINT ck_legend_member_restored_no_simulation 
                    CHECK (NOT (archive_status = 'restored' AND simulation_enabled = true))
            )
        """))
        
        conn.commit()
        print("legend_members table created successfully")


if __name__ == "__main__":
    migrate()

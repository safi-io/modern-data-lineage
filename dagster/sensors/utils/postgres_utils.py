# Check if new file exists in postgres with status new.


def has_new_files(cursor) -> bool:
    cursor.execute("""
        SELECT 1 FROM file_status
        WHERE status = 'new'
        LIMIT 1;
    """)
    return cursor.fetchone() is not None


# Marks all files as in-progress, and assigns job id.


def mark_files_inprogress(cursor, job_id: str):
    cursor.execute(
        """
        UPDATE file_status
        SET status = 'inprogress',
            job_id = %s,
            started_at = NOW(),
            updated_at = NOW()
        WHERE status = 'new';
        """,
        (job_id,),
    )

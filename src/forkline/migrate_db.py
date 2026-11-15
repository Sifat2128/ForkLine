
from sqlalchemy import inspect, text

from forkline.database import Base, engine
from forkline import models


def migrate() -> None:
    inspector = inspect(engine)

    existing_columns = {
        column["name"]
        for column in inspector.get_columns("jobs")
    }

    with engine.begin() as connection:
        if "started_at" not in existing_columns:
            connection.execute(
                text(
                    "ALTER TABLE jobs "
                    "ADD COLUMN started_at TIMESTAMP WITH TIME ZONE"
                )
            )

        if "worker_id" not in existing_columns:
            connection.execute(
                text(
                    "ALTER TABLE jobs "
                    "ADD COLUMN worker_id VARCHAR(100)"
                )
            )

    # Create any missing tables defined in our SQLAlchemy models.
    # Existing tables and their data are preserved.
    Base.metadata.create_all(bind=engine)

    print("Database migration completed successfully.")


if __name__ == "__main__":
    migrate()

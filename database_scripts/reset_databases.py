import subprocess


def _restore_one(service, port: int):
    cmd = [
        "docker", "exec", service,
        "mongorestore",
        "--drop",
        "--gzip",
        f"--archive=/generated_data/mongo_snapshot.gz",
        "--nsFrom", "testdb.*",
        "--nsTo", "testdb.*"
    ]
    subprocess.check_call(cmd,
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL  # ← wyciszenie
                          )


def restore_mongo_all():
    _restore_one("mongodb-latest", 27017)
    _restore_one("mongodb-old", 27018)
    print("↺  Mongo rollback OK")

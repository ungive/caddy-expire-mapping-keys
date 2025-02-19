import redis
import time
import sys
import os
import datetime
import traceback
from urllib.parse import unquote


# workaround to fix non-expiring mapping keys and ever-growing surrogate keys.
# see https://github.com/darkweak/storages/issues/23


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
DESIRED_EXPIRE = int(os.getenv("DESIRED_EXPIRE", "60"))
DESIRED_INTERVAL = int(os.getenv("DESIRED_INTERVAL", "30"))

MAPPING_KEY_PREFIX = "IDX_/"
MAPPING_KEY_PREFIX_BYTES = str.encode(MAPPING_KEY_PREFIX)
MAPPING_KEY_PATTERN = f"{MAPPING_KEY_PREFIX}*"

SURROGATE_KEY = "SURROGATE_"


def log(message):
    ts = datetime.datetime.now().isoformat()
    print(f"{ts} {message}", file=sys.stderr)


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    while True:
        # make sure non-expiring keys expire
        count_expiration_set = 0
        current_keys = set()
        with r.pipeline() as pipe:
            for key in r.scan_iter(MAPPING_KEY_PATTERN):
                current_keys.add(key.removeprefix(MAPPING_KEY_PREFIX_BYTES))
                if r.ttl(key) < 0:
                    pipe.expire(key, DESIRED_EXPIRE)
                    count_expiration_set += 1
            pipe.execute()

        # remove surrogate keys that don't exist anymore
        count_surrogates_removed = 0
        with r.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(SURROGATE_KEY)
                    response = pipe.get(SURROGATE_KEY)
                    current_value = bytes(response)
                    values = [
                        str.encode(unquote(v.strip()).removeprefix("/"))
                        for v in current_value.split(b",")
                        if len(v) > 0
                    ]
                    current_count = len(values)
                    values = [v for v in values if v in current_keys]
                    new_count = len(values)
                    if new_count == current_count:
                        break
                    new_value = b",".join(values)
                    pipe.multi()
                    pipe.set(SURROGATE_KEY, new_value)
                    pipe.execute()
                    count_surrogates_removed = current_count - new_count
                    break
                except redis.WatchError:
                    log("watch error: surrogate key changed while updating")
                    continue

        if count_expiration_set > 0:
            log(
                f"set expiration for {count_expiration_set} mapping keys to {DESIRED_EXPIRE}"
            )
        if count_surrogates_removed > 0:
            log(f"removed {count_surrogates_removed} keys from the surrogate key")

        time.sleep(DESIRED_INTERVAL)


if __name__ == "__main__":
    log(f"REDIS_HOST={REDIS_HOST}")
    log(f"REDIS_PORT={REDIS_PORT}")
    log(f"REDIS_DB={REDIS_DB}")
    log(f"DESIRED_EXPIRE={DESIRED_EXPIRE}")
    log(f"DESIRED_INTERVAL={DESIRED_INTERVAL}")
    while True:
        try:
            main()
        except Exception as e:
            log(f"An error occured:\n{traceback.format_exc().strip()}")
        time.sleep(1)

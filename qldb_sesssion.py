import os
from pyqldb.driver.pooled_qldb_driver import PooledQldbDriver


def session():
    ledger_name = os.environ["LEDGER_NAME"]
    qldb_driver = PooledQldbDriver(ledger_name=ledger_name)

    return qldb_driver.get_session()
from pyqldb.driver.pooled_qldb_driver import PooledQldbDriver


def session():
    ladger_name = ledger()
    qldb_driver = PooledQldbDriver(ledger_name=ladger_name)

    return qldb_driver.get_session()

def ledger():
    with open('ledger.txt') as f:
        return f.read()
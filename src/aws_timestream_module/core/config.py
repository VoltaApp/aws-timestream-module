import os


class Settings(object):
    debug: bool = True
    read_timeout = 20
    max_pool_connections = 5000
    retries = {'max_attempts': 10}
    region = 'ap-southeast-2'


class DevSettings(Settings):
    db_name = 'dev_volta_db'
    table_name = 'energy_measure_tbl'


class StagingSettings(Settings):
    pass


class ProdSettings(Settings):
    debug: bool = False


def get_settings():
    env = os.getenv("env", None)
    if env == 'staging':
        return StagingSettings()
    elif env == 'prod':
        return ProdSettings()
    else:
        return DevSettings()


config = get_settings()

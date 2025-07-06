from pyhive import hive

def connect_to_hive(host, port, user):
    return hive.Connection(
        host=host,
        port=port,
        username=user,
        auth='NONE' # Authentication mode (matches our Docker setup)
    )
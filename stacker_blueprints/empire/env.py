

def resolve_database_url(**kwargs):
    """Returns a database url.

    Args:
        provider (string): database provider, i.e. postgres
        user (string): db user name
        password (string): password for db user
        host (string): host name of the db
        db_name (string): db to connect to

    Returns:
        string: Formatted database url

    """
    return '%(provider)s://%(user)s:%(password)s@%(host)s/%(db_name)s' % kwargs

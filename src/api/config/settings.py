import os

API_CONFIG = {
    'host': os.getenv('API_HOST', '0.0.0.0'),
    'port': int(os.getenv('API_PORT', '5000')),
    'debug': os.getenv('API_DEBUG', 'False').lower() == 'true',
    'cors_enabled': True,
}

SERVER_CONFIG = {
    'production_host': os.getenv('PRODUCTION_HOST', ''),
    'production_port': int(os.getenv('PRODUCTION_PORT', '5000')),
    'ssh_user': os.getenv('SSH_USER', ''),
    'remote_dir': os.getenv('REMOTE_DIR', ''),
    'systemd_service': os.getenv('SYSTEMD_SERVICE', 'flask-api'),
}

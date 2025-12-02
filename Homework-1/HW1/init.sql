-- Crea il database per User Manager
CREATE DATABASE IF NOT EXISTS user_db;

-- Crea il database per Data Collector
CREATE DATABASE IF NOT EXISTS data_db;

-- Assegna i permessi all'utente dell'applicazione su entrambi i DB
GRANT ALL PRIVILEGES ON user_db.* TO 'app_user'@'%';
GRANT ALL PRIVILEGES ON data_db.* TO 'app_user'@'%';

FLUSH PRIVILEGES;
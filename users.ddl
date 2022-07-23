CREATE TABLE users (
  id INT64 NOT NULL,
  name STRING(1024) NOT NULL,
  tokens INT64 NOT NULL,
  type INT64,
  color INT64,
  coins INT64,
  location INT64,
  world INT64,
  create_time INT64,
  last_login_time INT64,  
) PRIMARY KEY(id);

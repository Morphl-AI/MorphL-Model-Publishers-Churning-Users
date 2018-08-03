CREATE TABLE morphl.ga_churned_users (
  client_id text,
  day_of_data_capture date,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_churned_users_sessions (
  client_id text,
  day_of_data_capture date,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

TRUNCATE TABLE morphl.ga_churned_users;

TRUNCATE TABLE morphl.ga_churned_users_sessions;

SELECT COUNT(1) FROM morphl.ga_churned_users;

SELECT COUNT(1) FROM morphl.ga_churned_users_sessions;
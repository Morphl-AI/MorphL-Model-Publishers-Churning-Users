CREATE TABLE morphl.ga_client_ids (
  client_id text,
  day_of_data_capture date,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_desktop_device (
  client_id text,
  day_of_data_capture date,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_events (
  client_id text,
  day_of_data_capture date,
  tz_time text,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, tz_time, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC, tz_time DESC);

CREATE TABLE morphl.ga_geo_content (
  client_id text,
  day_of_data_capture date,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_goal_1_thru_10_completions (
  client_id text,
  day_of_data_capture date,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_goal_11_thru_20_completions (
  client_id text,
  day_of_data_capture date,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_mobile_device (
  client_id text,
  day_of_data_capture date,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_page_tracking (
  client_id text,
  day_of_data_capture date,
  tz_time text,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, tz_time, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC, tz_time DESC);

CREATE TABLE morphl.ga_session_ids (
  client_id text,
  day_of_data_capture date,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_site_speed (
  client_id text,
  day_of_data_capture date,
  tz_time text,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, tz_time, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC, tz_time DESC);

CREATE TABLE morphl.ga_system_data (
  client_id text,
  day_of_data_capture date,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_traffic_sources (
  client_id text,
  day_of_data_capture date,
  session_id text,
  json_data text,
  PRIMARY KEY ((client_id), day_of_data_capture, session_id)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

TRUNCATE TABLE morphl.ga_client_ids;

TRUNCATE TABLE morphl.ga_desktop_device;

TRUNCATE TABLE morphl.ga_events;

TRUNCATE TABLE morphl.ga_geo_content;

TRUNCATE TABLE morphl.ga_goal_1_thru_10_completions;

TRUNCATE TABLE morphl.ga_goal_11_thru_20_completions;

TRUNCATE TABLE morphl.ga_mobile_device;

TRUNCATE TABLE morphl.ga_page_tracking;

TRUNCATE TABLE morphl.ga_session_ids;

TRUNCATE TABLE morphl.ga_site_speed;

TRUNCATE TABLE morphl.ga_system_data;

TRUNCATE TABLE morphl.ga_traffic_sources;

SELECT COUNT(1) FROM morphl.ga_client_ids;

SELECT COUNT(1) FROM morphl.ga_desktop_device;

SELECT COUNT(1) FROM morphl.ga_events;

SELECT COUNT(1) FROM morphl.ga_geo_content;

SELECT COUNT(1) FROM morphl.ga_goal_1_thru_10_completions;

SELECT COUNT(1) FROM morphl.ga_goal_11_thru_20_completions;

SELECT COUNT(1) FROM morphl.ga_mobile_device;

SELECT COUNT(1) FROM morphl.ga_page_tracking;

SELECT COUNT(1) FROM morphl.ga_session_ids;

SELECT COUNT(1) FROM morphl.ga_site_speed;

SELECT COUNT(1) FROM morphl.ga_system_data;

SELECT COUNT(1) FROM morphl.ga_traffic_sources;

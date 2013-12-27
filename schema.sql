drop table if exists chars;

drop table if exists users;
create table users (
  id integer primary key autoincrement,
  username string not null
);

drop table if exists run_groups;
create table run_groups (
  id integer primary key autoincrement,
  user_id integer
);

drop table if exists runs;
create table runs (
  id integer primary key autoincrement,
  group_id integer not null,
  gamename string,
  charname string,
  start_dt datetime,
  end_dt datetime
);

drop table if exists custom_types;
create table custom_types (
  run_type string not null
);

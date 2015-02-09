CREATE TABLE test_table(
  id BIGINT UNSIGNED,
  msg VARCHAR(250),
  keyspace_id bigint(20) unsigned not null,
  PRIMARY KEY (id)
) ENGINE=InnoDB


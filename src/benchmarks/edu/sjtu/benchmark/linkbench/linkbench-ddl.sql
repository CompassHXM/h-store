CREATE TABLE nodetable (
  id bigint NOT NULL,
  type int NOT NULL,
  version bigint NOT NULL,
  time int NOT NULL,
  data varchar(255) NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE linktable (
  id1 bigint NOT NULL,
  id2 bigint NOT NULL,
  link_type bigint NOT NULL,
  visibility tinyint NOT NULL,
  data varchar(255) NOT NULL,
  time bigint NOT NULL,
  version int NOT NULL,
  PRIMARY KEY (id1,id2,link_type),
);
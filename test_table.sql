-- to store xin group cognito and s3 info
-- ** group
-- - name: "ur-company-name",
-- - region: "ap-southeast-1",
-- - Bucket": "email-bucket-name",
-- - IdentityPoolId: "ap-southeast-1:xxxxxxxxxx-xxxx-xxxx-xxxxxx",
-- - UserPoolId: "ap-southeast-1_xxxxxxxxx",
-- - ClientId: "xxxxxxxxxxxxxxxxxxxxxxx"

-- ** insert hash
-- insert into hash (v) values ('ClientId', 'UserPoolId')

-- ** insert group
-- insert into group (name) values ('yr-company-name')
-- insert into group_text_map (group_id, k, v) values (1, 'ClientId', 'xxxxxxxxxxxxxxxx'), (1, 'UserPoolId', 'ap-southeast-1_xxxxxxxxx'), (1, 'env', '{"":""}')

-- ** find group
-- select id from group where name = ?;
-- ** verify
-- select count(*) from group_map where host_id = ? and k in ('ClientId', 'UserPoolId') and v2 in ('xxxxxxxxx', 'xxxxxx');
-- ** get
-- select v2 from group_map where host_id = ? and k = 'env';

CREATE TABLE IF NOT EXISTS hash (
    k SMALLINT UNSIGNED AUTO_INCREMENT,
    v VARCHAR(32) UNIQUE,
	cat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	uat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (k)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS group (
    id INT UNSIGNED AUTO_INCREMENT,
    un VARCHAR(32) UNIQUE,
	cby INT UNSIGNED,
	cat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	uby INT UNSIGNED,
	uat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS group_map (
    id INT UNSIGNED AUTO_INCREMENT,
	host_id INT UNSIGNED,
	k SMALLINT UNSIGNED NOT NULL,
	v1 INT,
	v2 TEXT,
	cby INT UNSIGNED,
	cat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	uby INT UNSIGNED,
	uat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
	UNIQUE KEY(host_id, k)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS group_list (
    id INT UNSIGNED AUTO_INCREMENT,
	host_id INT UNSIGNED,
	k SMALLINT UNSIGNED NOT NULL,
	v1 INT,
	v2 TEXT,
	cby INT UNSIGNED,
	cat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	uby INT UNSIGNED,
	uat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS group_ref (
    id INT UNSIGNED AUTO_INCREMENT,
	host_id INT UNSIGNED,
	ref_id INT UNSIGNED,
	k SMALLINT UNSIGNED NOT NULL,
	v1 INT,
	v2 TEXT,
	cby INT UNSIGNED,
	cat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	uby INT UNSIGNED,
	uat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=INNODB;

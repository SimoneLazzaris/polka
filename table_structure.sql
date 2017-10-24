CREATE TABLE `badmailfrom` (
  `sender` char(80) NOT NULL,
  PRIMARY KEY (`sender`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `badmailto` (
  `rcpt` char(80) NOT NULL,
  PRIMARY KEY (`rcpt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `relay_policy` (
  `type` char(1) NOT NULL DEFAULT '',
  `item` char(80) NOT NULL DEFAULT '',
  `max` int(5) DEFAULT NULL,
  `quota` float DEFAULT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`type`,`item`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



rules_struct
CREATE DATABASE `rules_struct` /*!40100 DEFAULT CHARACTER SET utf8 */

rules_struct.orders
CREATE TABLE `orders` (
  `id` int(11) NOT NULL,
  `note` varchar(80) DEFAULT NULL,
  `secret` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

rules_struct.audit
CREATE TABLE `audit` (
  `id` int(11) NOT NULL,
  `note` varchar(80) DEFAULT NULL,
  `secret` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

rules_struct_more
CREATE DATABASE `rules_struct_more` /*!40100 DEFAULT CHARACTER SET utf8 */

rules_struct.star
CREATE TABLE `star` (
  `id` int(11) NOT NULL,
  `note` varchar(80) DEFAULT NULL,
  `secret` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

rules_struct.star_many
CREATE TABLE `star_many` (
  `id` int(11) NOT NULL,
  `note` varchar(80) DEFAULT NULL,
  `secret` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

rules_struct_more.audit
CREATE TABLE `audit` (
  `id` int(11) NOT NULL,
  `note` varchar(80) DEFAULT NULL,
  `secret` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

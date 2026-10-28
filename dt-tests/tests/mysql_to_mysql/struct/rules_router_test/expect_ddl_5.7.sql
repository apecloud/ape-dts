rules_struct_dst
CREATE DATABASE `rules_struct_dst` /*!40100 DEFAULT CHARACTER SET utf8 */

rules_struct_dst.renamed
CREATE TABLE `renamed` (
  `id` int(11) NOT NULL,
  `note` varchar(80) DEFAULT NULL,
  `secret` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_note_orders` (`note`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

rules_struct_dst.audit
CREATE TABLE `audit` (
  `id` int(11) NOT NULL,
  `note` varchar(80) DEFAULT NULL,
  `secret` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_note_audit` (`note`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

rules_struct_dst.blocked
CREATE TABLE `blocked` (
  `id` int(11) NOT NULL,
  `note` varchar(80) DEFAULT NULL,
  `secret` varchar(80) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_note_blocked` (`note`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

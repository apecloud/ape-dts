rules_struct_dst
CREATE DATABASE `rules_struct_dst` /*!40100 DEFAULT CHARACTER SET utf8mb3 */ /*!80016 DEFAULT ENCRYPTION='N' */

rules_struct_dst.renamed
CREATE TABLE `renamed` (
  `id` int NOT NULL,
  `note` varchar(80) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `secret` varchar(80) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_note_orders` (`note`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

rules_struct_dst.audit
CREATE TABLE `audit` (
  `id` int NOT NULL,
  `note` varchar(80) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `secret` varchar(80) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_note_audit` (`note`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

rules_struct_dst.blocked
CREATE TABLE `blocked` (
  `id` int NOT NULL,
  `note` varchar(80) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `secret` varchar(80) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_note_blocked` (`note`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

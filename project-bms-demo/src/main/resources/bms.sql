create database bms default character set 'utf8mb4';
use bms;

/*
Navicat MySQL Data Transfer

Source Server         : mysql-local
Source Server Version : 50623
Source Host           : 127.0.0.1:3306
Source Database       : bms

Target Server Type    : MYSQL
Target Server Version : 50623
File Encoding         : 65001

Date: 2021-01-17 20:35:11
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for member
-- ----------------------------
DROP TABLE IF EXISTS `member`;
CREATE TABLE `member` (
                          `id` varchar(32) DEFAULT NULL,
                          `NAME` varchar(20) DEFAULT NULL,
                          `nickname` varchar(20) DEFAULT NULL,
                          `phoneNum` varchar(20) DEFAULT NULL,
                          `email` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of member
-- ----------------------------
INSERT INTO `member` VALUES ('E61D65F673D54F68B0861025C69773DB', '张三', '小三', '18888888888', 'zs@163.com');

-- ----------------------------
-- Table structure for orders
-- ----------------------------
DROP TABLE IF EXISTS `orders`;
CREATE TABLE `orders` (
                          `id` varchar(32) DEFAULT NULL,
                          `orderNum` varchar(20) NOT NULL,
                          `orderTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                          `peopleCount` int(11) DEFAULT NULL,
                          `orderDesc` varchar(500) DEFAULT NULL,
                          `payType` int(11) DEFAULT NULL,
                          `orderStatus` int(11) DEFAULT NULL,
                          `productId` varchar(32) DEFAULT NULL,
                          `memberId` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of orders
-- ----------------------------
INSERT INTO `orders` VALUES ('0E7231DC797C486290E8713CA3C6ECCC', '12345', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '676C5BD1D35E429A8C2E114939C5685A', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('0E7231DC797C486290E8713CA3C6ECCC', '12345', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '676C5BD1D35E429A8C2E114939C5685A', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('5DC6A48DD4E94592AE904930EA866AFA', '54321', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '676C5BD1D35E429A8C2E114939C5685A', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('2FF351C4AC744E2092DCF08CFD314420', '67890', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '12B7ABF2A4C544568B0A7C69F36BF8B7', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('A0657832D93E4B10AE88A2D4B70B1A28', '98765', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '12B7ABF2A4C544568B0A7C69F36BF8B7', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('E4DD4C45EED84870ABA83574A801083E', '11111', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '12B7ABF2A4C544568B0A7C69F36BF8B7', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('96CC8BD43C734CC2ACBFF09501B4DD5D', '22222', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '12B7ABF2A4C544568B0A7C69F36BF8B7', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('55F9AF582D5A4DB28FB4EC3199385762', '33333', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '9F71F01CB448476DAFB309AA6DF9497F', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('CA005CF1BE3C4EF68F88ABC7DF30E976', '44444', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '9F71F01CB448476DAFB309AA6DF9497F', 'E61D65F673D54F68B0861025C69773DB');
INSERT INTO `orders` VALUES ('3081770BC3984EF092D9E99760FDABDE', '55555', '0000-00-00 00:00:00', '2', '没什么', '0', '1', '9F71F01CB448476DAFB309AA6DF9497F', 'E61D65F673D54F68B0861025C69773DB');

-- ----------------------------
-- Table structure for order_traveller
-- ----------------------------
DROP TABLE IF EXISTS `order_traveller`;
CREATE TABLE `order_traveller` (
                                   `orderId` varchar(32) DEFAULT NULL,
                                   `travellerId` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of order_traveller
-- ----------------------------
INSERT INTO `order_traveller` VALUES ('0E7231DC797C486290E8713CA3C6ECCC', '3FE27DF2A4E44A6DBC5D0FE4651D3D3E');
INSERT INTO `order_traveller` VALUES ('2FF351C4AC744E2092DCF08CFD314420', '3FE27DF2A4E44A6DBC5D0FE4651D3D3E');
INSERT INTO `order_traveller` VALUES ('3081770BC3984EF092D9E99760FDABDE', 'EE7A71FB6945483FBF91543DBE851960');
INSERT INTO `order_traveller` VALUES ('55F9AF582D5A4DB28FB4EC3199385762', 'EE7A71FB6945483FBF91543DBE851960');
INSERT INTO `order_traveller` VALUES ('5DC6A48DD4E94592AE904930EA866AFA', '3FE27DF2A4E44A6DBC5D0FE4651D3D3E');
INSERT INTO `order_traveller` VALUES ('96CC8BD43C734CC2ACBFF09501B4DD5D', 'EE7A71FB6945483FBF91543DBE851960');
INSERT INTO `order_traveller` VALUES ('A0657832D93E4B10AE88A2D4B70B1A28', '3FE27DF2A4E44A6DBC5D0FE4651D3D3E');
INSERT INTO `order_traveller` VALUES ('CA005CF1BE3C4EF68F88ABC7DF30E976', 'EE7A71FB6945483FBF91543DBE851960');
INSERT INTO `order_traveller` VALUES ('E4DD4C45EED84870ABA83574A801083E', 'EE7A71FB6945483FBF91543DBE851960');

-- ----------------------------
-- Table structure for permission
-- ----------------------------
DROP TABLE IF EXISTS `permission`;
CREATE TABLE `permission` (
                              `id` varchar(32) DEFAULT NULL,
                              `permissionName` varchar(50) DEFAULT NULL,
                              `url` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of permission
-- ----------------------------

-- ----------------------------
-- Table structure for product
-- ----------------------------
DROP TABLE IF EXISTS `product`;
CREATE TABLE `product` (
                           `productNum` varchar(64) DEFAULT NULL,
                           `productName` varchar(64) DEFAULT NULL,
                           `cityName` varchar(64) DEFAULT NULL,
                           `departureTime` varchar(64) DEFAULT NULL,
                           `productPrice` varchar(64) DEFAULT NULL,
                           `productDesc` varchar(64) DEFAULT NULL,
                           `productStatus` varchar(64) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of product
-- ----------------------------

-- ----------------------------
-- Table structure for role
-- ----------------------------
DROP TABLE IF EXISTS `role`;
CREATE TABLE `role` (
                        `id` varchar(32) DEFAULT NULL,
                        `roleName` varchar(50) DEFAULT NULL,
                        `roleDesc` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of role
-- ----------------------------
INSERT INTO `role` VALUES ('1', 'ADMIN', 'ADMIN');
INSERT INTO `role` VALUES ('2', 'USER', 'USER');

-- ----------------------------
-- Table structure for role_permission
-- ----------------------------
DROP TABLE IF EXISTS `role_permission`;
CREATE TABLE `role_permission` (
                                   `permissionId` varchar(32) DEFAULT NULL,
                                   `roleId` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of role_permission
-- ----------------------------
INSERT INTO `role_permission` VALUES ('1', '1');
INSERT INTO `role_permission` VALUES ('2', '1');
INSERT INTO `role_permission` VALUES ('3', '2');
INSERT INTO `role_permission` VALUES ('4', '2');
INSERT INTO `role_permission` VALUES ('5', '1');

-- ----------------------------
-- Table structure for syslog
-- ----------------------------
DROP TABLE IF EXISTS `syslog`;
CREATE TABLE `syslog` (
                          `id` int(11) NOT NULL AUTO_INCREMENT,
                          `visitTime` varchar(64) DEFAULT NULL,
                          `username` varchar(64) DEFAULT NULL,
                          `ip` varchar(64) DEFAULT NULL,
                          `url` varchar(64) DEFAULT NULL,
                          `executionTime` varchar(64) DEFAULT NULL,
                          `method` varchar(64) DEFAULT NULL,
                          PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of syslog
-- ----------------------------

-- ----------------------------
-- Table structure for traveller
-- ----------------------------
DROP TABLE IF EXISTS `traveller`;
CREATE TABLE `traveller` (
                             `id` varchar(32) DEFAULT NULL,
                             `NAME` varchar(20) DEFAULT NULL,
                             `sex` varchar(20) DEFAULT NULL,
                             `phoneNum` varchar(20) DEFAULT NULL,
                             `credentialsType` int(11) DEFAULT NULL,
                             `credentialsNum` varchar(50) DEFAULT NULL,
                             `travellerType` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of traveller
-- ----------------------------
INSERT INTO `traveller` VALUES ('3FE27DF2A4E44A6DBC5D0FE4651D3D3E', '张龙', '男', '13333333333', '0', '123456789009876543', '0');
INSERT INTO `traveller` VALUES ('EE7A71FB6945483FBF91543DBE851960', '张小龙', '男', '15555555555', '0', '987654321123456789', '1');

-- ----------------------------
-- Table structure for users
-- ----------------------------
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
                         `id` varchar(32) DEFAULT NULL,
                         `email` varchar(50) NOT NULL,
                         `username` varchar(50) DEFAULT NULL,
                         `PASSWORD` varchar(255) DEFAULT NULL,
                         `phoneNum` varchar(20) DEFAULT NULL,
                         `STATUS` int(11) DEFAULT NULL,
                         UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of users
-- ----------------------------
INSERT INTO `users` VALUES ('2', '8116512081@qq.com', 'pgman', '$2a$10$zK5ghF9M3dk.HI4or7nJguSVXLxHz5Q4NojLHCAKiEx2YgYpBsRse', '15988886666', '1');
INSERT INTO `users` VALUES ('1', '811651208@qq.com', 'admin', '$2a$10$CD8ayrY2x4qyzIwemogG.uWdbhSNGdmWpdEwcUPoIiLCedbFKaASe', '15998889999', '1');

-- ----------------------------
-- Table structure for users_role
-- ----------------------------
DROP TABLE IF EXISTS `users_role`;
CREATE TABLE `users_role` (
                              `userId` varchar(32) DEFAULT NULL,
                              `roleId` varchar(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of users_role
-- ----------------------------
INSERT INTO `users_role` VALUES ('1', '1');
INSERT INTO `users_role` VALUES ('1', '2');

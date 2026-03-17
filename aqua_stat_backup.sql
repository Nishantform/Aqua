-- MySQL dump 10.13  Distrib 8.0.45, for Win64 (x86_64)
--
-- Host: localhost    Database: aqua
-- ------------------------------------------------------
-- Server version	8.0.45

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `active_alerts`
--

DROP TABLE IF EXISTS `active_alerts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `active_alerts` (
  `alert_id` int NOT NULL AUTO_INCREMENT,
  `source_name` varchar(100) DEFAULT NULL,
  `capacity_percent` int DEFAULT NULL,
  `ph_level` decimal(4,2) DEFAULT NULL,
  `alert_status` varchar(20) DEFAULT NULL,
  `alert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`alert_id`)
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `active_alerts`
--

LOCK TABLES `active_alerts` WRITE;
/*!40000 ALTER TABLE `active_alerts` DISABLE KEYS */;
INSERT INTO `active_alerts` VALUES (1,'Tehri Dam',92,7.30,'STABLE','2026-03-05 18:23:16'),(2,'Bhakra Dam',60,8.10,'LOW STORAGE','2026-03-05 18:23:16'),(3,'Sardar Sarovar',95,7.60,'STABLE','2026-03-05 18:23:16'),(4,'Nagarjuna Sagar',48,7.80,'LOW STORAGE','2026-03-05 18:23:16'),(5,'Indira Sagar',88,7.20,'STABLE','2026-03-05 18:23:16'),(6,'Rihand Dam',40,7.00,'CRITICAL','2026-03-05 18:23:16'),(7,'Idukki Dam',90,7.40,'STABLE','2026-03-05 18:23:16'),(8,'Bisalpur Dam',35,7.10,'CRITICAL','2026-03-05 18:23:16'),(9,'Mithi - Mumbai',28,5.80,'CRITICAL','2026-03-05 18:02:08'),(10,'Yamuna - Agra',22,9.40,'CRITICAL','2026-03-05 16:02:08'),(11,'Musi - Hyderabad',25,9.10,'CRITICAL','2026-03-05 14:02:08'),(12,'Buckingham Canal',20,7.10,'CRITICAL','2026-03-05 17:02:08'),(13,'Sambhar Lake',30,8.40,'CRITICAL','2026-03-05 15:02:08'),(14,'Vaigai Dam',45,7.20,'WARNING','2026-03-05 13:02:08'),(15,'Upper Bari Doab Canal',40,7.30,'WARNING','2026-03-05 11:02:08'),(16,'Sirhind Canal',40,7.40,'WARNING','2026-03-05 07:02:08'),(17,'Tungabhadra Dam',54,7.80,'WARNING','2026-03-04 19:02:08'),(18,'Bhavani Sagar Dam',48,7.50,'WARNING','2026-03-03 19:02:08'),(19,'Mettur Dam',71,7.40,'STABLE','2026-03-04 19:02:08'),(20,'Bhakra Dam',85,7.20,'STABLE','2026-03-03 19:02:08'),(21,'Tehri Dam',92,7.30,'STABLE','2026-03-02 19:02:08'),(22,'Dal Lake',95,7.50,'STABLE','2026-03-01 19:02:08'),(23,'Chilika Lake',100,7.80,'STABLE','2026-02-28 19:02:08'),(24,'Periyar - Kochi',88,7.20,'STABLE','2026-02-27 19:02:08'),(25,'Cauvery - Mettur',71,7.40,'STABLE','2026-03-04 19:02:08'),(26,'Cauvery - Srirangapatna',65,7.60,'STABLE','2026-03-03 19:02:08'),(27,'Godavari - Rajahmundry',68,7.80,'STABLE','2026-03-02 19:02:08'),(28,'Krishna - Vijayawada',72,7.10,'STABLE','2026-03-01 19:02:08');
/*!40000 ALTER TABLE `active_alerts` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `groundwater_levels`
--

DROP TABLE IF EXISTS `groundwater_levels`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `groundwater_levels` (
  `district_name` varchar(50) NOT NULL,
  `avg_depth_meters` decimal(5,2) DEFAULT NULL,
  `extraction_pct` decimal(5,2) DEFAULT NULL,
  `recharge_rate_mcm` decimal(10,2) DEFAULT NULL,
  `assessment_year` year DEFAULT NULL,
  PRIMARY KEY (`district_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `groundwater_levels`
--

LOCK TABLES `groundwater_levels` WRITE;
/*!40000 ALTER TABLE `groundwater_levels` DISABLE KEYS */;
INSERT INTO `groundwater_levels` VALUES ('Agra',44.20,165.40,580.00,2026),('Amreli',18.40,61.13,1261.00,2025),('Anantapur',41.20,115.30,540.00,2026),('Aurangabad',35.60,98.40,610.00,2026),('Bhopal',24.60,78.90,850.00,2026),('Gurdaspur',22.10,115.50,300.00,2025),('Gurugram',49.50,240.00,310.00,2026),('Guwahati',5.20,25.40,2200.00,2026),('Indore',32.10,110.50,740.00,2026),('Jind',38.20,129.63,1014.00,2025),('Jodhpur',55.40,140.00,450.00,2026),('Karnal',42.10,185.20,980.00,2026),('Kochi',8.90,28.70,1850.00,2026),('Latur',22.80,55.67,688.00,2025),('Lucknow',22.40,85.60,1200.00,2026),('Ludhiana',45.50,210.80,2393.00,2025),('Madurai',31.40,105.20,620.00,2026),('Mangalore',11.20,35.80,1420.00,2026),('Mehsana',52.80,160.70,820.00,2026),('Mettur',12.50,78.40,540.00,2025),('Mysore',14.80,48.60,1350.00,2026),('Nagpur',15.20,62.40,950.00,2026),('New Delhi',42.10,159.42,321.00,2025),('Patiala',48.20,225.50,1150.00,2026),('Raipur',12.80,45.20,1800.00,2026),('Ranchi',18.90,55.20,1100.00,2026),('Salem',45.50,98.20,150.00,2025),('Shimla',8.50,30.10,1400.00,2026),('Surat',28.50,92.10,1050.00,2026),('Thiruvananthapuram',9.40,32.50,1650.00,2026),('Vellore',38.90,122.10,480.00,2026),('Visakhapatnam',7.26,45.20,432.00,2026);
/*!40000 ALTER TABLE `groundwater_levels` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `rainfall_history`
--

DROP TABLE IF EXISTS `rainfall_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `rainfall_history` (
  `id` int NOT NULL AUTO_INCREMENT,
  `district_name` varchar(50) DEFAULT NULL,
  `rainfall_cm` decimal(6,2) DEFAULT NULL,
  `record_year` year DEFAULT NULL,
  `season` enum('Monsoon','Post-Monsoon','Winter','Summer') DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=23 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `rainfall_history`
--

LOCK TABLES `rainfall_history` WRITE;
/*!40000 ALTER TABLE `rainfall_history` DISABLE KEYS */;
INSERT INTO `rainfall_history` VALUES (1,'Cherrapunji',1150.20,2025,'Monsoon'),(2,'Bikaner',15.40,2025,'Summer'),(3,'Mumbai',280.50,2025,'Monsoon'),(4,'Ludhiana',45.20,2025,'Winter'),(5,'Chennai',120.80,2025,'Post-Monsoon'),(6,'Jaipur',35.40,2025,'Summer'),(7,'Guwahati',180.20,2025,'Monsoon'),(8,'Kochi',310.40,2025,'Monsoon'),(9,'Ahmedabad',65.20,2025,'Post-Monsoon'),(10,'Patna',110.50,2025,'Monsoon'),(11,'Shimla',140.20,2026,'Winter'),(12,'Hyderabad',78.40,2025,'Monsoon'),(13,'Bhopal',95.60,2025,'Monsoon'),(14,'Thiruvananthapuram',190.20,2025,'Post-Monsoon'),(15,'Dehradun',210.50,2025,'Monsoon'),(16,'Agartala',240.20,2025,'Monsoon'),(17,'Bangalore',88.50,2025,'Post-Monsoon'),(18,'Indore',72.40,2025,'Monsoon'),(19,'Lucknow',85.20,2025,'Monsoon'),(20,'Nagpur',105.80,2025,'Monsoon'),(21,'Salem',95.00,2025,'Monsoon'),(22,'Gurdaspur',78.00,2025,'Monsoon');
/*!40000 ALTER TABLE `rainfall_history` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `regional_stats`
--

DROP TABLE IF EXISTS `regional_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `regional_stats` (
  `region_name` varchar(50) NOT NULL,
  `population_count` bigint DEFAULT NULL,
  `annual_rainfall_avg_cm` decimal(6,2) DEFAULT NULL,
  PRIMARY KEY (`region_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `regional_stats`
--

LOCK TABLES `regional_stats` WRITE;
/*!40000 ALTER TABLE `regional_stats` DISABLE KEYS */;
INSERT INTO `regional_stats` VALUES ('Ahmedabad',8000000,78.00),('Ahmedabad Urban',6200000,78.40),('Bangalore',12600000,97.00),('Bangalore Urban',11500000,97.20),('Bhopal',2200000,98.40),('Chennai',12300000,140.00),('Chennai Urban',7500000,140.20),('Gurdaspur',2298323,85.00),('Guwahati',1200000,180.40),('Hyderabad',10500000,82.00),('Hyderabad Urban',8200000,81.40),('Indore',2400000,75.40),('Jaipur',3100000,52.00),('Kanpur',3200000,95.20),('Kolkata Urban',4500000,160.80),('Lucknow',3400000,100.00),('Ludhiana',1850000,65.00),('Mumbai Suburban',9500000,240.50),('Nagpur',2500000,110.00),('North Delhi',2500000,61.20),('Patna',1700000,115.00),('Pune',8200000,72.00),('Pune Urban',5500000,72.10),('Salem',3482056,102.00),('Srinagar',1400000,70.20),('Surat',6500000,110.00),('Thane',18000000,240.00),('Visakhapatnam',2000000,105.20);
/*!40000 ALTER TABLE `regional_stats` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `water_monitoring_stations`
--

DROP TABLE IF EXISTS `water_monitoring_stations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `water_monitoring_stations` (
  `station_id` int NOT NULL AUTO_INCREMENT,
  `station_name` varchar(100) NOT NULL,
  `state_name` varchar(50) DEFAULT NULL,
  `district_name` varchar(50) DEFAULT NULL,
  `latitude` decimal(10,8) DEFAULT NULL,
  `longitude` decimal(11,8) DEFAULT NULL,
  `ph_level` decimal(4,2) DEFAULT '7.00',
  `dissolved_oxygen_mg_l` decimal(5,2) DEFAULT NULL,
  `turbidity_ntu` decimal(5,2) DEFAULT NULL,
  `status` enum('Active','Maintenance','Inactive') DEFAULT 'Active',
  PRIMARY KEY (`station_id`)
) ENGINE=InnoDB AUTO_INCREMENT=61 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `water_monitoring_stations`
--

LOCK TABLES `water_monitoring_stations` WRITE;
/*!40000 ALTER TABLE `water_monitoring_stations` DISABLE KEYS */;
INSERT INTO `water_monitoring_stations` VALUES (1,'Yamuna - Wazirabad','Delhi','North Delhi',28.71000000,77.23000000,9.20,3.50,25.40,'Active'),(2,'Ganga - Kanpur','Uttar Pradesh','Kanpur',26.44000000,80.33000000,8.80,4.20,45.00,'Active'),(3,'Cauvery - Mettur','Tamil Nadu','Salem',11.78000000,77.80000000,7.40,7.80,2.10,'Active'),(4,'Ludhiana Station 4','Punjab','Ludhiana',30.90000000,75.85000000,7.00,6.50,0.80,'Active'),(5,'Ganga - Varanasi','UP','Varanasi',25.31000000,83.00000000,8.20,5.10,12.40,'Active'),(6,'Yamuna - Agra','UP','Agra',27.18000000,78.02000000,9.40,2.20,45.80,'Active'),(7,'Sabarmati - Ahmedabad','Gujarat','Ahmedabad',23.02000000,72.57000000,8.90,3.10,32.50,'Active'),(8,'Mithi - Mumbai','Maharashtra','Mumbai',19.07000000,72.87000000,5.80,1.20,85.00,'Maintenance'),(9,'Periyar - Kochi','Kerala','Ernakulam',9.93000000,76.26000000,7.20,7.50,4.20,'Active'),(10,'Brahmaputra - Dibrugarh','Assam','Dibrugarh',27.47000000,94.91000000,7.40,8.20,18.50,'Active'),(11,'Godavari - Rajahmundry','Andhra Pradesh','East Godavari',17.00000000,81.78000000,7.80,6.40,15.20,'Active'),(12,'Krishna - Vijayawada','Andhra Pradesh','Krishna',16.50000000,80.64000000,8.10,5.80,10.40,'Active'),(13,'Narmada - Jabalpur','MP','Jabalpur',23.18000000,79.98000000,7.50,8.00,5.10,'Active'),(14,'Musi - Hyderabad','Telangana','Hyderabad',17.38000000,78.48000000,9.10,2.50,55.40,'Active'),(15,'Tapi - Surat','Gujarat','Surat',21.17000000,72.83000000,8.30,4.80,22.10,'Active'),(16,'Hooghly - Kolkata','West Bengal','Kolkata',22.57000000,88.36000000,7.90,5.20,28.60,'Active'),(17,'Cauvery - Srirangapatna','Karnataka','Mandya',12.42000000,76.69000000,7.60,6.80,6.40,'Active'),(18,'Beas - Manali','HP','Kullu',32.24000000,77.18000000,7.10,9.20,1.20,'Active'),(19,'Chambal - Kota','Rajasthan','Kota',25.21000000,75.86000000,8.00,7.20,8.50,'Active'),(20,'Mahanadi - Cuttack','Odisha','Cuttack',20.46000000,85.87000000,7.70,6.50,14.20,'Active'),(21,'Netravati - Mangalore','Karnataka','Dakshina Kannada',12.91000000,74.85000000,7.30,7.40,3.80,'Active'),(22,'Jhelum - Srinagar','J&K','Srinagar',34.08000000,74.79000000,7.50,7.00,9.50,'Active'),(23,'Damodar - Dhanbad','Jharkhand','Dhanbad',23.79000000,86.43000000,6.20,4.50,38.20,'Active'),(24,'Betwa - Vidisha','MP','Vidisha',23.52000000,77.81000000,7.80,6.20,12.10,'Active'),(25,'Yamuna - Wazirabad','Delhi','North Delhi',28.71000000,77.23000000,7.20,3.50,25.40,'Active'),(26,'Yamuna - Nizamuddin','Delhi','South Delhi',28.59000000,77.27000000,7.10,3.20,28.50,'Active'),(27,'Ganga - Kanpur','Uttar Pradesh','Kanpur',26.44000000,80.33000000,7.80,4.20,45.00,'Active'),(28,'Ganga - Varanasi','Uttar Pradesh','Varanasi',25.31000000,83.00000000,7.20,5.10,12.40,'Active'),(29,'Yamuna - Agra','Uttar Pradesh','Agra',27.18000000,78.02000000,8.40,2.20,45.80,'Maintenance'),(30,'Ganga - Haridwar','Uttarakhand','Haridwar',29.95000000,78.17000000,7.40,7.80,5.20,'Active'),(31,'Beas - Manali','Himachal Pradesh','Kullu',32.24000000,77.18000000,7.10,9.20,1.20,'Active'),(32,'Sutlej - Ropar','Punjab','Rupnagar',30.97000000,76.52000000,7.30,6.80,8.40,'Active'),(33,'Jhelum - Srinagar','Jammu and Kashmir','Srinagar',34.08000000,74.79000000,7.50,7.00,9.50,'Active'),(34,'Cauvery - Mettur','Tamil Nadu','Salem',11.78000000,77.80000000,7.40,7.80,2.10,'Active'),(35,'Cauvery - Srirangapatna','Karnataka','Mandya',12.42000000,76.69000000,7.60,6.80,6.40,'Active'),(36,'Godavari - Rajahmundry','Andhra Pradesh','East Godavari',17.00000000,81.78000000,7.80,6.40,15.20,'Active'),(37,'Krishna - Vijayawada','Andhra Pradesh','Krishna',16.50000000,80.64000000,7.10,5.80,10.40,'Active'),(38,'Musi - Hyderabad','Telangana','Hyderabad',17.38000000,78.48000000,8.10,2.50,55.40,'Maintenance'),(39,'Tungabhadra - Kurnool','Andhra Pradesh','Kurnool',15.82000000,78.03000000,7.90,5.20,18.30,'Active'),(40,'Kaveri - Trichy','Tamil Nadu','Tiruchirappalli',10.81000000,78.69000000,7.30,6.50,8.70,'Active'),(41,'Periyar - Kochi','Kerala','Ernakulam',9.93000000,76.26000000,7.20,7.50,4.20,'Active'),(42,'Vaigai - Madurai','Tamil Nadu','Madurai',9.92000000,78.12000000,7.60,5.90,12.80,'Active'),(43,'Sabarmati - Ahmedabad','Gujarat','Ahmedabad',23.02000000,72.57000000,8.90,3.10,32.50,'Active'),(44,'Mithi - Mumbai','Maharashtra','Mumbai',19.07000000,72.87000000,6.80,1.20,85.00,'Maintenance'),(45,'Tapi - Surat','Gujarat','Surat',21.17000000,72.83000000,7.30,4.80,22.10,'Active'),(46,'Narmada - Jabalpur','Madhya Pradesh','Jabalpur',23.18000000,79.98000000,7.50,8.00,5.10,'Active'),(47,'Chambal - Kota','Rajasthan','Kota',25.21000000,75.86000000,7.00,7.20,8.50,'Active'),(48,'Godavari - Nashik','Maharashtra','Nashik',20.01000000,73.78000000,7.40,6.30,14.20,'Active'),(49,'Krishna - Sangli','Maharashtra','Sangli',16.86000000,74.58000000,7.60,5.70,16.80,'Active'),(50,'Brahmaputra - Dibrugarh','Assam','Dibrugarh',27.47000000,94.91000000,7.40,8.20,18.50,'Active'),(51,'Hooghly - Kolkata','West Bengal','Kolkata',22.57000000,88.36000000,7.90,5.20,28.60,'Active'),(52,'Mahanadi - Cuttack','Odisha','Cuttack',20.46000000,85.87000000,7.70,6.50,14.20,'Active'),(53,'Damodar - Dhanbad','Jharkhand','Dhanbad',23.79000000,86.43000000,6.20,4.50,38.20,'Active'),(54,'Subarnarekha - Jamshedpur','Jharkhand','East Singhbhum',22.78000000,86.20000000,7.10,5.80,21.40,'Active'),(55,'Brahmaputra - Guwahati','Assam','Kamrup',26.18000000,91.75000000,7.30,7.40,15.60,'Active'),(56,'Barak - Silchar','Assam','Cachar',24.82000000,92.80000000,7.20,6.90,12.30,'Active'),(57,'Betwa - Vidisha','Madhya Pradesh','Vidisha',23.52000000,77.81000000,7.80,6.20,12.10,'Active'),(58,'Son - Sidhi','Madhya Pradesh','Sidhi',24.40000000,81.88000000,7.40,6.70,9.80,'Active'),(59,'Mahanadi - Raipur','Chhattisgarh','Raipur',21.25000000,81.63000000,7.50,6.10,16.20,'Active'),(60,'Indravati - Jagdalpur','Chhattisgarh','Bastar',19.08000000,82.03000000,7.60,7.20,8.90,'Active');
/*!40000 ALTER TABLE `water_monitoring_stations` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `water_sources`
--

DROP TABLE IF EXISTS `water_sources`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `water_sources` (
  `source_id` int NOT NULL AUTO_INCREMENT,
  `source_name` varchar(100) NOT NULL,
  `source_type` varchar(50) DEFAULT NULL,
  `capacity_percent` int DEFAULT '100',
  `max_capacity_mcm` decimal(15,2) DEFAULT NULL,
  `build_year` int DEFAULT NULL,
  `state` varchar(50) DEFAULT NULL,
  `district` varchar(50) DEFAULT NULL,
  `origin_state` varchar(50) DEFAULT NULL,
  `is_transboundary` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`source_id`)
) ENGINE=InnoDB AUTO_INCREMENT=82 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `water_sources`
--

LOCK TABLES `water_sources` WRITE;
/*!40000 ALTER TABLE `water_sources` DISABLE KEYS */;
INSERT INTO `water_sources` VALUES (1,'Mettur Dam','Reservoir',71,2700.00,1934,'Tamil Nadu','Salem',NULL,1),(11,'Bhakra Dam','Dam',85,9340.00,1963,'Himachal Pradesh','Bilaspur','Himachal',1),(12,'Mettur Dam','Dam',65,2700.00,1934,'Tamil Nadu','Salem','Karnataka',1),(13,'Sardar Sarovar','Dam',92,9500.00,2017,'Gujarat','Narmada','Madhya Pradesh',1),(14,'Upper Bari Doab Canal','Canal',40,150.00,1859,'Punjab','Gurdaspur','Punjab',0),(15,'Teesta Barrage','Canal',75,800.00,1998,'West Bengal','Gajoldoba','Sikkim',1),(16,'Tehri Dam','Dam',92,2600.00,2006,'Uttarakhand','Tehri','Uttarakhand',0),(17,'Hirakud Dam','Dam',75,5896.00,1957,'Odisha','Sambalpur','Chhattisgarh',1),(18,'Nagarjuna Sagar','Dam',68,9371.00,1967,'Telangana','Nalgonda','Maharashtra',1),(19,'Indira Sagar','Dam',88,12220.00,2005,'Madhya Pradesh','Khandwa','MP',0),(20,'Koyna Dam','Dam',72,2797.00,1964,'Maharashtra','Satara','Maharashtra',0),(21,'Tungabhadra Dam','Dam',54,3766.00,1953,'Karnataka','Ballari','Karnataka',1),(22,'Rihand Dam','Dam',81,10600.00,1962,'Uttar Pradesh','Sonbhadra','Chhattisgarh',1),(23,'Idukki Dam','Dam',90,1996.00,1973,'Kerala','Idukki','Kerala',0),(24,'Vaigai Dam','Dam',45,175.00,1959,'Tamil Nadu','Theni','Tamil Nadu',0),(25,'Bisalpur Dam','Dam',60,1100.00,1999,'Rajasthan','Tonk','Rajasthan',0),(26,'Farakka Barrage','Canal',85,1200.00,1975,'West Bengal','Murshidabad','Uttarakhand',1),(27,'Sirhind Canal','Canal',40,350.00,1882,'Punjab','Rupnagar','Punjab',0),(28,'Lower Jhelum Canal','Canal',65,210.00,1901,'J&K','Baramulla','J&K',1),(29,'Sarda Canal','Canal',70,450.00,1928,'Uttar Pradesh','Pilibhit','Nepal',1),(30,'Buckingham Canal','Canal',20,100.00,1806,'Andhra Pradesh','Nellore','AP',0),(31,'Chilika Lake','Lake',100,950.00,0,'Odisha','Puri','Odisha',0),(32,'Vembanad Lake','Lake',100,1200.00,0,'Kerala','Kottayam','Kerala',0),(33,'Dal Lake','Lake',95,300.00,0,'J&K','Srinagar','J&K',0),(34,'Sambhar Lake','Lake',30,150.00,0,'Rajasthan','Jaipur','Rajasthan',0),(35,'Pulicat Lake','Lake',80,400.00,0,'Andhra Pradesh','Nellore','AP',1),(36,'Mettur Dam','Dam',71,2700.00,1934,'Tamil Nadu','Salem','Karnataka',1),(37,'Bhakra Dam','Dam',85,9340.00,1963,'Himachal Pradesh','Bilaspur','Himachal Pradesh',1),(38,'Sardar Sarovar Dam','Dam',92,9500.00,2017,'Gujarat','Narmada','Madhya Pradesh',1),(39,'Tehri Dam','Dam',92,2600.00,2006,'Uttarakhand','Tehri','Uttarakhand',0),(40,'Hirakud Dam','Dam',75,5896.00,1957,'Odisha','Sambalpur','Chhattisgarh',1),(41,'Nagarjuna Sagar Dam','Dam',68,9371.00,1967,'Telangana','Nalgonda','Maharashtra',1),(42,'Indira Sagar Dam','Dam',88,12220.00,2005,'Madhya Pradesh','Khandwa','Madhya Pradesh',0),(43,'Koyna Dam','Dam',72,2797.00,1964,'Maharashtra','Satara','Maharashtra',0),(44,'Tungabhadra Dam','Dam',54,3766.00,1953,'Karnataka','Ballari','Karnataka',1),(45,'Rihand Dam','Dam',81,10600.00,1962,'Uttar Pradesh','Sonbhadra','Chhattisgarh',1),(46,'Idukki Dam','Dam',90,1996.00,1973,'Kerala','Idukki','Kerala',0),(47,'Vaigai Dam','Dam',45,175.00,1959,'Tamil Nadu','Theni','Tamil Nadu',0),(48,'Bisalpur Dam','Dam',60,1100.00,1999,'Rajasthan','Tonk','Rajasthan',0),(49,'Krishna Raja Sagara Dam','Dam',65,1245.00,1932,'Karnataka','Mandya','Karnataka',0),(50,'Almatti Dam','Dam',78,3215.00,2005,'Karnataka','Bijapur','Maharashtra',1),(51,'Srisailam Dam','Dam',72,8723.00,1981,'Andhra Pradesh','Kurnool','Telangana',1),(52,'Cheruthoni Dam','Dam',88,1745.00,1976,'Kerala','Idukki','Kerala',0),(53,'Papanasam Dam','Dam',52,325.00,1944,'Tamil Nadu','Tirunelveli','Tamil Nadu',0),(54,'Malampuzha Dam','Dam',63,455.00,1955,'Kerala','Palakkad','Kerala',0),(55,'Bhavani Sagar Dam','Dam',48,980.00,1955,'Tamil Nadu','Erode','Tamil Nadu',0),(56,'Upper Bari Doab Canal','Canal',40,150.00,1859,'Punjab','Gurdaspur','Punjab',0),(57,'Teesta Barrage Canal','Canal',75,800.00,1998,'West Bengal','Gajoldoba','Sikkim',1),(58,'Farakka Barrage Canal','Canal',85,1200.00,1975,'West Bengal','Murshidabad','Uttarakhand',1),(59,'Sirhind Canal','Canal',40,350.00,1882,'Punjab','Rupnagar','Punjab',0),(60,'Lower Jhelum Canal','Canal',65,210.00,1901,'Jammu and Kashmir','Baramulla','Jammu and Kashmir',1),(61,'Sarda Canal','Canal',70,450.00,1928,'Uttar Pradesh','Pilibhit','Nepal',1),(62,'Buckingham Canal','Canal',20,100.00,1806,'Andhra Pradesh','Nellore','Andhra Pradesh',0),(63,'Indira Gandhi Canal','Canal',82,1580.00,1987,'Rajasthan','Ganganagar','Punjab',1),(64,'Kurnool Cuddapah Canal','Canal',55,230.00,1870,'Andhra Pradesh','Kurnool','Andhra Pradesh',0),(65,'Kakinada Canal','Canal',68,180.00,1965,'Andhra Pradesh','East Godavari','Andhra Pradesh',0),(66,'Damodar Canal','Canal',72,340.00,1955,'West Bengal','Hooghly','Jharkhand',1),(67,'Dal Lake','Lake',95,300.00,0,'Jammu and Kashmir','Srinagar','Jammu and Kashmir',0),(68,'Chilika Lake','Lake',100,950.00,0,'Odisha','Puri','Odisha',0),(69,'Vembanad Lake','Lake',100,1200.00,0,'Kerala','Kottayam','Kerala',0),(70,'Sambhar Lake','Lake',30,150.00,0,'Rajasthan','Jaipur','Rajasthan',0),(71,'Pulicat Lake','Lake',80,400.00,0,'Andhra Pradesh','Nellore','Andhra Pradesh',1),(72,'Loktak Lake','Lake',85,380.00,0,'Manipur','Bishnupur','Manipur',0),(73,'Wular Lake','Lake',72,250.00,0,'Jammu and Kashmir','Bandipora','Jammu and Kashmir',0),(74,'Kolleru Lake','Lake',65,320.00,0,'Andhra Pradesh','West Godavari','Andhra Pradesh',0),(75,'Pushkar Lake','Lake',45,25.00,0,'Rajasthan','Ajmer','Rajasthan',0),(76,'Umiam Lake','Lake',90,180.00,1965,'Meghalaya','East Khasi Hills','Meghalaya',0),(77,'Govind Ballabh Pant Sagar','Reservoir',83,2650.00,1976,'Uttar Pradesh','Sonbhadra','Uttar Pradesh',0),(78,'Dharoi Reservoir','Reservoir',58,875.00,1978,'Gujarat','Mehsana','Gujarat',0),(79,'Ujjani Reservoir','Reservoir',62,1450.00,1980,'Maharashtra','Solapur','Maharashtra',0),(80,'Jayakwadi Reservoir','Reservoir',71,2050.00,1976,'Maharashtra','Aurangabad','Maharashtra',0),(81,'Sri Ram Sagar Reservoir','Reservoir',55,1560.00,1977,'Telangana','Nizamabad','Maharashtra',1);
/*!40000 ALTER TABLE `water_sources` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `water_usage_history`
--

DROP TABLE IF EXISTS `water_usage_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `water_usage_history` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `source_id` int DEFAULT NULL,
  `sector` enum('Agriculture','Industrial','Domestic') DEFAULT NULL,
  `sub_sector` varchar(50) DEFAULT NULL,
  `consumer_name` varchar(100) DEFAULT 'General',
  `consumption_mcm` decimal(12,2) DEFAULT NULL,
  `record_year` int DEFAULT NULL,
  `record_date` date DEFAULT NULL,
  `season` enum('Monsoon','Post-Monsoon','Winter','Summer') DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `source_id` (`source_id`),
  KEY `record_date` (`record_date`),
  KEY `season` (`season`),
  CONSTRAINT `water_usage_history_ibfk_1` FOREIGN KEY (`source_id`) REFERENCES `water_sources` (`source_id`)
) ENGINE=InnoDB AUTO_INCREMENT=175 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `water_usage_history`
--

LOCK TABLES `water_usage_history` WRITE;
/*!40000 ALTER TABLE `water_usage_history` DISABLE KEYS */;
INSERT INTO `water_usage_history` VALUES (4,1,'Agriculture','Rice Paddy','General',450.00,2025,NULL,'Summer'),(5,1,'Industrial','Textiles','General',120.00,2025,NULL,'Summer'),(6,1,'Domestic','Urban Supply','General',85.00,2025,NULL,'Summer'),(91,1,'Agriculture','Rice Paddy','Farmer Cooperative',450.00,2025,'2025-06-15','Summer'),(92,1,'Agriculture','Sugarcane','Sugar Mills',320.00,2025,'2025-07-10','Monsoon'),(93,1,'Industrial','Textiles','Textile Park',120.00,2025,'2025-05-20','Summer'),(94,1,'Industrial','Chemicals','Chemical Industries',85.00,2025,'2025-08-05','Monsoon'),(95,1,'Domestic','Urban Supply','Salem Municipal',85.00,2025,'2025-04-10','Summer'),(96,1,'Domestic','Rural Supply','Panchayat Union',65.00,2025,'2025-09-12','Post-Monsoon'),(97,1,'Agriculture','Rice Paddy','Farmer Cooperative',430.00,2024,'2024-06-10','Summer'),(98,1,'Industrial','Textiles','Textile Park',115.00,2024,'2024-07-15','Monsoon'),(99,11,'Agriculture','Wheat','Punjab Farmers',850.00,2025,'2025-03-15','Winter'),(100,11,'Agriculture','Rice Paddy','Haryana Farmers',780.00,2025,'2025-07-20','Monsoon'),(101,11,'Industrial','Power Generation','BBMB',450.00,2025,'2025-01-10','Winter'),(102,11,'Domestic','Urban Supply','Chandigarh MC',180.00,2025,'2025-05-25','Summer'),(103,11,'Agriculture','Cotton','Cotton Farmers',320.00,2025,'2025-08-30','Monsoon'),(104,13,'Agriculture','Cotton','Gujarat Farmers',920.00,2025,'2025-07-05','Monsoon'),(105,13,'Agriculture','Groundnut','Farmer Cooperative',450.00,2025,'2025-08-12','Monsoon'),(106,13,'Industrial','Petrochemicals','Industrial Estate',380.00,2025,'2025-04-18','Summer'),(107,13,'Domestic','Urban Supply','Ahmedabad Municipal',250.00,2025,'2025-06-22','Summer'),(108,13,'Domestic','Urban Supply','Vadodara Municipal',180.00,2025,'2025-09-05','Post-Monsoon'),(109,16,'Industrial','Power Generation','THDC India',320.00,2025,'2025-02-14','Winter'),(110,16,'Domestic','Urban Supply','Delhi Jal Board',280.00,2025,'2025-05-30','Summer'),(111,16,'Agriculture','Vegetables','Uttarakhand Farmers',120.00,2025,'2025-08-08','Monsoon'),(112,16,'Domestic','Urban Supply','Uttarakhand Jal',95.00,2025,'2025-10-15','Post-Monsoon'),(113,17,'Agriculture','Rice Paddy','Odisha Farmers',580.00,2025,'2025-07-25','Monsoon'),(114,17,'Industrial','Aluminum','NALCO',210.00,2025,'2025-03-20','Summer'),(115,17,'Domestic','Urban Supply','Sambalpur MC',75.00,2025,'2025-09-18','Post-Monsoon'),(116,17,'Agriculture','Pulses','Farmer Cooperative',165.00,2025,'2025-11-05','Winter'),(117,18,'Agriculture','Rice Paddy','Telangana Farmers',620.00,2025,'2025-07-12','Monsoon'),(118,18,'Agriculture','Cotton','Andhra Farmers',340.00,2025,'2025-08-22','Monsoon'),(119,18,'Industrial','Fertilizers','Fertilizer Plant',185.00,2025,'2025-04-05','Summer'),(120,18,'Domestic','Urban Supply','Hyderabad Metro',290.00,2025,'2025-05-15','Summer'),(121,20,'Industrial','Power Generation','Maharashtra Genco',410.00,2025,'2025-01-20','Winter'),(122,20,'Agriculture','Sugarcane','Sugar Cooperatives',380.00,2025,'2025-02-25','Winter'),(123,20,'Domestic','Urban Supply','Pune Municipal',195.00,2025,'2025-04-30','Summer'),(124,20,'Agriculture','Sugarcane','Satara Farmers',290.00,2025,'2025-08-15','Monsoon'),(125,21,'Agriculture','Rice Paddy','Karnataka Farmers',450.00,2025,'2025-07-18','Monsoon'),(126,21,'Agriculture','Cotton','Ballari Farmers',280.00,2025,'2025-08-25','Monsoon'),(127,21,'Industrial','Mining','Iron Ore Industries',120.00,2025,'2025-03-10','Summer'),(128,21,'Domestic','Urban Supply','Ballari Municipal',85.00,2025,'2025-05-05','Summer'),(129,22,'Industrial','Power Generation','NTPC',520.00,2025,'2025-01-15','Winter'),(130,22,'Agriculture','Rice Paddy','UP Farmers',380.00,2025,'2025-07-22','Monsoon'),(131,22,'Industrial','Coal Mining','Coal India',210.00,2025,'2025-04-12','Summer'),(132,22,'Domestic','Urban Supply','Sonbhadra Municipal',65.00,2025,'2025-09-20','Post-Monsoon'),(133,23,'Industrial','Power Generation','KSEB',420.00,2025,'2025-02-10','Winter'),(134,23,'Agriculture','Spices','Idukki Farmers',95.00,2025,'2025-07-28','Monsoon'),(135,23,'Domestic','Urban Supply','Kochi Metro',150.00,2025,'2025-05-18','Summer'),(136,23,'Agriculture','Tea','Tea Estates',75.00,2025,'2025-09-15','Post-Monsoon'),(137,24,'Agriculture','Rice Paddy','Madurai Farmers',85.00,2025,'2025-07-08','Monsoon'),(138,24,'Agriculture','Cotton','Theni Farmers',45.00,2025,'2025-08-20','Monsoon'),(139,24,'Domestic','Urban Supply','Madurai Municipal',32.00,2025,'2025-04-25','Summer'),(140,24,'Domestic','Rural Supply','Panchayat Union',18.00,2025,'2025-10-10','Post-Monsoon'),(141,25,'Agriculture','Wheat','Tonk Farmers',210.00,2025,'2025-03-05','Winter'),(142,25,'Agriculture','Pulses','Farmer Cooperative',85.00,2025,'2025-08-12','Monsoon'),(143,25,'Domestic','Urban Supply','Jaipur Municipal',180.00,2025,'2025-05-22','Summer'),(144,25,'Domestic','Urban Supply','Ajmer Municipal',95.00,2025,'2025-09-28','Post-Monsoon'),(145,26,'Industrial','Power Generation','NTPC Farakka',280.00,2025,'2025-01-25','Winter'),(146,26,'Agriculture','Rice Paddy','Murshidabad Farmers',320.00,2025,'2025-07-15','Monsoon'),(147,26,'Agriculture','Jute','Jute Mills',150.00,2025,'2025-08-28','Monsoon'),(148,26,'Domestic','Urban Supply','Kolkata Municipal',420.00,2025,'2025-04-08','Summer'),(149,27,'Agriculture','Wheat','Punjab Farmers',180.00,2025,'2025-03-12','Winter'),(150,27,'Agriculture','Rice Paddy','Rupnagar Farmers',150.00,2025,'2025-07-30','Monsoon'),(151,27,'Agriculture','Sugarcane','Sugar Mills',85.00,2025,'2025-02-18','Winter'),(152,27,'Domestic','Rural Supply','Village Councils',35.00,2025,'2025-09-05','Post-Monsoon'),(153,33,'Domestic','Tourism','Houseboat Owners',5.00,2025,'2025-06-10','Summer'),(154,33,'Domestic','Tourism','Shikara Operators',2.00,2025,'2025-07-15','Monsoon'),(155,33,'Domestic','Urban Supply','Srinagar Municipal',45.00,2025,'2025-05-20','Summer'),(156,33,'Agriculture','Vegetables','Floating Gardens',8.00,2025,'2025-08-05','Monsoon'),(157,31,'Agriculture','Aquaculture','Fishermen Cooperative',25.00,2025,'2025-01-10','Winter'),(158,31,'Agriculture','Prawn Farming','Prawn Farmers',18.00,2025,'2025-07-20','Monsoon'),(159,31,'Domestic','Tourism','Tour Operators',3.00,2025,'2025-10-05','Post-Monsoon'),(160,31,'Domestic','Rural Supply','Coastal Villages',12.00,2025,'2025-04-15','Summer'),(161,32,'Agriculture','Backwater Fishing','Fishermen',15.00,2025,'2025-01-20','Winter'),(162,32,'Domestic','Tourism','Houseboat Operators',8.00,2025,'2025-08-12','Monsoon'),(163,32,'Agriculture','Coconut Farming','Coconut Farmers',10.00,2025,'2025-05-25','Summer'),(164,32,'Domestic','Rural Supply','Kuttanad Villages',20.00,2025,'2025-09-18','Post-Monsoon'),(165,1,'Agriculture','Rice Paddy','Farmer Cooperative',440.00,2024,'2024-06-12','Summer'),(166,1,'Industrial','Textiles','Textile Park',110.00,2024,'2024-05-18','Summer'),(167,11,'Agriculture','Wheat','Punjab Farmers',820.00,2024,'2024-03-10','Winter'),(168,11,'Industrial','Power Generation','BBMB',430.00,2024,'2024-01-15','Winter'),(169,13,'Agriculture','Cotton','Gujarat Farmers',890.00,2024,'2024-07-08','Monsoon'),(170,13,'Domestic','Urban Supply','Ahmedabad Municipal',240.00,2024,'2024-05-20','Summer'),(171,16,'Industrial','Power Generation','THDC India',310.00,2024,'2024-02-12','Winter'),(172,17,'Agriculture','Rice Paddy','Odisha Farmers',560.00,2024,'2024-07-20','Monsoon'),(173,18,'Agriculture','Rice Paddy','Telangana Farmers',600.00,2024,'2024-07-15','Monsoon'),(174,20,'Industrial','Power Generation','Maharashtra Genco',400.00,2024,'2024-01-18','Winter');
/*!40000 ALTER TABLE `water_usage_history` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-03-17 22:05:51

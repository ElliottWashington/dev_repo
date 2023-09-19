SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

CREATE SCHEMA IF NOT EXISTS `rbandits2` DEFAULT CHARACTER SET latin1 ;
USE `rbandits2` ;

-- -----------------------------------------------------
-- Table `rbandits2`.`etf_and_indices_tickers`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`etf_and_indices_tickers` (
  `ticker` VARCHAR(10) NULL DEFAULT NULL )
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`exchopt`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`exchopt` (
  `id` CHAR(1) NOT NULL ,
  `name` VARCHAR(16) NOT NULL ,
  PRIMARY KEY (`id`) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`instreq`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`instreq` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(20) NULL DEFAULT NULL ,
  `penny` CHAR(1) NULL DEFAULT NULL ,
  `type` VARCHAR(1) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `sym_name` (`name` ASC) )
ENGINE = InnoDB
AUTO_INCREMENT = 20276
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`instropt`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`instropt` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(30) NOT NULL ,
  `underlying` VARCHAR(10) NOT NULL ,
  `expiration` DATE NULL DEFAULT NULL ,
  `strike` DECIMAL(10,2) NULL DEFAULT NULL ,
  `callput` CHAR(1) NULL DEFAULT NULL ,
  `spc` VARCHAR(10) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `sym_name` (`name` ASC) ,
  INDEX `sym_lookup` (`underlying` ASC, `expiration` ASC, `strike` ASC, `callput` ASC) )
ENGINE = InnoDB
AUTO_INCREMENT = 23503888
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`opnintopt`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`opnintopt` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `optid` BIGINT(64) UNSIGNED NOT NULL ,
  `ds` DATE NOT NULL ,
  `vol` INT(10) UNSIGNED NULL DEFAULT NULL ,
  `exch` VARCHAR(20) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `opt_id` (`optid` ASC, `ds` ASC) )
ENGINE = InnoDB
AUTO_INCREMENT = 401348862
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`opteodquote`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`opteodquote` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `optid` BIGINT(64) UNSIGNED NOT NULL ,
  `ds` DATE NOT NULL ,
  `bidsize45` INT(11) NULL DEFAULT NULL ,
  `bid45` DOUBLE NULL DEFAULT NULL ,
  `asksize45` INT(11) NULL DEFAULT NULL ,
  `ask45` DOUBLE NULL DEFAULT NULL ,
  `underbid45` DOUBLE NULL DEFAULT NULL ,
  `underask45` DOUBLE NULL DEFAULT NULL ,
  `bidsizeeod` INT(11) NULL DEFAULT NULL ,
  `bideod` DOUBLE NULL DEFAULT NULL ,
  `asksizeeod` INT(11) NULL DEFAULT NULL ,
  `askeod` DOUBLE NULL DEFAULT NULL ,
  `underbideod` FLOAT NULL DEFAULT NULL ,
  `underaskeod` DOUBLE NULL DEFAULT NULL ,
  `vwap` DOUBLE NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `optid_ds` (`optid` ASC, `ds` ASC) ,
  CONSTRAINT `opteodquote_ibfk_1`
    FOREIGN KEY (`optid` )
    REFERENCES `rbandits2`.`instropt` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`sprdsymbolexpiration`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`sprdsymbolexpiration` (
  `sprdsym` VARCHAR(255) NOT NULL ,
  `expirationone` DATE NOT NULL ,
  `expirationtwo` DATE NOT NULL ,
  PRIMARY KEY (`sprdsym`) ,
  INDEX `idx_sprdsymbolexpiration_sprdsym` (`sprdsym` ASC) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`trdindopt`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`trdindopt` (
  `id` CHAR(1) CHARACTER SET 'latin1' COLLATE 'latin1_bin' NOT NULL ,
  `name` VARCHAR(16) CHARACTER SET 'latin1' NOT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `idx_name` (`name` ASC) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1
COLLATE = latin1_general_cs;


-- -----------------------------------------------------
-- Table `rbandits2`.`trdopt`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`trdopt` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `ts` DATETIME NOT NULL ,
  `optid` BIGINT(64) UNSIGNED NOT NULL ,
  `price` DECIMAL(10,2) NULL DEFAULT NULL ,
  `size` INT(11) NOT NULL ,
  `exchcode` CHAR(1) NULL DEFAULT NULL ,
  `ind` CHAR(1) NULL DEFAULT NULL ,
  `bidprcbbo` DECIMAL(10,2) NULL DEFAULT NULL ,
  `bidszbbo` INT(11) NULL DEFAULT NULL ,
  `bidexchbbo` VARCHAR(20) NULL DEFAULT NULL ,
  `askprcbbo` DECIMAL(10,2) NULL DEFAULT NULL ,
  `askszbbo` INT(11) NULL DEFAULT NULL ,
  `askexchbbo` VARCHAR(20) NULL DEFAULT NULL ,
  `dayVol` INT(11) NULL DEFAULT NULL ,
  `openInt` INT(11) NULL DEFAULT NULL ,
  `sigma` DOUBLE NULL DEFAULT NULL ,
  `delta` DOUBLE NULL DEFAULT NULL ,
  `gamma` DOUBLE NULL DEFAULT NULL ,
  `theta` DOUBLE NULL DEFAULT NULL ,
  `vega` DOUBLE NULL DEFAULT NULL ,
  `rho` DOUBLE NULL DEFAULT NULL ,
  `highD` DECIMAL(10,2) NULL DEFAULT NULL ,
  `lowD` DECIMAL(10,2) NULL DEFAULT NULL ,
  `undbidsz` INT(11) NULL DEFAULT NULL ,
  `undbidprc` DECIMAL(10,2) NULL DEFAULT NULL ,
  `undaskprc` DECIMAL(10,2) NULL DEFAULT NULL ,
  `undasksz` INT(11) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `ts` (`ts` ASC) ,
  INDEX `optid` (`optid` ASC) ,
  CONSTRAINT `trdopt_ibfk_1`
    FOREIGN KEY (`optid` )
    REFERENCES `rbandits2`.`instropt` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
AUTO_INCREMENT = 1004735786
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`trdsprd`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`trdsprd` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `ts` DATETIME NOT NULL ,
  `sprdsym` VARCHAR(200) NOT NULL ,
  `underly` VARCHAR(10) NULL DEFAULT NULL ,
  `price` DECIMAL(10,2) NULL DEFAULT NULL ,
  `size` INT(11) NOT NULL ,
  `exchcode` CHAR(1) NULL DEFAULT NULL ,
  `impbidprc` DECIMAL(10,2) NULL DEFAULT NULL ,
  `impbidsz` INT(11) NULL DEFAULT NULL ,
  `impbidexch` VARCHAR(20) NULL DEFAULT NULL ,
  `impaskprc` DECIMAL(10,2) NULL DEFAULT NULL ,
  `impasksz` INT(11) NULL DEFAULT NULL ,
  `impaskexch` VARCHAR(20) NULL DEFAULT NULL ,
  `legcnt` TINYINT(2) UNSIGNED NULL DEFAULT NULL ,
  `xdays` SMALLINT(4) UNSIGNED NULL DEFAULT NULL ,
  `edge` INT(4) NULL DEFAULT NULL ,
  `sprdtype` TINYINT(2) UNSIGNED NULL DEFAULT NULL ,
  `ratiocnt` TINYINT(2) UNSIGNED NULL DEFAULT NULL ,
  `bidexchcnt` TINYINT(1) UNSIGNED NULL DEFAULT NULL ,
  `askexchcnt` TINYINT(1) UNSIGNED NULL DEFAULT NULL ,
  `trdcnt` INT(11) UNSIGNED NULL DEFAULT NULL ,
  `dayvolume` INT(11) UNSIGNED NULL DEFAULT NULL ,
  `impsprd` DECIMAL(10,2) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `ts` (`ts` ASC) )
ENGINE = InnoDB
AUTO_INCREMENT = 302936176
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`trdstatseq`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`trdstatseq` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `eqid` BIGINT(64) UNSIGNED NOT NULL ,
  `ds` DATE NOT NULL ,
  `open` DECIMAL(10,2) NULL DEFAULT NULL ,
  `high` DECIMAL(10,2) NULL DEFAULT NULL ,
  `low` DECIMAL(10,2) NULL DEFAULT NULL ,
  `last` DECIMAL(10,2) NULL DEFAULT NULL ,
  `count` INT(11) NULL DEFAULT NULL ,
  `volume` INT(11) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `eqid` (`eqid` ASC, `ds` ASC) ,
  CONSTRAINT `trdstatseq_ibfk_1`
    FOREIGN KEY (`eqid` )
    REFERENCES `rbandits2`.`instreq` (`id` ))
ENGINE = InnoDB
AUTO_INCREMENT = 5849088285
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`trdstatsopt`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`trdstatsopt` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `optid` BIGINT(64) UNSIGNED NOT NULL ,
  `ds` DATE NOT NULL ,
  `open` DECIMAL(10,2) NULL DEFAULT NULL ,
  `high` DECIMAL(10,2) NULL DEFAULT NULL ,
  `low` DECIMAL(10,2) NULL DEFAULT NULL ,
  `last` DECIMAL(10,2) NULL DEFAULT NULL ,
  `count` INT(11) NULL DEFAULT NULL ,
  `volume` INT(11) NULL DEFAULT NULL ,
  `highVol` DOUBLE NULL DEFAULT NULL ,
  `lowVol` DOUBLE NULL DEFAULT NULL ,
  `highDelta` DOUBLE NULL DEFAULT NULL ,
  `lowDelta` DOUBLE NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `optid_ds` (`optid` ASC, `ds` ASC) ,
  CONSTRAINT `trdstatsopt_ibfk_1`
    FOREIGN KEY (`optid` )
    REFERENCES `rbandits2`.`instropt` (`id` )
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
AUTO_INCREMENT = 4810127450
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `rbandits2`.`trdstatssprd`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `rbandits2`.`trdstatssprd` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `sprdsym` VARCHAR(200) NOT NULL ,
  `ds` DATE NOT NULL ,
  `open` DECIMAL(10,2) NULL DEFAULT NULL ,
  `high` DECIMAL(10,2) NULL DEFAULT NULL ,
  `low` DECIMAL(10,2) NULL DEFAULT NULL ,
  `last` DECIMAL(10,2) NULL DEFAULT NULL ,
  `count` INT(11) NULL DEFAULT NULL ,
  `volume` INT(11) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `uniq1` (`sprdsym` ASC, `ds` ASC) )
ENGINE = InnoDB
AUTO_INCREMENT = 302936162
DEFAULT CHARACTER SET = latin1;

USE `rbandits2` ;

-- -----------------------------------------------------
-- function hello_world
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`RBadmin`@`172.30.68.%` FUNCTION `hello_world`() RETURNS text CHARSET latin1
    COMMENT 'Hello World'
BEGIN
  RETURN 'Hello World';
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure insertopnint
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `insertopnint`(
  IN _name VARCHAR(20),
  IN _ul VARCHAR(10),
  IN _spc VARCHAR(10),
  IN _expiration DATE,
  IN _strike DECIMAL(10,2),
  IN _callput CHAR(1),
  IN _ds DATE,
  IN _vol INT unsigned,
  IN _exch VARCHAR(20)
)
BEGIN
  DECLARE oid BIGINT(64) unsigned;
  SELECT id FROM instropt WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(NULL,_name,_ul,_expiration,_strike,_callput,_spc);
    SET oid = LAST_INSERT_ID();
  END IF;
  INSERT INTO opnintopt VALUES(NULL,oid,_ds,_vol,_exch)
  ON DUPLICATE KEY UPDATE
  vol = _vol,
  exch = _exch;
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure insertopteodquote
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `insertopteodquote`(
  IN _name VARCHAR(20),
  IN _ul VARCHAR(10),
  IN _spc VARCHAR(10),
  IN _expiration DATE,
  IN _strike DECIMAL(10,2),
  IN _callput CHAR(1),  
  IN _ts DATETIME(6),  
  IN _bidsize45 INT,
  IN _bid45 double,
  IN _asksize45 INT,
  IN _ask45 double,
  IN _underbid45 double,
  IN _underask45 double,
  IN _bidsizeeod INT,
  IN _bideod double,
  IN _asksizeeod INT,
  IN _askeod double,
  IN _underbideod double,
  IN _underaskeod double,
  IN _vwap double  
)
BEGIN
  DECLARE oid BIGINT(64) unsigned;
  DECLARE _ds DATE;  
  SELECT id FROM instropt WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(NULL,_name,_ul,_expiration,_strike,_callput,_spc);
    SET oid = LAST_INSERT_ID();
  END IF; 
  SET _ds = DATE(_ts);  
  INSERT INTO trdstatsopt VALUES(NULL,oid,_ds,_bidsize45,_bid45,_asksize45,_ask45,_underbid45,_underask45,_bidsizeeod,_bideod,_asksizeeod,_askeod,_underbideod,_underaskeod,_vwap);  
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure insertoptgrk
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `insertoptgrk`(
  IN _name VARCHAR(20),
  IN _ul VARCHAR(10),
  IN _spc VARCHAR(10),
  IN _expiration DATE,
  IN _strike DECIMAL(10,2),
  IN _callput CHAR(1),
  IN _ds DATE,
  IN _sigma DECIMAL(10,4),
  IN _delta DECIMAL(10,4)
)
BEGIN
  DECLARE oid BIGINT(64) unsigned;
  SELECT id FROM instropt WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(NULL,_name,_ul,_expiration,_strike,_callput,_spc);
    SET oid = LAST_INSERT_ID();
  END IF;
  INSERT INTO volstatsopt VALUES(NULL,oid,_ds,_sigma,_sigma,_sigma,_sigma)
  ON DUPLICATE KEY UPDATE
  high = IF(_sigma > high,_sigma,high), 
  low = IF(_sigma < low,_sigma,low),
  last = _sigma;
  INSERT INTO deltastatsopt VALUES(NULL,oid,_ds,_delta,_delta,_delta,_delta)
  ON DUPLICATE KEY UPDATE
  high = IF(_delta > high,_delta,high),
  low = IF(_delta < low,_delta,low),
  last = _delta;
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure insertoptvol
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `insertoptvol`(
  IN _name VARCHAR(20),
  IN _ul VARCHAR(10),
  IN _expiration DATE,
  IN _strike DECIMAL(10,2),
  IN _callput CHAR(1),
  IN _ds DATE,
  IN _vol DECIMAL(10,2)
)
BEGIN
  DECLARE oid BIGINT(64) unsigned;
  SELECT id FROM instropt WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(NULL,_name,_ul,_expiration,_strike,_callput);
    SET oid = LAST_INSERT_ID();
  END IF;
  INSERT INTO volstatsopt VALUES(NULL,oid,_ds,_vol,_vol,_vol,_vol)
  ON DUPLICATE KEY UPDATE
  high = IF(_vol > high,_vol,high), 
  low = IF(_vol < low,_vol,low),
  last = _vol;
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure inserttrdeq
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `inserttrdeq`(
  IN _name VARCHAR(20),
  IN _ts DATETIME(6),
  IN _price DECIMAL(10,2),
  IN _size INT,
  IN _exchcode CHAR(1),
  IN _ind VARCHAR(10),
  IN _bidprcbbo DECIMAL(10,2),
  IN _bidszbbo INT,
  IN _bidexchbbo VARCHAR(20),
  IN _askprcbbo DECIMAL(10,2),
  IN _askszbbo INT,
  IN _askexchbbo VARCHAR(20)
  
)
BEGIN
  DECLARE oid BIGINT(64) unsigned;
  DECLARE _ds DATE;
  SELECT id FROM instreq WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instreq VALUES(NULL,_name,'N', 'E');
    SET oid = LAST_INSERT_ID();
  END IF;
  SET _ds = DATE(_ts);
  INSERT INTO trdstatseq VALUES(NULL,oid,_ds,_price,_price,_price,_price,1,_size)
  ON DUPLICATE KEY UPDATE
  high = IF(_price > high,_price,high), 
  low = IF(_price < low,_price,low),
  last = _price,
  count = count + 1,
  volume = volume + _size;
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure inserttrdopt
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `inserttrdopt`(
  IN _name VARCHAR(20),
  IN _ul VARCHAR(10),
  IN _spc VARCHAR(10),
  IN _expiration DATE,
  IN _strike DECIMAL(10,2),
  IN _callput CHAR(1),
  IN _ts DATETIME(6),
  IN _price DECIMAL(10,2),
  IN _size INT,
  IN _exchcode CHAR(1),
  IN _ind CHAR(1),
  IN _bidprcbbo DECIMAL(10,2),
  IN _bidszbbo INT,
  IN _bidexchbbo VARCHAR(20),
  IN _askprcbbo DECIMAL(10,2),
  IN _askszbbo INT,
  IN _askexchbbo VARCHAR(20),
  IN _dayVol INT,
  IN _openInt INT,
  IN _sigma double,
  IN _delta double,
  IN _gamma double,
  IN _theta double,
  IN _vega double,
  IN _rho double,
  IN _highD DECIMAL(10,2),
  IN _lowD DECIMAL(10,2)
)
BEGIN
  DECLARE oid BIGINT(64) unsigned;
  DECLARE _ds DATE;
  
  SELECT id FROM instropt WHERE name = _name INTO oid;
  
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(NULL,_name,_ul,_expiration,_strike,_callput,_spc);
    SET oid = LAST_INSERT_ID();
  END IF;
  
  INSERT INTO trdopt VALUES(NULL,_ts,oid,_price,_size,_exchcode,_ind,_bidprcbbo,_bidszbbo,_bidexchbbo,_askprcbbo,_askszbbo,_askexchbbo,_dayVol,_openInt,_sigma,_delta,_gamma,_theta,_vega,_rho,_highD,_lowD);
   
  IF(_ind = 'A' OR _ind= 'C' OR _ind='E' OR _ind='G' OR _ind='O') THEN
  SET _ds = DATE(_ts);
  INSERT INTO trdstatsopt VALUES(NULL,oid,_ds,_price,_price,_price,_price,1,_size,_sigma,_sigma,_delta,_delta)
  ON DUPLICATE KEY UPDATE  
  high = high, 
  low = low,
  last = _price,
  count = count - 1,
  volume = volume - _size,
  highVol = highVol,
  lowVol = lowVol,
  highDelta = highDelta,
  lowDelta = lowDelta;
  else
  SET _ds = DATE(_ts);
  INSERT INTO trdstatsopt VALUES(NULL,oid,_ds,_price,_price,_price,_price,1,_size,_sigma,_sigma,_delta,_delta)
  ON DUPLICATE KEY UPDATE
  high = IF(_price > high,_price,high), 
  low = IF(_price < low,_price,low),
  last = _price,
  count = count + 1,
  volume = volume + _size,  
  highVol = IF(_sigma > highVol,_sigma,highVol),
  highVol = IF(highVol = '0', '-888888', highVol),
  lowVol = IF(_sigma < lowVol, _sigma, lowVol),
  lowVol = IF(lowVol = '0', '888888', lowVol),
  highDelta = IF(_delta > highDelta,_delta,highDelta),
  highDelta = IF(highDelta = '0', '-888888', highDelta),
  lowDelta = IF(_delta < lowDelta, _delta, lowDelta),
  lowDelta = IF(lowDelta = '0', '888888', lowDelta);
  END IF;
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure inserttrdopt2
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `inserttrdopt2`(
  IN _name VARCHAR(20),
  IN _ul VARCHAR(10),
  IN _spc VARCHAR(10),
  IN _expiration DATE,
  IN _strike DECIMAL(10,2),
  IN _callput CHAR(1),
  IN _ts DATETIME(6),
  IN _price DECIMAL(10,2),
  IN _size INT,
  IN _exchcode CHAR(1),
  IN _ind CHAR(1),
  IN _bidprcbbo DECIMAL(10,2),
  IN _bidszbbo INT,
  IN _bidexchbbo VARCHAR(20),
  IN _askprcbbo DECIMAL(10,2),
  IN _askszbbo INT,
  IN _askexchbbo VARCHAR(20),
  IN _dayVol INT,
  IN _openInt INT,
  IN _sigma double,
  IN _delta double,
  IN _gamma double,
  IN _theta double,
  IN _vega double,
  IN _rho double,
  IN _highD DECIMAL(10,2),
  IN _lowD DECIMAL(10,2),
  IN _undbidsz INT,
  IN _undbidprc DECIMAL(10,2),
  IN _undaskprc DECIMAL(10,2),
  IN _undasksz INT
)
BEGIN
  DECLARE oid BIGINT(64) unsigned;
  DECLARE _ds DATE;
  
  SELECT id FROM instropt WHERE name = _name INTO oid;
  
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(NULL,_name,_ul,_expiration,_strike,_callput,_spc);
    SET oid = LAST_INSERT_ID();
  END IF;
  
  INSERT INTO trdopt VALUES(NULL,_ts,oid,_price,_size,_exchcode,_ind,_bidprcbbo,_bidszbbo,_bidexchbbo,_askprcbbo,_askszbbo,_askexchbbo,_dayVol,_openInt,_sigma,_delta,_gamma,_theta,_vega,_rho,_highD,_lowD,_undbidsz,_undbidprc,_undaskprc,_undasksz);
  
  IF(_ind = 'A' OR _ind= 'C' OR _ind='E' OR _ind='G' OR _ind='O') THEN
  SET _ds = DATE(_ts);
  INSERT INTO trdstatsopt VALUES(NULL,oid,_ds,_price,_price,_price,_price,1,_size,_sigma,_sigma,_delta,_delta)
  ON DUPLICATE KEY UPDATE  
  high = high, 
  low = low,
  last = _price,
  count = count - 1,
  volume = volume - _size,
  highVol = highVol,
  lowVol = lowVol,
  highDelta = highDelta,
  lowDelta = lowDelta;
  else
  SET _ds = DATE(_ts);
  INSERT INTO trdstatsopt VALUES(NULL,oid,_ds,_price,_price,_price,_price,1,_size,_sigma,_sigma,_delta,_delta)
  ON DUPLICATE KEY UPDATE
  high = IF(_price > high,_price,high), 
  low = IF(_price < low,_price,low),
  last = _price,
  count = count + 1,
  volume = volume + _size,  
  highVol = IF(_sigma > highVol,_sigma,highVol),
  highVol = IF(highVol = '0', '-888888', highVol),
  lowVol = IF(_sigma < lowVol, _sigma, lowVol),
  lowVol = IF(lowVol = '0', '888888', lowVol),
  highDelta = IF(_delta > highDelta,_delta,highDelta),
  highDelta = IF(highDelta = '0', '-888888', highDelta),
  lowDelta = IF(_delta < lowDelta, _delta, lowDelta),
  lowDelta = IF(lowDelta = '0', '888888', lowDelta);
  END IF;
  
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure inserttrdsprd
-- -----------------------------------------------------

DELIMITER $$
USE `rbandits2`$$
CREATE DEFINER=`RBadmin`@`localhost` PROCEDURE `inserttrdsprd`(
  IN _sprdsym VARCHAR(200),
  IN _ul VARCHAR(10),  
  IN _ts DATETIME(6),
  IN _price DECIMAL(10,2),
  IN _size INT,
  IN _exchcode CHAR(1),  
  IN _impbidprc DECIMAL(10,2),
  IN _impbidsz INT,
  IN _impbidexch VARCHAR(20),
  IN _impaskprc DECIMAL(10,2),
  IN _impasksz INT,
  IN _impaskexch VARCHAR(20),
  IN _legcnt tinyint(2),
  IN _xdays SMALLINT(4),
  IN _edge INT(4),
  IN _sprdtype TINYINT(2),
  IN _ratiocnt TINYINT(2),
  IN _bidexchcnt TINYINT(1),
  IN _askexchcnt TINYINT(1),
  IN _trdcnt INT(11),
  IN _dayvolume INT(11),
  IN _impsprd DECIMAL(10,2)
)
BEGIN 
  DECLARE _ds DATE;    
  INSERT INTO trdsprd VALUES(NULL,_ts,_sprdsym,_ul,_price,_size,_exchcode,_impbidprc,_impbidsz,_impbidexch,_impaskprc,_impasksz,_impaskexch,_legcnt,_xdays,_edge,_sprdtype,_ratiocnt,_bidexchcnt,_askexchcnt,_trdcnt,_dayvolume,_impsprd);
  SET _ds = DATE(_ts);
  INSERT INTO trdstatssprd VALUES(NULL,_sprdsym,_ds,_price,_price,_price,_price,1,_size)
  ON DUPLICATE KEY UPDATE
  high = IF(_price > high,_price,high), 
  low = IF(_price < low,_price,low),
  last = _price,
  count = count + 1,
  volume = volume + _size;
END$$

DELIMITER ;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

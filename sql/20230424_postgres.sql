CREATE SCHEMA IF NOT EXISTS rbandits2;

-- Table rbandits2.etf_and_indices_tickers
CREATE TABLE IF NOT EXISTS rbandits2.etf_and_indices_tickers (
  ticker varchar(10) NULL DEFAULT NULL
);

-- Table rbandits2.exchopt
CREATE TABLE IF NOT EXISTS rbandits2.exchopt (
  id char(1) NOT NULL,
  name varchar(16) NOT NULL,
  PRIMARY KEY (id)
);

-- Table rbandits2.instreq
CREATE TABLE IF NOT EXISTS rbandits2.instreq (
  id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
  name varchar(20) NULL DEFAULT NULL,
  penny char(1) NULL DEFAULT NULL,
  type varchar(1) NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE (name)
);

-- Table rbandits2.instropt
CREATE TABLE IF NOT EXISTS rbandits2.instropt (
  id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
  name varchar(30) NOT NULL,
  underlying varchar(10) NOT NULL,
  expiration date NULL DEFAULT NULL,
  strike decimal(10,2) NULL DEFAULT NULL,
  callput char(1) NULL DEFAULT NULL,
  spc varchar(10) NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE (name),
  CONSTRAINT fk_instropt_instreq
    FOREIGN KEY (underlying)
    REFERENCES rbandits2.instreq (name)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

-- Table rbandits2.opnintopt
CREATE TABLE IF NOT EXISTS rbandits2.opnintopt (
  id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
  optid bigint NOT NULL,
  ds date NOT NULL,
  vol integer NULL DEFAULT NULL,
  exch varchar(20) NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE (optid, ds),
  CONSTRAINT fk_opnintopt_instropt
    FOREIGN KEY (optid)
    REFERENCES rbandits2.instropt (id)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

-- -----------------------------------------------------
-- Table `rbandits2`.`opnintopt`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `rbandits2`.`opnintopt` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `optid` BIGINT(64) UNSIGNED NOT NULL ,
  `ds` DATE NOT NULL ,
  `vol` INT(10) UNSIGNED NULL DEFAULT NULL ,
  `exch` VARCHAR(20) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `opt_id` (`optid` ASC, `ds` ASC)
);

-- -----------------------------------------------------
-- Table `rbandits2`.`opteodquote`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `rbandits2`.`opteodquote` (
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
  UNIQUE INDEX `optid_ds` (`optid` ASC, `ds` ASC),
  CONSTRAINT `opteodquote_ibfk_1` FOREIGN KEY (`optid`)
    REFERENCES `rbandits2`.`instropt` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

-- -----------------------------------------------------
-- Table `rbandits2`.`sprdsymbolexpiration`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `rbandits2`.`sprdsymbolexpiration` (
  `sprdsym` VARCHAR(255) NOT NULL ,
  `expirationone` DATE NOT NULL ,
  `expirationtwo` DATE NOT NULL ,
  PRIMARY KEY (`sprdsym`) ,
  INDEX `idx_sprdsymbolexpiration_sprdsym` (`sprdsym` ASC)
);

-- -----------------------------------------------------
-- Table `rbandits2`.`trdindopt`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `rbandits2`.`trdindopt` (
  `id` CHAR(1) NOT NULL ,
  `name` VARCHAR(16) NOT NULL ,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `idx_name` (`name` ASC)
);

-- -----------------------------------------------------
-- Table `rbandits2`.`trdopt`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `rbandits2`.`trdopt` (
  `id` BIGINT(64) UNSIGNED NOT NULL AUTO_INCREMENT ,
  `ts` TIMESTAMP NOT NULL ,
  `optid` BIGINT(64) UNSIGNED NOT NULL ,
  `price` DECIMAL(10,2) NULL DEFAULT NULL ,
  `size` INT(11) NOT NULL ,
  `exchcode` CHAR(1) NULL DEFAULT NULL ,
  `ind` CHAR(1) NULL DEFAULT NULL ,
  `bidprcbbo` DECIMAL(10,2) NULL DEFAULT NULL ,
  `bidszbbo` INT(11) NULL DEFAULT NULL ,
 
-- -----------------------------------------------------
-- Table `rbandits2`.`trdopt`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `rbandits2`.`trdopt` (
  `id` BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  `ts` TIMESTAMP NOT NULL,
  `optid` BIGINT NOT NULL,
  `price` NUMERIC(10,2),
  `size` INTEGER NOT NULL,
  `exchcode` CHAR(1),
  `ind` CHAR(1),
  `bidprcbbo` NUMERIC(10,2),
  `bidszbbo` INTEGER,
  `bidexchbbo` VARCHAR(20),
  `askprcbbo` NUMERIC(10,2),
  `askszbbo` INTEGER,
  `askexchbbo` VARCHAR(20),
  `dayVol` INTEGER,
  `openInt` INTEGER,
  `sigma` DOUBLE PRECISION,
  `delta` DOUBLE PRECISION,
  `gamma` DOUBLE PRECISION,
  `theta` DOUBLE PRECISION,
  `vega` DOUBLE PRECISION,
  `rho` DOUBLE PRECISION,
  `highD` NUMERIC(10,2),
  `lowD` NUMERIC(10,2),
  `undbidsz` INTEGER,
  `undbidprc` NUMERIC(10,2),
  `undaskprc` NUMERIC(10,2),
  `undasksz` INTEGER,
  PRIMARY KEY (`id`),
  CONSTRAINT `trdopt_optid_fk`
    FOREIGN KEY (`optid`)
    REFERENCES `rbandits2`.`instropt` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX `trdopt_ts_idx` ON `rbandits2`.`trdopt` USING BTREE (`ts`);
CREATE INDEX `trdopt_optid_idx` ON `rbandits2`.`trdopt` USING BTREE (`optid`);

SET search_path = rbandits2;

-- -----------------------------------------------------
-- function hello_world
-- -----------------------------------------------------

CREATE OR REPLACE FUNCTION hello_world()
RETURNS text AS $$
BEGIN
  RETURN 'Hello World';
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------
-- procedure insertopnint
-- -----------------------------------------------------

CREATE OR REPLACE PROCEDURE insertopnint(
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
LANGUAGE plpgsql AS $$
DECLARE 
    oid BIGINT unsigned;
BEGIN
  SELECT id FROM instropt WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(DEFAULT,_name,_ul,_expiration,_strike,_callput,_spc) RETURNING id INTO oid;
  END IF;
  INSERT INTO opnintopt VALUES(DEFAULT,oid,_ds,_vol,_exch)
  ON CONFLICT (instropt_id, opnintopt_date) DO UPDATE
  SET vol = EXCLUDED.vol,
      exch = EXCLUDED.exch;
END;
$$;

-- -----------------------------------------------------
-- procedure insertopteodquote
-- -----------------------------------------------------

CREATE OR REPLACE PROCEDURE insertopteodquote(
  IN _name VARCHAR(20),
  IN _ul VARCHAR(10),
  IN _spc VARCHAR(10),
  IN _expiration DATE,
  IN _strike DECIMAL(10,2),
  IN _callput CHAR(1),
  IN _ts TIMESTAMP(6),
  IN _bidsize45 INT,
  IN _bid45 double precision,
  IN _asksize45 INT,
  IN _ask45 double precision,
  IN _underbid45 double precision,
  IN _underask45 double precision,
  IN _bidsizeeod INT,
  IN _bideod double precision,
  IN _asksizeeod INT,
  IN _askeod double precision,
  IN _underbideod double precision,
  IN _underaskeod double precision,
  IN _vwap double precision
)
LANGUAGE plpgsql
AS $$
DECLARE
  oid BIGINT;
  _ds DATE;
BEGIN
  SELECT id FROM instropt WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(DEFAULT, _name, _ul, _expiration, _strike, _callput, _spc);
    oid := currval(pg_get_serial_sequence('instropt', 'id'));
  END IF;
  _ds := DATE(_ts);
  INSERT INTO trdstatsopt VALUES(DEFAULT, oid, _ds, _bidsize45, _bid45, _asksize45, _ask45, _underbid45, _underask45, _bidsizeeod, _bideod, _asksizeeod, _askeod, _underbideod, _underaskeod, _vwap);
END;
$$;

-- -----------------------------------------------------
-- procedure insertoptgrk
-- -----------------------------------------------------

CREATE OR REPLACE PROCEDURE insertoptgrk(
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
LANGUAGE plpgsql
AS $$
DECLARE
  oid BIGINT;
BEGIN
  SELECT id FROM instropt WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(DEFAULT, _name, _ul, _expiration, _strike, _callput, _spc);
    oid := currval(pg_get_serial_sequence('instropt', 'id'));
  END IF;
  INSERT INTO volstatsopt VALUES(DEFAULT, oid, _ds, _sigma, _sigma, _sigma, _sigma)
  ON CONFLICT (instrument_id, date) DO UPDATE
  SET high = GREATEST(EXCLUDED.high, volstatsopt.high),
      low = LEAST(EXCLUDED.low, volstatsopt.low),
      last = EXCLUDED.last;
  INSERT INTO deltastatsopt VALUES(DEFAULT, oid, _ds, _delta, _delta, _delta, _delta)
  ON CONFLICT (instrument_id, date) DO UPDATE
  SET high = GREATEST(EXCLUDED.high, deltastatsopt.high),
      low = LEAST(EXCLUDED.low, deltastatsopt.low),
      last = EXCLUDED.last;
END;
$$;

-- -----------------------------------------------------
-- procedure insertoptvol
-- -----------------------------------------------------

CREATE OR REPLACE PROCEDURE insertoptvol(
  _name VARCHAR(20),
  _ul VARCHAR(10),
  _expiration DATE,
  _strike DECIMAL(10,2),
  _callput CHAR(1),
  _ds DATE,
  _vol DECIMAL(10,2)
)
LANGUAGE plpgsql
AS $$
DECLARE oid BIGINT;
BEGIN
  SELECT id FROM instropt WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instropt VALUES(DEFAULT, _name, _ul, _expiration, _strike, _callput);
    oid := currval(pg_get_serial_sequence('instropt', 'id'));
  END IF;
  INSERT INTO volstatsopt VALUES(DEFAULT, oid, _ds, _vol, _vol, _vol, _vol)
  ON CONFLICT (instropt_id, ds) DO UPDATE SET
    high = CASE WHEN _vol > high THEN _vol ELSE high END,
    low = CASE WHEN _vol < low THEN _vol ELSE low END,
    last = _vol;
END;
$$;

-- -----------------------------------------------------
-- procedure inserttrdeq
-- -----------------------------------------------------

CREATE OR REPLACE PROCEDURE inserttrdeq(
  _name VARCHAR(20),
  _ts TIMESTAMP(6),
  _price DECIMAL(10,2),
  _size INTEGER,
  _exchcode CHAR(1),
  _ind VARCHAR(10),
  _bidprcbbo DECIMAL(10,2),
  _bidszbbo INTEGER,
  _bidexchbbo VARCHAR(20),
  _askprcbbo DECIMAL(10,2),
  _askszbbo INTEGER,
  _askexchbbo VARCHAR(20)
)
LANGUAGE plpgsql
AS $$
DECLARE oid BIGINT;
  _ds DATE;
BEGIN
  SELECT id FROM instreq WHERE name = _name INTO oid;
  IF (oid IS NULL) THEN
    INSERT INTO instreq VALUES(DEFAULT, _name, 'N', 'E');
    oid := currval(pg_get_serial_sequence('instreq', 'id'));
  END IF;
  _ds := DATE(_ts);
  INSERT INTO trdstatseq VALUES(DEFAULT, oid, _ds, _price, _price, _price, _price, 1, _size)
  ON CONFLICT (instreq_id, ds) DO UPDATE SET
    high = CASE WHEN _price > high THEN _price ELSE high END,
    low = CASE WHEN _price < low THEN _price ELSE low END,
    last = _price,
    count = count + 1,
    volume = volume + _size;
END;
$$;

-- -----------------------------------------------------
-- procedure inserttrdopt
-- -----------------------------------------------------

CREATE OR REPLACE FUNCTION inserttrdopt(
  _name VARCHAR(20),
  _ul VARCHAR(10),
  _spc VARCHAR(10),
  _expiration DATE,
  _strike DECIMAL(10,2),
  _callput CHAR(1),
  _ts TIMESTAMP,
  _price DECIMAL(10,2),
  _size INT,
  _exchcode CHAR(1),
  _ind CHAR(1),
  _bidprcbbo DECIMAL(10,2),
  _bidszbbo INT,
  _bidexchbbo VARCHAR(20),
  _askprcbbo DECIMAL(10,2),
  _askszbbo INT,
  _askexchbbo VARCHAR(20),
  _dayVol INT,
  _openInt INT,
  _sigma DOUBLE PRECISION,
  _delta DOUBLE PRECISION,
  _gamma DOUBLE PRECISION,
  _theta DOUBLE PRECISION,
  _vega DOUBLE PRECISION,
  _rho DOUBLE PRECISION,
  _highD DECIMAL(10,2),
  _lowD DECIMAL(10,2)
)
RETURNS VOID AS $$
DECLARE
  oid BIGINT;
  _ds DATE;
BEGIN
  SELECT id INTO oid FROM instropt WHERE name = _name;
  
  IF (oid IS NULL) THEN
    INSERT INTO instropt (name, ul, expiration, strike, callput, spc) VALUES(_name, _ul, _expiration, _strike, _callput, _spc);
    GET DIAGNOSTICS oid = RESULT_OID;
  END IF;
  
  INSERT INTO trdopt (ts, oid, price, size, exchcode, ind, bidprcbbo, bidszbbo, bidexchbbo, askprcbbo, askszbbo, askexchbbo, dayVol, openInt, sigma, delta, gamma, theta, vega, rho, highD, lowD) VALUES(_ts, oid, _price, _size, _exchcode, _ind, _bidprcbbo, _bidszbbo, _bidexchbbo, _askprcbbo, _askszbbo, _askexchbbo, _dayVol, _openInt, _sigma, _delta, _gamma, _theta, _vega, _rho, _highD, _lowD);
   
  IF(_ind = 'A' OR _ind= 'C' OR _ind='E' OR _ind='G' OR _ind='O') THEN
    _ds := _ts::DATE;
    INSERT INTO trdstatsopt (oid, ds, open, high, low, last, count, volume, highVol, lowVol, highDelta, lowDelta)
    VALUES (oid, _ds, _price, _price, _price, _price, 1, _size, _sigma, _sigma, _delta, _delta)
    ON CONFLICT (oid, ds) DO UPDATE SET
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      last = EXCLUDED.last,
      count = trdstatsopt.count - 1,
      volume = trdstatsopt.volume - EXCLUDED.volume,
      highVol = EXCLUDED.highVol,
      lowVol = EXCLUDED.lowVol,
      highDelta = EXCLUDED.highDelta,
      lowDelta = EXCLUDED.lowDelta;
  ELSE
    _ds := _ts::DATE;
    INSERT INTO trdstatsopt (oid, ds, open, high, low, last, count, volume, highVol, lowVol, highDelta, lowDelta)
    VALUES (oid, _ds, _price, _price, _price, _price, 1, _size, _sigma, _sigma, _delta, _delta)
    ON CONFLICT (oid, ds) DO UPDATE SET
      high = GREATEST(trdstatsopt.high, EXCLUDED.high),
      low = LEAST(trdstatsopt.low, EXCLUDED.low),
      last = EXCLUDED.last,
      count = trdstatsopt.count + 1,
      volume = trdstatsopt.volume + EXCLUDED.volume,
      highVol = GREATEST(trdstatsopt.highVol, EXCLUDED.highVol),
      lowVol = LEAST(trdstatsopt.lowVol, EXCLUDED.lowVol),
      highDelta = GREATEST(trdstatsopt.highDelta, EXCLUDED.highDelta),
      lowDelta = LEAST(trdstatsopt.lowDelta, EXCLUDED.lowDelta);

-- -----------------------------------------------------
-- procedure inserttrdopt2
-- -----------------------------------------------------

CREATE OR REPLACE FUNCTION inserttrdopt2(
  _name VARCHAR(20),
  _ul VARCHAR(10),
  _spc VARCHAR(10),
  _expiration DATE,
  _strike DECIMAL(10,2),
  _callput CHAR(1),
  _ts TIMESTAMP,
  _price DECIMAL(10,2),
  _size INT,
  _exchcode CHAR(1),
  _ind CHAR(1),
  _bidprcbbo DECIMAL(10,2),
  _bidszbbo INT,
  _bidexchbbo VARCHAR(20),
  _askprcbbo DECIMAL(10,2),
  _askszbbo INT,
  _askexchbbo VARCHAR(20),
  _dayVol INT,
  _openInt INT,
  _sigma DOUBLE PRECISION,
  _delta DOUBLE PRECISION,
  _gamma DOUBLE PRECISION,
  _theta DOUBLE PRECISION,
  _vega DOUBLE PRECISION,
  _rho DOUBLE PRECISION,
  _highD DECIMAL(10,2),
  _lowD DECIMAL(10,2),
  _undbidsz INT,
  _undbidprc DECIMAL(10,2),
  _undaskprc DECIMAL(10,2),
  _undasksz INT
)
RETURNS VOID AS $$
DECLARE
  oid BIGINT;
  _ds DATE;
BEGIN
  SELECT id INTO oid FROM instropt WHERE name = _name;
  
  IF (oid IS NULL) THEN
    INSERT INTO instropt (name, ul, expiration, strike, callput, spc) VALUES(_name, _ul, _expiration, _strike, _callput, _spc);
    GET DIAGNOSTICS oid = RESULT_OID;
  END IF;
  
  INSERT INTO trdopt (ts, oid, price, size, exchcode, ind, bidprcbbo, bidszbbo, bidexchbbo, askprcbbo, askszbbo, askexchbbo, dayVol, openInt, sigma, delta, gamma, theta, vega, rho, highD, lowD, undbidsz, undbidprc, undaskprc, undasksz) VALUES(_ts, oid, _price, _size, _exchcode, _ind, _bidprcbbo, _bidszbbo, _bidexchbbo, _askprcbbo, _askszbbo, _askexchbbo, _dayVol, _openInt, _sigma, _delta, _gamma, _theta, _vega, _rho, _highD, _lowD, _undbidsz, _undbidprc, _undaskprc, _undasksz);
  
  IF(_ind = 'A' OR _ind= 'C' OR _ind='E' OR _ind='G' OR _ind='O') THEN
    _ds := _ts::DATE;
    INSERT INTO trdstatsopt (oid, ds, open, high, low, last, count, volume, highVol, lowVol, highDelta, lowDelta)
    VALUES (oid, _ds, _price, _price, _price, _price, 1, _size, _sigma, _sigma, _delta, _delta)
    ON CONFLICT (oid, ds) DO UPDATE SET
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      last = EXCLUDED.last,
      count = trdstatsopt.count - 1,
      volume = trdstatsopt.volume - EXCLUDED.volume,
      highVol = trdstatsopt.highVol,
      lowVol = trdstatsopt.lowVol,
      highDelta = trdstatsopt.highDelta,
      lowDelta = trdstatsopt.lowDelta;
  ELSE
    _ds := _ts::DATE;
    INSERT INTO trdstatsopt (oid, ds, open, high, low, last, count, volume, highVol, lowVol, highDelta, lowDelta)
    VALUES (oid, _ds, _price, _price, _price, _price, 1, _size, _sigma, _sigma, _delta, _delta)
    ON CONFLICT (oid, ds) DO UPDATE SET
      high = GREATEST(trdstatsopt.high, EXCLUDED.high),
      low = LEAST(trdstatsopt.low, EXCLUDED.low),
      last = EXCLUDED.last,
      count = trdstatsopt.count + 1,
      volume = trdstatsopt.volume + EXCLUDED.volume,
      highVol = GREATEST(trdstatsopt.highVol, EXCLUDED.highVol),
      lowVol = LEAST(trdstatsopt.lowVol, EXCLUDED.lowVol),
      highDelta = GREATEST(trdstatsopt.highDelta, EXCLUDED.highDelta),
      lowDelta = LEAST(trdstatsopt.lowDelta, EXCLUDED.lowDelta);
  END IF;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------
-- procedure inserttrdsprd
-- -----------------------------------------------------

CREATE OR REPLACE FUNCTION inserttrdsprd(
  _sprdsym VARCHAR(200),
  _ul VARCHAR(10),  
  _ts TIMESTAMP,
  _price DECIMAL(10,2),
  _size INT,
  _exchcode CHAR(1),  
  _impbidprc DECIMAL(10,2),
  _impbidsz INT,
  _impbidexch VARCHAR(20),
  _impaskprc DECIMAL(10,2),
  _impasksz INT,
  _impaskexch VARCHAR(20),
  _legcnt SMALLINT,
  _xdays SMALLINT,
  _edge INTEGER,
  _sprdtype SMALLINT,
  _ratiocnt SMALLINT,
  _bidexchcnt SMALLINT,
  _askexchcnt SMALLINT,
  _trdcnt INTEGER,
  _dayvolume INTEGER,
  _impsprd DECIMAL(10,2)
) RETURNS VOID AS $$
DECLARE
  _ds DATE;
BEGIN
  INSERT INTO trdsprd (ts, sprdsym, ul, price, size, exchcode, impbidprc, impbidsz, impbidexch, impaskprc, impasksz, impaskexch, legcnt, xdays, edge, sprdtype, ratiocnt, bidexchcnt, askexchcnt, trdcnt, dayvolume, impsprd)
  VALUES (_ts, _sprdsym, _ul, _price, _size, _exchcode, _impbidprc, _impbidsz, _impbidexch, _impaskprc, _impasksz, _impaskexch, _legcnt, _xdays, _edge, _sprdtype, _ratiocnt, _bidexchcnt, _askexchcnt, _trdcnt, _dayvolume, _impsprd);
  
  _ds := _ts::DATE;
  
  INSERT INTO trdstatssprd (sprdsym, ds, open, high, low, last, count, volume)
  VALUES (_sprdsym, _ds, _price, _price, _price, _price, 1, _size)
  ON CONFLICT (sprdsym, ds) DO UPDATE SET
    high = GREATEST(trdstatssprd.high, EXCLUDED.high),
    low = LEAST(trdstatssprd.low, EXCLUDED.low),
    last = EXCLUDED.last,
    count = trdstatssprd.count + 1,
    volume = trdstatssprd.volume + EXCLUDED.volume;
END;
$$ LANGUAGE plpgsql;



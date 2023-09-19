--
-- PostgreSQL database dump
--

-- Dumped from database version 14.7
-- Dumped by pg_dump version 15.1

-- Started on 2023-04-18 15:43:28

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DROP DATABASE rbandits;
--
-- TOC entry 3354 (class 1262 OID 24994)
-- Name: rbandits; Type: DATABASE; Schema: -; Owner: elliott
--

CREATE DATABASE rbandits WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'en_US.UTF-8';


ALTER DATABASE rbandits OWNER TO elliott;

\connect rbandits

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 4 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--

-- *not* creating schema, since initdb creates it


ALTER SCHEMA public OWNER TO postgres;

--
-- TOC entry 6 (class 2615 OID 24995)
-- Name: rbandits; Type: SCHEMA; Schema: -; Owner: elliott
--

CREATE SCHEMA rbandits;


ALTER SCHEMA rbandits OWNER TO elliott;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 211 (class 1259 OID 25055)
-- Name: deltastatsopt; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.deltastatsopt (
    id bigint NOT NULL,
    optid numeric NOT NULL,
    ds date NOT NULL,
    open numeric(10,4),
    high numeric(10,4),
    low numeric(10,4),
    last numeric(10,4)
);


ALTER TABLE rbandits.deltastatsopt OWNER TO elliott;

--
-- TOC entry 210 (class 1259 OID 25054)
-- Name: deltastatsopt_id_seq; Type: SEQUENCE; Schema: rbandits; Owner: elliott
--

CREATE SEQUENCE rbandits.deltastatsopt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE rbandits.deltastatsopt_id_seq OWNER TO elliott;

--
-- TOC entry 3356 (class 0 OID 0)
-- Dependencies: 210
-- Name: deltastatsopt_id_seq; Type: SEQUENCE OWNED BY; Schema: rbandits; Owner: elliott
--

ALTER SEQUENCE rbandits.deltastatsopt_id_seq OWNED BY rbandits.deltastatsopt.id;


--
-- TOC entry 212 (class 1259 OID 25061)
-- Name: exchopt; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.exchopt (
    id character(1) NOT NULL,
    name character varying(16) NOT NULL
);


ALTER TABLE rbandits.exchopt OWNER TO elliott;

--
-- TOC entry 214 (class 1259 OID 25065)
-- Name: instreq; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.instreq (
    id bigint NOT NULL,
    name character varying(20),
    penny character(1)
);


ALTER TABLE rbandits.instreq OWNER TO elliott;

--
-- TOC entry 213 (class 1259 OID 25064)
-- Name: instreq_id_seq; Type: SEQUENCE; Schema: rbandits; Owner: elliott
--

CREATE SEQUENCE rbandits.instreq_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE rbandits.instreq_id_seq OWNER TO elliott;

--
-- TOC entry 3357 (class 0 OID 0)
-- Dependencies: 213
-- Name: instreq_id_seq; Type: SEQUENCE OWNED BY; Schema: rbandits; Owner: elliott
--

ALTER SEQUENCE rbandits.instreq_id_seq OWNED BY rbandits.instreq.id;


--
-- TOC entry 216 (class 1259 OID 25070)
-- Name: instropt; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.instropt (
    id bigint NOT NULL,
    name character varying(30) NOT NULL,
    underlying character varying(10) NOT NULL,
    expiration date,
    strike numeric(10,2),
    callput character(1),
    spc character varying(10)
);


ALTER TABLE rbandits.instropt OWNER TO elliott;

--
-- TOC entry 215 (class 1259 OID 25069)
-- Name: instropt_id_seq; Type: SEQUENCE; Schema: rbandits; Owner: elliott
--

CREATE SEQUENCE rbandits.instropt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE rbandits.instropt_id_seq OWNER TO elliott;

--
-- TOC entry 3358 (class 0 OID 0)
-- Dependencies: 215
-- Name: instropt_id_seq; Type: SEQUENCE OWNED BY; Schema: rbandits; Owner: elliott
--

ALTER SEQUENCE rbandits.instropt_id_seq OWNED BY rbandits.instropt.id;


--
-- TOC entry 218 (class 1259 OID 25075)
-- Name: opnintopt; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.opnintopt (
    id bigint NOT NULL,
    optid numeric NOT NULL,
    ds date NOT NULL,
    vol bigint,
    exch character varying(20)
);


ALTER TABLE rbandits.opnintopt OWNER TO elliott;

--
-- TOC entry 217 (class 1259 OID 25074)
-- Name: opnintopt_id_seq; Type: SEQUENCE; Schema: rbandits; Owner: elliott
--

CREATE SEQUENCE rbandits.opnintopt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE rbandits.opnintopt_id_seq OWNER TO elliott;

--
-- TOC entry 3359 (class 0 OID 0)
-- Dependencies: 217
-- Name: opnintopt_id_seq; Type: SEQUENCE OWNED BY; Schema: rbandits; Owner: elliott
--

ALTER SEQUENCE rbandits.opnintopt_id_seq OWNED BY rbandits.opnintopt.id;


--
-- TOC entry 219 (class 1259 OID 25081)
-- Name: trdindopt; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.trdindopt (
    id character(1) NOT NULL,
    name character varying(16) NOT NULL
);


ALTER TABLE rbandits.trdindopt OWNER TO elliott;

--
-- TOC entry 221 (class 1259 OID 25085)
-- Name: trdopt; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.trdopt (
    id bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    optid numeric NOT NULL,
    price numeric(10,2),
    size bigint NOT NULL,
    exchcode character(1),
    ind character(1),
    bidprcbbo numeric(10,2),
    bidszbbo bigint,
    bidexchbbo character varying(20),
    askprcbbo numeric(10,2),
    askszbbo bigint,
    askexchbbo character varying(20)
);


ALTER TABLE rbandits.trdopt OWNER TO elliott;

--
-- TOC entry 220 (class 1259 OID 25084)
-- Name: trdopt_id_seq; Type: SEQUENCE; Schema: rbandits; Owner: elliott
--

CREATE SEQUENCE rbandits.trdopt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE rbandits.trdopt_id_seq OWNER TO elliott;

--
-- TOC entry 3360 (class 0 OID 0)
-- Dependencies: 220
-- Name: trdopt_id_seq; Type: SEQUENCE OWNED BY; Schema: rbandits; Owner: elliott
--

ALTER SEQUENCE rbandits.trdopt_id_seq OWNED BY rbandits.trdopt.id;


--
-- TOC entry 223 (class 1259 OID 25092)
-- Name: trdstatseq; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.trdstatseq (
    id bigint NOT NULL,
    eqid numeric NOT NULL,
    ds date NOT NULL,
    open numeric(10,2),
    high numeric(10,2),
    low numeric(10,2),
    last numeric(10,2),
    count bigint,
    volume bigint
);


ALTER TABLE rbandits.trdstatseq OWNER TO elliott;

--
-- TOC entry 222 (class 1259 OID 25091)
-- Name: trdstatseq_id_seq; Type: SEQUENCE; Schema: rbandits; Owner: elliott
--

CREATE SEQUENCE rbandits.trdstatseq_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE rbandits.trdstatseq_id_seq OWNER TO elliott;

--
-- TOC entry 3361 (class 0 OID 0)
-- Dependencies: 222
-- Name: trdstatseq_id_seq; Type: SEQUENCE OWNED BY; Schema: rbandits; Owner: elliott
--

ALTER SEQUENCE rbandits.trdstatseq_id_seq OWNED BY rbandits.trdstatseq.id;


--
-- TOC entry 225 (class 1259 OID 25099)
-- Name: trdstatsopt; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.trdstatsopt (
    id bigint NOT NULL,
    optid numeric NOT NULL,
    ds date NOT NULL,
    open numeric(10,2),
    high numeric(10,2),
    low numeric(10,2),
    last numeric(10,2),
    count bigint,
    volume bigint
);


ALTER TABLE rbandits.trdstatsopt OWNER TO elliott;

--
-- TOC entry 224 (class 1259 OID 25098)
-- Name: trdstatsopt_id_seq; Type: SEQUENCE; Schema: rbandits; Owner: elliott
--

CREATE SEQUENCE rbandits.trdstatsopt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE rbandits.trdstatsopt_id_seq OWNER TO elliott;

--
-- TOC entry 3362 (class 0 OID 0)
-- Dependencies: 224
-- Name: trdstatsopt_id_seq; Type: SEQUENCE OWNED BY; Schema: rbandits; Owner: elliott
--

ALTER SEQUENCE rbandits.trdstatsopt_id_seq OWNED BY rbandits.trdstatsopt.id;


--
-- TOC entry 227 (class 1259 OID 25106)
-- Name: volstatsopt; Type: TABLE; Schema: rbandits; Owner: elliott
--

CREATE TABLE rbandits.volstatsopt (
    id bigint NOT NULL,
    optid numeric NOT NULL,
    ds date NOT NULL,
    open numeric(10,4),
    high numeric(10,4),
    low numeric(10,4),
    last numeric(10,4)
);


ALTER TABLE rbandits.volstatsopt OWNER TO elliott;

--
-- TOC entry 226 (class 1259 OID 25105)
-- Name: volstatsopt_id_seq; Type: SEQUENCE; Schema: rbandits; Owner: elliott
--

CREATE SEQUENCE rbandits.volstatsopt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE rbandits.volstatsopt_id_seq OWNER TO elliott;

--
-- TOC entry 3363 (class 0 OID 0)
-- Dependencies: 226
-- Name: volstatsopt_id_seq; Type: SEQUENCE OWNED BY; Schema: rbandits; Owner: elliott
--

ALTER SEQUENCE rbandits.volstatsopt_id_seq OWNED BY rbandits.volstatsopt.id;


--
-- TOC entry 3202 (class 2604 OID 25058)
-- Name: deltastatsopt id; Type: DEFAULT; Schema: rbandits; Owner: elliott
--

ALTER TABLE ONLY rbandits.deltastatsopt ALTER COLUMN id SET DEFAULT nextval('rbandits.deltastatsopt_id_seq'::regclass);


--
-- TOC entry 3203 (class 2604 OID 25068)
-- Name: instreq id; Type: DEFAULT; Schema: rbandits; Owner: elliott
--

ALTER TABLE ONLY rbandits.instreq ALTER COLUMN id SET DEFAULT nextval('rbandits.instreq_id_seq'::regclass);


--
-- TOC entry 3204 (class 2604 OID 25073)
-- Name: instropt id; Type: DEFAULT; Schema: rbandits; Owner: elliott
--

ALTER TABLE ONLY rbandits.instropt ALTER COLUMN id SET DEFAULT nextval('rbandits.instropt_id_seq'::regclass);


--
-- TOC entry 3205 (class 2604 OID 25078)
-- Name: opnintopt id; Type: DEFAULT; Schema: rbandits; Owner: elliott
--

ALTER TABLE ONLY rbandits.opnintopt ALTER COLUMN id SET DEFAULT nextval('rbandits.opnintopt_id_seq'::regclass);


--
-- TOC entry 3206 (class 2604 OID 25088)
-- Name: trdopt id; Type: DEFAULT; Schema: rbandits; Owner: elliott
--

ALTER TABLE ONLY rbandits.trdopt ALTER COLUMN id SET DEFAULT nextval('rbandits.trdopt_id_seq'::regclass);


--
-- TOC entry 3207 (class 2604 OID 25095)
-- Name: trdstatseq id; Type: DEFAULT; Schema: rbandits; Owner: elliott
--

ALTER TABLE ONLY rbandits.trdstatseq ALTER COLUMN id SET DEFAULT nextval('rbandits.trdstatseq_id_seq'::regclass);


--
-- TOC entry 3208 (class 2604 OID 25102)
-- Name: trdstatsopt id; Type: DEFAULT; Schema: rbandits; Owner: elliott
--

ALTER TABLE ONLY rbandits.trdstatsopt ALTER COLUMN id SET DEFAULT nextval('rbandits.trdstatsopt_id_seq'::regclass);


--
-- TOC entry 3209 (class 2604 OID 25109)
-- Name: volstatsopt id; Type: DEFAULT; Schema: rbandits; Owner: elliott
--

ALTER TABLE ONLY rbandits.volstatsopt ALTER COLUMN id SET DEFAULT nextval('rbandits.volstatsopt_id_seq'::regclass);


--
-- TOC entry 3355 (class 0 OID 0)
-- Dependencies: 4
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


-- Completed on 2023-04-18 15:43:28

--
-- PostgreSQL database dump complete
--


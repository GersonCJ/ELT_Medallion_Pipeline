--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

-- Started on 2026-03-26 11:03:53

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 6 (class 2615 OID 246064)
-- Name: co2_project; Type: SCHEMA; Schema: -; Owner: -
--

-- Cria o schema para o Python depositar os dados brutos
CREATE SCHEMA IF NOT EXISTS staging_raw;

-- Cria o schema onde o dbt vai construir a sua camada Silver/Gold
CREATE SCHEMA IF NOT EXISTS co2_project;


SET default_table_access_method = heap;

-- Create database Airflow
CREATE DATABASE airflow;

-- Create database for metabase
CREATE DATABASE metabase_db;



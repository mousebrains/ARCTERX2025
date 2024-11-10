-- Run this as user postgres via
--
-- cat postgresql.init.sql | sudo -u postgres psql

-- Create a login role
CREATE ROLE pat WITH LOGIN;

-- Create the ARCTERX DB
CREATE DATABASE arcterx WITH OWNER=pat;

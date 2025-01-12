CREATE TABLE IF NOT EXISTS drifter ( -- Luca/SIO drifter information
  id VARCHAR(20) COMPRESSION lz4,
  t TIMESTAMP WITH TIME ZONE,
  lat DOUBLE PRECISION, -- May be null, but other data should not be
  lon DOUBLE PRECISION,
  SST DOUBLE PRECISION,
  SLP DOUBLE PRECISION,
  battery DOUBLE PRECISION,
  drogueCounts INTEGER,
  qCSV BOOLEAN DEFAULT FALSE,
  PRIMARY KEY(t, id)
);

-- Fast lookup with id
CREATE INDEX IF NOT EXISTS drifter_ident ON drifter (id);

-- Function sends a notification whenever drifter is updated
CREATE OR REPLACE FUNCTION drifter_updated_func() 
  RETURNS  TRIGGER AS $psql$
BEGIN
  PERFORM pg_notify('drifter_updated', 'drifter');
  RETURN NEW;
end;
$psql$ 
LANGUAGE plpgsql;

-- When drifter is updated or inserted into, call the nofiication function
CREATE OR REPLACE TRIGGER drifter_updated AFTER INSERT OR UPDATE 
  ON drifter
  FOR EACH STATEMENT
    EXECUTE PROCEDURE drifter_updated_func();

--------

CREATE TABLE IF NOT EXISTS filePosition ( -- Position to start reading records from
  filename TEXT COMPRESSION lz4 PRIMARY KEY,
  position BIGINT
); -- filePosition


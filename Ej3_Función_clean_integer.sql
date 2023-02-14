
/* Creo dos funciones, con la misma funcionalidad. La primera utiliza COALESCE y la segunda un IF */


CREATE OR REPLACE FUNCTION keepcoding.clean_integer(p_integer INT64) RETURNS INT64 AS (
  COALESCE(p_integer, -999999)
);

CREATE OR REPLACE FUNCTION keepcoding.clean_integer2(p_integer INT64) RETURNS INT64 AS (
  IF(p_integer IS NULL, -999999, p_integer)
);

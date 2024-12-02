SET input_dir3='${input_dir3}';

SET input_dir4='${input_dir4}';

SET output_dir6='${output_dir6}';

-- Tworzenie tabeli z danymi ligi (datasource4)
CREATE EXTERNAL TABLE IF NOT EXISTS datasource4 (
    league_id INT,
    league_name STRING,
    league_level INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${input_dir4}';

-- Tworzenie tabeli ze statystykami ligi (datasource3)
CREATE EXTERNAL TABLE IF NOT EXISTS datasource3 (
    league_id INT,
    avg_wage DOUBLE,
    avg_age DOUBLE,
    count_players INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${input_dir3}';

-- Tworzenie tabeli z najwyzszymi zarobkami w kazdej lidze
CREATE TABLE IF NOT EXISTS league_analysis (
    league_id INT,
    league_name STRING,
    league_level INT,
    avg_wage DOUBLE,
    avg_age DOUBLE,
    count_players INT
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
location '${output_dir6}';

-- Wstawianie danych do tabeli wynikowej
INSERT OVERWRITE TABLE league_analysis
SELECT
    league_id,
    league_name,
    league_level,
    avg_wage,
    avg_age,
    count_players
FROM (
    SELECT
        m.league_id,
        d.league_name,
        d.league_level,
        m.avg_wage,
        m.avg_age,
        m.count_players,
        ROW_NUMBER() OVER (PARTITION BY d.league_level ORDER BY m.avg_wage DESC) AS rank
    FROM datasource3 m
    JOIN datasource4 d
    ON m.league_id = d.league_id
) ranked
WHERE rank <= 3;

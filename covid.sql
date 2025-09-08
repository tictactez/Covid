-- Creating Database
CREATE DATABASE IF NOT EXISTS covid; 

-- Creating the Table
USE covid;
CREATE TABLE deaths (
    iso_code CHAR(3),
    continent VARCHAR(15),
    location VARCHAR(30),
    date DATE DEFAULT NULL,
    population INT DEFAULT NULL,
    total_cases INT DEFAULT NULL,
    new_cases INT DEFAULT NULL,
    total_deaths INT DEFAULT NULL,
    new_deaths INT DEFAULT NULL,
    total_cases_per_million DOUBLE DEFAULT NULL,
    aged_70_older DOUBLE DEFAULT NULL,
    cardiovasc_death_rate DOUBLE DEFAULT NULL,
    diabetes_prevalence DOUBLE DEFAULT NULL
);

CREATE TABLE vacc (
    iso_code CHAR(3),
    continent VARCHAR(15),
    location VARCHAR(30),
    date DATE DEFAULT NULL,
    population INT DEFAULT NULL,
    new_tests INT DEFAULT NULL,
    total_tests INT DEFAULT NULL,
    positive_rate DOUBLE DEFAULT NULL,
    total_vaccinations INT DEFAULT NULL,
    people_vaccinated INT DEFAULT NULL,
    people_fully_vaccinated INT DEFAULT NULL,
    new_vaccinations INT DEFAULT NULL
);

 -- Importing the data into mysql using data infile feature
 
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidDeaths.csv" INTO TABLE deaths
 FIELDS TERMINATED BY ','
 IGNORE 1 LINES;
 
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidVaccinations.csv" INTO TABLE vacc
 FIELDS TERMINATED BY ','
 IGNORE 1 LINES;
  
SELECT 
    *
FROM
    deaths;
  
SELECT 
    *
FROM
    vacc;

-- Updating zeroes to Nulls in table
UPDATE vacc
SET 
    new_vaccinations = NULL
WHERE
    new_vaccinations = 0;
    
    
    UPDATE deaths
SET 
    new_deaths = NULL
WHERE
    new_deaths = 0;
    
    
UPDATE deaths 
SET 
    new_cases = NULL
WHERE
    new_cases = 0;


-- Total cases by Country

SELECT 
    location AS Country, MAX(total_cases) AS Total_Cases
FROM
    deaths
GROUP BY location
ORDER BY Country;


-- Total cases and deaths globally using Aggregrate Functions
SELECT 
    SUM(new_cases) AS Total_Cases,
    SUM(new_deaths) AS Total_Deaths,
    ROUND(((SUM(new_deaths) / SUM(new_cases)) * 100),
            2) AS Death_Percentage
FROM
    deaths;


-- Cases by Country using Partition By
SELECT location AS County,date as Date,total_cases AS Infections,
ROW_NUMBER() OVER(PARTITION BY location ORDER BY date) as Row_Num
FROM deaths
WHERE location IN ('United States','India','Brazil','France');
 
 
-- Deaths by countries
SELECT 
    location, MAX(total_deaths) AS Deaths
FROM
    deaths
GROUP BY location
HAVING location IN ('United States' , 'India', 'Brazil', 'France')
ORDER BY Deaths DESC;


 -- Percent of People Vaccinated vs New Cases using Subqueries and Lag Function
SELECT 
    date,
    People_Vaccinated,
    People_Vaccinated / Population AS Percent_Vaccinated,
    LAG(new_cases,20) OVER (ORDER BY date) as Lagged_cases
FROM
    (SELECT 
        d.date,
            d.new_cases,
            d.population,
            SUM(v.new_vaccinations) AS People_Vaccinated
    FROM
        deaths AS d
    JOIN vacc AS v ON d.date = v.date
    GROUP BY d.date
    ORDER BY d.date) AS ad
WHERE
    People_vaccinated IS NOT NULL;


 -- Percent of People Vaccinated vs New Cases By Country using CTE

WITH vstats AS (
    SELECT
        d.location,
        d.date,
        d.population,
        v.new_vaccinations,
        SUM(v.new_vaccinations) OVER (
            PARTITION BY d.location
            ORDER BY d.date
        ) AS People_Vaccinated
    FROM deaths AS d
    JOIN vacc AS v
        ON d.location = v.location
        AND d.date = v.date
)
SELECT
    location,
    date,
    population,
    LAG(new_vaccinations,20) OVER (ORDER BY date)
    People_Vaccinated,
    People_Vaccinated / population AS Percent_Vaccinated
FROM vstats
WHERE People_Vaccinated IS NOT NULL;


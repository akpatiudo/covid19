--SQLServer and Teablu Project: Covid-19 Data Analysis, with focus on africal 
--A subset of this dataset from www.ourworldindata.org was analyze, my goal was to keep it simple and to focus on the continent africal.The dataset has 307,831 rows before africal was
--continent was sorted out for analysis 69370 rows

--Calling up the imported Tables
SELECT *
FROM PortfolioProjects..covidDeath$
ORDER BY 3,4;

--CREATING TEMPT TABLE with a few column and narrowing the dataset to africal continient
 drop table covidAfricaDeath
CREATE TABLE covidAfricaDeath (
  location varchar(255),
  Continent varchar(255),
  date datetime,
  total_cases bigint,
  new_cases bigint,
  total_deaths bigint,
  population bigint,
  icu_patients bigint,
  hosp_patients bigint,
  new_deaths bigint
);

INSERT INTO covidAfricaDeath (location, date, total_cases, new_cases, total_deaths, new_deaths, population, icu_patients, hosp_patients)
SELECT location, date, total_cases, new_cases, total_deaths, new_deaths ,population, icu_patients, hosp_patients
FROM PortfolioProjects..covidDeath$
WHERE continent LIKE '%Africa%'
ORDER BY date, new_cases;

-- Create a view to calculate death rate for the African continent
DROP VIEW Deathrate;

CREATE VIEW Deathrate AS
SELECT
    SUM(total_cases) AS total_case,
    SUM(total_deaths) AS total_death,
    (SUM(total_deaths * 1.0) / SUM(total_cases)) * 100.0 AS death_rate
FROM covidAfricaDeath;


--looking at highest cases vs Country

SELECT location, 
	Max(total_cases) as Max_totalcaseCount,
	SUM(total_cases) as SUM_totalcases
From covidAfricaDeath
Where total_cases is not null
Group by location
order by Max_totalcaseCount Desc; 


--looking at highest cases vs location with window function

SELECT location, 
  MAX(total_cases) OVER (PARTITION BY location) AS Max_totalcaseCount,
  SUM(total_cases) OVER (PARTITION BY location) AS SUM_totalcases
FROM covidAfricaDeath 
ORDER BY Max_totalcaseCount DESC;

--Showing total covid cases in africa by Country

SELECT Top 20 location, 
	SUM(total_cases) as Sumtotalcase
FROM covidAfricaDeath 
WHERE location IS NOT NULL
GROUP BY location
ORDER BY 2 DESC;
 
-- look at death rate by location by year
--showing pecentage of total death by number of cases
SELECT 
	location,
	DATEPART(YEAR, date) AS Year, 
	(SUM(total_deaths*1.0)/ SUM(total_cases))*100 AS total_deathpercentage 
FROM covidAfricaDeath 
WHERE total_deaths IS NOT NULL 
AND total_cases IS NOT NULL 
Group by location, DATEPART(YEAR, date)
ORDER BY 1,2;

--LOOKING AT COUNTRIES WITH HIGHEST POPULATION INFECTION RATE COMPARE TO POPULATION
--SHOWS POPULATION GOT COVID by Countries

SELECT 
	location, 
	Sum(population) as total_population, 
	Sum(total_cases) as populationInfected, 
	(Sum(total_cases * 1.0) /Sum(population)) * 100 as pecentPopulationInfected
FROM  covidAfricaDeath 
GROUP BY LOCATION, population
ORDER BY pecentPopulationInfected DESC;

--looking at location that managed their caseds in the hospital

SELECT location, 
	Sum(total_deaths) As totalDeath, 
	Sum(hosp_patients) AS hosp_patients,
	Sum(icu_patients) As icu_patients 
FROM covidAfricaDeath
where total_deaths is not null
and hosp_patients is not null
and icu_patients is not null
Group by location
ORDER BY 2,3;
-- was supress to see that only South africal has recoud for patients manage in hospital, my curosity lead me to change the sort cretaria 

SELECT location, 
	Max(total_cases) as highestCountCases, 
	Max(icu_patients) as highestCountIcupatients, 
	Max(hosp_patients) as highestCountHosp_patients
FROM covidAfricaDeath
where icu_patients is not null
Group by location
ORDER BY 1,2;

--Lookig at new cases vs new death

SELECT location, date, 
	SUM(new_cases) over(partition by location order by location, date) as totalNew_cases,
	SUM(new_deaths) over(partition by date ) as totalNew_deaths
FROM covidAfricaDeath
where new_cases is not null
ORDER BY 3,4 Desc;

SELECT location, date, 
	SUM(new_cases) as totalNew_cases,
	SUM(new_deaths) as totalNew_deaths
FROM covidAfricaDeath
where new_cases is not null
Group by location, date
ORDER BY 3,4 Desc;
-- calling my secound imported table to look at vacination analysis

--looking at total population vs vacination
--trying my skill
--creating CTE
WITH CovidCTE (continent, location, date, population, 
total_vaccinations, people_vaccinated, people_fully_vaccinated, new_vaccinations) AS (
SELECT d.continent, d.location, d.date, d.population, 
v.total_vaccinations, v.people_vaccinated, v.people_fully_vaccinated, v.new_vaccinations
FROM PortfolioProjects..covidDeath$ d
 JOIN PortfolioProjects..covidVaccinations$ v
ON d.location = v.location
AND d.date = v.date
Where d.continent Like '%Africa%'
AND d.continent is not null
)
--Looking at poeple vaccinated: showing percentage of total vaccinated by populationand poeple full vaccinated by population
SELECT location, population,
(Cast(people_fully_vaccinated AS int )*1.0/(population))* 100 AS percentage_fullyVaccinated
FROM CovidCTE
Group by location, population, people_fully_vaccinated
Order By location;
--method 2: join table

;WITH CovidCTE2  AS (
SELECT d.continent, d.location, d.date, d.population, 
v.total_vaccinations, v.people_vaccinated, v.people_fully_vaccinated, v.new_vaccinations
FROM PortfolioProjects..covidDeath$ d
 JOIN PortfolioProjects..covidVaccinations$ v
ON d.location = v.location
AND d.date = v.date
Where d.continent Like '%Africa%'
AND d.continent is not null
)
SELECT location, population,
(Cast(people_fully_vaccinated AS int )*1.0/(population))* 100 AS percentage_fullyVaccinated
FROM CovidCTE2
Group by location, population, people_fully_vaccinated
Order By location;

--CREATING TEMPT TABLE
 drop table covidVaccination
CREATE TABLE covidVaccination (
  continent varchar(255),
  location varchar(255),
  population int,
  date datetime,
  total_vaccinations int,
  people_vaccinated int,
  people_fully_vaccinated int,
  new_vaccinations int
  );

INSERT INTO covidVaccination (continent, location, date, population, 
total_vaccinations, people_vaccinated, people_fully_vaccinated, new_vaccinations) 
SELECT d.continent, d.location, d.date, d.population, 
v.total_vaccinations, v.people_vaccinated, v.people_fully_vaccinated, v.new_vaccinations
FROM PortfolioProjects..covidDeath$ d
 JOIN PortfolioProjects..covidVaccinations$ v
ON d.location = v.location
AND d.date = v.date
Where d.continent Like '%Africa%'
AND d.continent is not null
ORDER BY date, location;

--showing pecentage  total vacination by population
SELECT 
	location, date, population,  
	((total_vaccinations *1.0)/(population))* 100 AS percentageTotalVaccinated
FROM covidVaccination
Where total_vaccinations is not null
Group by location,population,date, total_vaccinations
order By 1,3 ,4;

--showing pecentage  of people not fully vaccinated
SELECT 
	location, date, population, 
	((people_vaccinated - people_fully_vaccinated) *1.0/population)* 100 AS percentage_NotfullyVaccinated
FROM covidVaccination
Where people_vaccinated is not null
And people_fully_vaccinated is not null
Group by location,population,date,people_vaccinated,people_fully_vaccinated
order By 1,2,3;


--showing pecentage  of people fully vaccinated
SELECT 
location, date, population, 
((people_fully_vaccinated) *1.0/population)* 100 AS percentage_fullyVaccinated
FROM covidVaccination
Where people_fully_vaccinated is not null
Group by location,population,date,people_vaccinated,people_fully_vaccinated
order By 1,2,4;


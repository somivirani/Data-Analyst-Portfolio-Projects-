-- ----------------------------------------------------------------------------------------------------------------------
--                     SQL Data Cleaning & Preprocessing - "2022 Workforce Layoffs"
--                     Source: https://www.kaggle.com/datasets/swaptr/layoffs-2022
-- ----------------------------------------------------------------------------------------------------------------------

-- 1. Create a Staging Table to Preserve Raw Data
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT INTO world_layoffs.layoffs_staging 
SELECT * 
FROM world_layoffs.layoffs;

-- Check the raw data
SELECT * FROM world_layoffs.layoffs_staging;

-- ----------------------------------------------------------------------------------------------------------------------
--                     Data Cleaning Steps:
--                     1. Remove duplicates
--                     2. Standardize data formats and correct errors
--                     3. Review and manage null values
--                     4. Remove unnecessary columns/rows
-- ----------------------------------------------------------------------------------------------------------------------

-- Remove duplicates using a ROW_NUMBER() approach:
-- (Here, we create a new staging table with a row number to identify duplicates)
CREATE TABLE world_layoffs.layoffs_staging2 (
  company                TEXT,
  location               TEXT,
  industry               TEXT,
  total_laid_off         INT DEFAULT NULL,
  percentage_laid_off    TEXT,
  `date`                 TEXT,
  stage                  TEXT,
  country                TEXT,
  funds_raised_millions  INT DEFAULT NULL,
  row_num                INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO world_layoffs.layoffs_staging2
(
  company,
  location,
  industry,
  total_laid_off,
  percentage_laid_off,
  `date`,
  stage,
  country,
  funds_raised_millions,
  row_num
)
SELECT 
  company,
  location,
  industry,
  total_laid_off,
  percentage_laid_off,
  `date`,
  stage,
  country,
  funds_raised_millions,
  ROW_NUMBER() OVER (
      PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
  ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Remove duplicate rows by deleting those with row_num > 1
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- Verify duplicates are removed
SELECT * FROM world_layoffs.layoffs_staging2;

-- Optionally, drop the row_num column now that duplicates are handled
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- ----------------------------------------------------------------------------------------------------------------------
--                     2. Standardize Data
-- ----------------------------------------------------------------------------------------------------------------------

-- Trim whitespace in company names
UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

-- Standardize empty industry values to NULL
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate null industry values using other rows with the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- Standardize variations in the Crypto category to 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize country names: remove trailing periods from 'United States.' entries
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Convert date strings to DATE type
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- ----------------------------------------------------------------------------------------------------------------------
--                     3. Look at Null Values
-- ----------------------------------------------------------------------------------------------------------------------

-- The nulls in total_laid_off, percentage_laid_off, and funds_raised_millions are expected.
-- We keep them as null since it simplifies calculations during EDA.
SELECT * FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

-- Delete rows where both total_laid_off and percentage_laid_off are null (if they are not useful)
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- ----------------------------------------------------------------------------------------------------------------------
--                     4. Remove Any Unnecessary Columns/Rows
-- ----------------------------------------------------------------------------------------------------------------------

-- (This section is for additional cleaning if needed; currently, our dataset is well-prepared for analysis.)
SELECT * FROM world_layoffs.layoffs_staging2;

-- ----------------------------------------------------------------------------------------------------------------------
--                     Exploratory Data Analysis (EDA)
--                     "Here, we’re going to explore the data to identify trends, patterns, and any interesting insights such as outliers."
-- ----------------------------------------------------------------------------------------------------------------------

-- Total Layoffs by Company (Descending Order)
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY total_layoffs DESC;

-- Total Layoffs by Industry (Descending Order)
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY total_layoffs DESC;

-- Total Layoffs by Country (Descending Order)
SELECT country, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY total_layoffs DESC;

-- Total Layoffs by Year (Most recent first)
SELECT YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY year DESC;

-- Companies with 100% layoffs (percentage_laid_off = 1), ordered by funds raised
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Ranking: Top 5 Companies with Largest Single-Day Layoffs
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY total_laid_off DESC
LIMIT 5;

-- Ranking: Top 10 Companies with Highest Total Layoffs
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY total_layoffs DESC
LIMIT 10;

-- Top 10 Locations by Total Layoffs
SELECT location, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY total_layoffs DESC
LIMIT 10;

-- ----------------------------------------------------------------------------------------------------------------------
-- Advanced Analysis: Top 3 Companies per Year by Total Layoffs
-- ----------------------------------------------------------------------------------------------------------------------
WITH CompanyYear AS (
  SELECT 
    company, 
    YEAR(`date`) AS year, 
    SUM(total_laid_off) AS total_layoffs
  FROM world_layoffs.layoffs_staging2
  GROUP BY company, YEAR(`date`)
),
RankedCompanies AS (
  SELECT 
    company, 
    year,
    total_layoffs,
    DENSE_RANK() OVER (PARTITION BY year ORDER BY total_layoffs DESC) AS ranking
  FROM CompanyYear
)
SELECT 
  company, 
  year, 
  total_layoffs, 
  ranking
FROM RankedCompanies
WHERE ranking <= 3
ORDER BY year ASC, total_layoffs DESC;

-- ----------------------------------------------------------------------------------------------------------------------
--                                    Conclusion
-- ----------------------------------------------------------------------------------------------------------------------
-- This project demonstrates an end-to-end approach to SQL data cleaning and exploratory analysis.
-- It transforms raw 2022 workforce layoffs data into actionable insights,
-- setting the stage for further predictive modeling.


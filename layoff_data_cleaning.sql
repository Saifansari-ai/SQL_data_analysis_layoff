USE layoff;
CREATE TABLE layoff_staging
LIKE layoff;

SELECT *
FROM layoff_staging;


INSERT INTO layoff_staging
SELECT *
FROM layoff;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,
date,stage,country,funds_raised_millions) AS row_num
FROM layoff_staging;

WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions) AS row_num
FROM layoff_staging)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoff_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
  `percentage_laid_off` double DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoff_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions) AS row_num
FROM layoff_staging;

DELETE
FROM layoff_staging3
WHERE row_num > 1;

SELECT *
FROM layoff_staging3;


ALTER TABLE layoff_staging3
DROP row_num;

DROP TABLE layoff_staging2;

DESCRIBE layoff_staging;


SELECT *
FROM layoff_staging3 LIMIT 1;

SELECT COUNT(*) FROM layoff_staging3
WHERE total_laid_off AND funds_raised_millions IS NULL;

SELECT country,stage,COUNT(company) AS 'COUNT',SUM(total_laid_off) AS 'SUM' FROM layoff_staging3
GROUP BY country,stage
ORDER BY COUNT DESC;

SELECT *
FROM layoff_staging3 LIMIT 1;

UPDATE layoff_staging3
SET country = TRIM(country);

SELECT DISTINCT stage
FROM layoff_staging3
ORDER BY 1;

UPDATE layoff_staging3
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT *
FROM layoff_staging3;

UPDATE layoff_staging3
SET country = LOWER(country);

DESCRIBE layoff_staging3;

SELECT *
FROM layoff_staging3;


UPDATE layoff_staging3
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

DESCRIBE layoff_staging3;

ALTER TABLE layoff_staging3
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoff_staging3;

SELECT * 
FROM layoff_staging3
WHERE industry IS NULL;

SELECT *
FROM layoff_staging3
WHERE company = 'carvana';

SELECT * 
FROM layoff_staging3 t1
JOIN layoff_staging3 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoff_staging3 t1
JOIN layoff_staging3 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoff_staging3
SET industry = NULL 
WHERE industry = '';

SELECT *
FROM layoff_staging3
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoff_staging3
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


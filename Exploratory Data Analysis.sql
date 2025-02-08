-- Exploratory Data Analysis

SELECT MAX(total_laid_off),MAX(percentage_laid_off)
FROM layoff_staging3; 

SELECT * 
FROM layoff_staging3
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT * 
FROM layoff_staging3
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company,SUM(total_laid_off)
FROM layoff_staging3
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`),MAX(`date`)
FROM layoff_staging3;

SELECT industry,SUM(total_laid_off)
FROM layoff_staging3
GROUP BY industry
ORDER BY 2 DESC;

SELECT country,SUM(total_laid_off)
FROM layoff_staging3
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoff_staging3
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage,SUM(total_laid_off)
FROM layoff_staging3
GROUP BY stage
ORDER BY 2 DESC;


SELECT SUBSTRING(`date`,1,7) AS `months`,SUM(total_laid_off)
FROM layoff_staging3
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `months`
ORDER BY 1 ASC;

WITH Rolling_total AS
(SELECT SUBSTRING(`date`,1,7) AS `months`,SUM(total_laid_off) AS total_off
FROM layoff_staging3
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `months`
ORDER BY 1 ASC
)
SELECT `months`,total_off, 
SUM(total_off) OVER(ORDER BY `months`) AS rolling_total
FROM Rolling_total;

SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoff_staging3
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year(company,years,total_laid_off) AS 
(SELECT company, YEAR(`date`),SUM(total_laid_off)
FROM layoff_staging3
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
),Company_Year_Rank AS 
(
SELECT *,DENSE_RANK()
OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS `rank`
FROM Company_Year
WHERE years IS NOT NULL)
SELECT * 
FROM Company_Year_Rank
WHERE `rank` <= 5;


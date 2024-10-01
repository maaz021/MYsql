-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022






SELECT * 
FROM world_layoffs.layoffs;



-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates



SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- now you may want to write it like this:
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;







-- 2. Standardize Data

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM world_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM world_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;





-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values




-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;







-- work 

select *
from layoffs ;

create table layoffs_staging2
like layoffs;

select *
from layoffs_staging2 ;
 
insert layoffs_staging2
select *
from layoffs ;

select* ,
row_number()over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from  layoffs_staging2 ;
 
with duplicate_cte as 
(
select* ,
row_number()over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging2
)
select* 
from duplicate_cte
where row_num>1 ;


select *
from layoffs_staging2
where company='hibob' ;





CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INt 
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoffs_staging3 ;


insert into layoffs_staging3
select* ,
row_number()over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging2 ;

select *
from layoffs_staging3 
where row_num > 1 ;

delete
from layoffs_staging3 
where row_num > 1 ;


select *
from layoffs_staging3 ;

-- standardizing data -
select distinct (company)
from layoffs_staging3 ;

select distinct (trim(company))
from layoffs_staging3 ;

select company , (trim(company))
from layoffs_staging3 ;


update layoffs_staging3 
set company = trim(company) ;


select distinct (company)
from layoffs_staging3 ;

select distinct (industry)
from layoffs_staging3 ;


select distinct (industry)
from layoffs_staging3 
order by 1 ;

select *
from layoffs_staging3 
where industry like 'crypto%' ;

update layoffs_staging3
set industry = 'crypto' 
where industry like 'crypto%' ;

select distinct (location)
from layoffs_staging3 
order by 1 ;
select distinct (country)
from layoffs_staging3 
order by 1 ;

select *
from layoffs_staging3 
where country like 'united states%'
order by 1 ;

select distinct country , trim(trailing '.' from country)
from layoffs_staging3 
order by 1 ;

update layoffs_staging3 
set country =  trim(trailing '.' from country)
where country like ' United States% ' ;


select distinct country 
from layoffs_staging3 
order by 1 ;

select distinct country , trim(trailing '.' from country)
from layoffs_staging3 
order by 1 ;

update layoffs_staging3 
set country =  trim(trailing '.' from country)
where country like ' United States% ' ;

select distinct country 
from layoffs_staging3 
order by 1 ;


select distinct country , trim(trailing '.' from country)
from layoffs_staging3 
order by 1 ;


update layoffs_staging3 
set country = trim(trailing '.' from country)
where country like 'United States%';


select*
from layoffs_staging3 ;
select `date`
from layoffs_staging3 ;
select`date`,
str_to_date( `date` , '%m/%d/%Y')
from layoffs_staging3 ;

update layoffs_staging3 
set `date` = str_to_date( `date` , '%m/%d/%Y') ;


select`date`
from layoffs_staging3 ;



INSERT INTO `world_layoffs`.`layoffs_staging3`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
VALUES
(<{company: }>,
<{location: }>,
<{industry: }>,
<{total_laid_off: }>,
<{percentage_laid_off: }>,
<{date: }>,
<{stage: }>,
<{country: }>,
<{funds_raised_millions: }>,
<{row_num: }>);


select`date`,
str_to_date( `date` , '%m/%d/%Y')
from layoffs_staging3 ;

alter table layoffs_staging3
modify column `date` date ;

select*
from layoffs_staging3
where total_laid_off is null 
and percentage_laid_off is null;

select * 
from layoffs_staging3
where industry is null
or industry = '' ; 

select*
from layoffs_staging3
where company = 'Airbnb' ;

select*
from layoffs_staging3
where company like 'Bally%';

select *
from layoffs_staging3 t1
join layoffs_staging3 t2
     on t1.company = t2.company 
where (t1.industry is null or t1.industry ='')
and t2.industry is not null ; 

select t1.industry , t2.industry
from layoffs_staging3 t1
join layoffs_staging3 t2
     on t1.company = t2.company 
where (t1.industry is null or t1.industry ='')
and t2.industry is not null ; 




update layoffs_staging3 t1
join layoffs_staging3 t2
     on t1.company = t2.company 
set t1.industry=t2.industry
where (t1.industry is null or t1.industry ='')
and t2.industry is not null ; 



update layoffs_staging3
set industry = null 
where industry = '' ;

update layoffs_staging3 t1
join layoffs_staging3 t2
     on t1.company = t2.company 
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null ; 


select t1.industry , t2.industry
from layoffs_staging3 t1
join layoffs_staging3 t2
     on t1.company = t2.company 
where t1.industry is null 
and t2.industry is not null ; 

UPDATE layoffs_staging3 t1
        JOIN
    layoffs_staging3 t2 ON t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL; 








select*
from layoffs_staging3
where total_laid_off is null 
and percentage_laid_off is null;

delete
from layoffs_staging3
where total_laid_off is null 
and percentage_laid_off is null;

select*
from layoffs_staging3 ;

alter table layoffs_staging3
drop column row_num ;


































































































































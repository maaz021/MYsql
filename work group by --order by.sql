SELECT *
FROM employee_demographics;

 SELECT gender , age
FROM employee_demographics
GROUP BY gender,age;




SELECT gender , avg(age)
FROM employee_demographics
GROUP BY gender;
SELECT occupation,salary
FROM employee_salary
GROUP BY occupation , salary


SELECT  gender , avg(age) ,max(age)
FROM employee_demographics
GROUP BY gender
;
 SELECT gender ,avg( age),max(age),min(age),count(age)
FROM employee_demographics
GROUP BY gender;
-- order by

SELECT  *
FROM employee_demographics
order by gender, age desc;


SELECT  *
FROM employee_demographics
order by 5, 4 desc;
SELECT  *
FROM employee_salary
order by age ;
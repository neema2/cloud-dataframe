from dataframe import *

e_df = Dataframe.from_("employees", alias="e", columns={ "id": int, "name": str, "department": str, "salary": float })
d_df = Dataframe.from_("department", alias="d")

(e_df.filter( lambda e: e.id > 10).
 left_join(d_df, lambda e, d: e.department_id == d.id).
 select(lambda e, d: [ e.id, e.name, (department_name := d.name), d.location, e.salary]).
 extend(lambda e, d: [
    (ids := e.id + d.id),
    (avg_val := over(
        func=avg(e.salary),
        partition=d.location,
        order_by=[e.name, d.location],
        frame=rows(0, unbounded())))]).
 group_by(lambda df: [df.id, df.name], lambda df: [ sum_salary := sum(df.salary),count_dept := count(df.department_name) ]).
 having(lambda df: df.sum_salary > 100).
 filter(lambda df: df.id > 100).
 extend(lambda df: (calc_col := df.name + df.title))) #should we allow this?

'''
SELECT df.id, df.name, sum_salary, count_dept, df.name + df.title AS calc_col
FROM 
(
SELECT df.id, df.name, sum( df.salary ) AS sum_salary, count( df.department_name ) as count_dept
FROM   (
        SELECT e.id, e.name, d.name AS department_name, d.location, e.salary, e.id + d.id AS ids, 
               avg(e.salary) over ( partition by d.location order by e.name, d.location BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS avg_value
        FROM   employees e
        LEFT   JOIN department d
        ON     e.department_id = d.id
        WHERE  e.id > 10
       ) df       
GROUP  BY df.id, df.name
HAVING sum_salary > 100
) df
WHERE  df.id > 100
'''

# https://prql-lang.org/book/index.html
p_df = Dataframe.from_("employees", alias="e")
p_df = p_df.filter(lambda e: e.start_date > '2021-01-01')
p_df = p_df.extend(lambda e: [
    (gross_salary := e.salary + 10),
    (gross_cost := gross_salary + e.benefits) ])
p_df = p_df.filter(lambda e: e.gross_cost > 0)
p_df = p_df.group_by(lambda df: [df.title, df.country],
        lambda df: [
             (avg_gross_salary := avg(df.gross_salary)),
             (sum_gross_cost := sum(df.gross_cost))])
p_df = p_df.having(lambda df: df.sum_gross_cost > 100_000)
p_df = p_df.extend(lambda df: (id := f"{df.title}_{df.country}"))
p_df = p_df.extend(lambda df: (country_code := left(df.country, 2)))
p_df = p_df.order_by(lambda df: [df.sum_gross_cost, -df.country])
p_df = p_df.limit(10)

'''
SELECT df.title, df.country, avg(df.gross_salary) as avg_gross_salary, sum(df.gross_cost) as sum_gross_cost
FROM   (
        SELECT *, e.salary + 10 as gross_salary, (e.salary + 10) + e.benefits as gross_cost
        FROM   employees e
        WHERE  e.start_date > '2021-01-01'
        AND    ((e.salary + 10) + e.benefits) > 0 
       ) df
GROUP  BY df.title, df.country
HAVING df.sum_gross_cost > 100_000       
'''

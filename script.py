from legendql import *

emp = LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
dep = LegendQL.from_("department", columns={ "id": int, "name": str, "city": str, "code": str })

(emp.filter( lambda r: r.id > 10)
 .left_join(dep,
    lambda e, d: e.dept_id == d.id,
    lambda e, d: [ e.id, e.name, (department_name := d.name), d.location, e.salary])
 .extend(lambda r: [
    (ids := r.emp_id + r.dept_id),
    (avg_val := over(r.location, avg(r.salary), sort=[r.emp_name, -r.location], frame=rows(0, unbounded())))])
 .group_by(lambda r: aggregate(
    [ r.id, r.name ],
    [ sum_salary := sum(r.salary), count_dept := count(r.department_name) ],
     filter=r.sum_salary > 100_000))
 .filter(lambda r: r.id > 100)
 .extend(lambda r: (calc_col := r.name + r.title)))

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
lq = LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
lq = lq.filter(lambda e: e.start_date > '2021-01-01')
lq = lq.extend(lambda e: [
        (gross_salary := e.salary + 10),
        (gross_cost := gross_salary + e.benefits) ])
lq = lq.filter(lambda e: e.gross_cost > 0)
lq = lq.group_by(lambda e: aggregate(
        [e.title, e.country],
        [avg_gross_salary := avg(e.gross_salary), sum_gross_cost := sum(e.gross_cost)],
        filter=e.sum_gross_cost > 100_000))
lq = lq.extend(lambda e: (id := f"{e.title}_{e.country}"))
lq = lq.extend(lambda e: (country_code := left(e.country, 2)))
lq = lq.order_by(lambda e: [e.sum_gross_cost, -e.country])
lq = lq.limit(10)


(LegendQL.from_("employees", columns={ "id": int, "name": str, "dept_id": str, "salary": float })
 .filter(lambda e: e.start_date > '2021-01-01')
 .extend(lambda e: [
    (gross_salary := e.salary + 10),
    (gross_cost := gross_salary + e.benefits) ])
 .filter(lambda e: e.gross_cost > 0)
 .group_by(lambda e: aggregate(
    [e.title, e.country],
    [avg_gross_salary := avg(e.gross_salary),
     sum_gross_cost := sum(e.gross_cost)],
    filter=e.sum_gross_cost > 100_000))
 .extend(lambda e: (id := f"{e.title}_{e.country}"))
 .extend(lambda e: (country_code := left(e.country, 2)))
 .order_by(lambda e: [e.sum_gross_cost, -e.country])
 .limit(10))


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

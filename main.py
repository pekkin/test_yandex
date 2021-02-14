# CREATE TABLE IF NOT EXISTS st_tasks
# (key INTEGER PRIMARY KEY AUTOINCREMENT,(уникальный идентификатор задачи)
# assignee VARCHAR(50),(уникальное имя исполнителя)
# status VARCHAR(50),(статус задачи: Open, On support side, Verifying, Close)
# update_u DATE,(дата последнего обновления)
# create_c DATE);(дата создания задачи)

import sqlite3
conn = sqlite3.connect('Yandex2test1_corr')
c = conn.cursor()


def show(*args):
    for row in c.execute(*args):
        print(row)


def calculate(days):
    border = (days, days)

    number = c.execute(
        "SELECT ROUND (CAST(COUNT(*) AS REAL) / COUNT (DISTINCT assignee) )  FROM st_tasks WHERE status!= 'Close'").fetchone()
    c.execute(
        "CREATE TEMPORARY TABLE assigneeTasks AS Select assignee AS assigneeId, sum(case status when 'Close' then 0 else 1 end)- "
        "sum (CASE WHEN "
        "((((julianday(update_u) - julianday(create_c)   < 0 )"
        "AND(julianday(Current_date) - julianday(create_c) >= ?)) OR ((julianday(Current_date) - julianday(update_u) >= ?) "
        "AND (julianday(update_u) - julianday(create_c)   > 0)))AND status!= 'Close') "
        "THEN 1 "
        "ELSE 0 "
        "END) AS TasksCount "
        "FROM st_tasks  GROUP BY assignee ", border)
    c.execute("CREATE TEMPORARY TABLE generator (id INTEGER)")
    list_l = []
    for i in range(1, int(number[0]) + 1):
        list_l.append((i,))
    c.executemany('INSERT INTO generator VALUES (?)', list_l)
    c.execute(
        "CREATE TEMPORARY TABLE number_of_tasks AS select row_number() over(ORDER BY assigneeTasks.TasksCount ) as number, "
        "assigneeId from assigneeTasks join generator where generator.id > assigneeTasks.TasksCount")
    c.execute(
        "CREATE TEMPORARY TABLE number_of_distribute AS Select row_number() over(ORDER BY key ) as number, key,assignee FROM st_tasks WHERE (((julianday(update_u) - julianday(create_c)   < 0 ) "
        "AND(julianday(Current_date) - julianday(create_c) >= ?)) OR ((julianday(Current_date) - julianday(update_u) >= ?) "
        "AND (julianday(update_u) - julianday(create_c)   > 0)))AND status!= 'Close' ", border)
    show(
        "SELECT key,assigneeId from number_of_distribute join number_of_tasks on number_of_tasks.number = number_of_distribute.number")

if __name__ == '__main__':
    calculate(10)  # days

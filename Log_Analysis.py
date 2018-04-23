import psycopg2

DBNAME = "news"


def query1():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("select substring (path from 10) as path, count(id) as views "
              "from log group by path order by views desc;")
    c.execute("select articles.title, pop_article.views from articles "
              "join pop_article on articles.slug=pop_article.path "
              "order by pop_article.views desc; "
              "select * from popular_articles limit 3;")
    results = c.fetchall()
    print results
    db.close()

print "What are the most popular three articles of all time?"
print "Answer - "
query1()


def query2():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("select articles.title, authors.name from articles "
              "join authors on articles.author = authors.id;")
    c.execute("select author_works.name, sum(popular_articles.views) "
              "as total_views from author_works join popular_articles "
              "on author_works.title=popular_articles.title "
              "group by author_works.name order by total_views desc;")
    results = c.fetchall()
    print results
    db.close()

print "Who are the most popular article authors of all time?"
print "Answer - "
query2()


def query3():
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("select count(status) as failed_attempts, "
              "to_char(time, 'Month DD, YYYY') as day "
              "from log where status != '200 OK' group by day;")
    c.execute("select count(status) as total_attempts, "
              "to_char(time, 'Month DD, YYYY') as day "
              "from log group by day;")
    c.execute("select total.day, total.total_attempts, "
              "failed.failed_attempts from total "
              "left join failed on total.day=failed.day;")
    c.execute("select day, 100 * "
              "cast(failed_attempts as float)/cast(total_attempts as float)) "
              "as error from error_table order by error desc limit 3")
    results = c.fetchall()
    print results
    db.close()

print "On which days did more than 1% of requests lead to errors?"
print "Answer - "
query3()

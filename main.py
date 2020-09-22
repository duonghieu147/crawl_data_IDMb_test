import bs4
import pandas
import requests
import psycopg2

url = 'https://www.imdb.com/search/title/?count=100&groups=top_1000&sort=user_rating%27' # các bạn thay link của trang mình cần lấy dữ liệu tại đây
def get_page_content(url):
   page = requests.get(url,headers={"Accept-Language":"en-US"})
   return bs4.BeautifulSoup(page.text,"html.parser")
soup = get_page_content(url)
#print(soup)
movies = soup.findAll('h3', class_='lister-item-header')
titles = [movie.find('a').text for movie in movies]

release = [rs.find('span',class_="lister-item-year text-muted unbold").text for rs in movies]

rate2= soup.findAll('div', class_='inline-block ratings-imdb-rating')
rate = [rate3.find('strong').text for rate3 in rate2]
#rate4 = soup.find('div', class_='inline-block ratings-imdb-rating')['data-value']

certificate = [ce.text for ce in soup.findAll('span',class_='certificate')]
runtime = [rt.text for rt in soup.findAll('span',class_='runtime')]
genre = [gr.text for gr in soup.findAll('span',class_="genre")]


#print(movies)
print(titles)
print('=================================================')
print(release)
print('=================================================')
print(rate)
print('=================================================')
print(certificate)
print('=================================================')
print(runtime)
print('=================================================')
print(genre)


pandas.DataFrame({'titles':titles,
                  'release':release,
                  'certificate':certificate,
                  'runtime':runtime,
                  'genre': genre,
                  'rates': rate})


# Connect to an existing database
conn = psycopg2.connect("dbname=test user=postgres")
#Open a cursor to perform database operations
cur = conn.cursor()
#Execute a command: this creates a new tabl
cur.execute("CREATE TABLE test (id serial PRIMARY KEY, titles varchar, release varchar,certificate varchar, runtime varchar,genre varchar, rates integer);")
#Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
cur.execute("INSERT INTO test (titles, release,certificate,runtime,,rates) VALUES (%s, %s,%s,%s,%s)"


#Query the database and obtain data as Python objects
cur.execute("SELECT * FROM test;")
cur.fetchone()
(1, 100, "abc'def")

# Make the changes to the database persistent/Thực hiện các thay đổi đối với cơ sở dữ liệu liên tục
conn.commit()

# Close communication with the database/dong giao tiep
cur.close()
conn.close()




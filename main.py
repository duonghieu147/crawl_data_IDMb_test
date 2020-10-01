from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import numpy as np
import re
import psycopg2
import datetime

url=['https://www.vietnamworks.com/viec-lam-internet-online-media-i57-vn',
     'https://www.vietnamworks.com/viec-lam-it-phan-mem-i35-vn',
     'https://www.vietnamworks.com/viec-lam-it-phan-cung-mang-i55-vn']
request_url=['https://jf8q26wwud-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser&x-algolia-application-id=JF8Q26WWUD&x-algolia-api-key=2bc790c0d4f44db9ab6267a597d17f1a']

from_data=['{"requests":[{"indexName":"vnw_job_v2_57","params":"query=&hitsPerPage=200&attributesToRetrieve=%5B%22*%22%2C%22-jobRequirement%22%2C%22-jobDescription%22%5D&attributesToHighlight=%5B%5D&query=&facetFilters=%5B%5B%22categoryIds%3A57%22%5D%2C%5B%22locationIds%3A29%22%5D%5D&filters=&numericFilters=%5B%5D&page=0&restrictSearchableAttributes=%5B%22jobTitle%22%2C%22skills%22%2C%22company%22%5D"}]}',
           '{"requests":[{"indexName":"vnw_job_v2_35","params":"query=&hitsPerPage=200&attributesToRetrieve=%5B%22*%22%2C%22-jobRequirement%22%2C%22-jobDescription%22%5D&attributesToHighlight=%5B%5D&query=&facetFilters=%5B%5B%22categoryIds%3A35%22%5D%2C%5B%22locationIds%3A29%22%5D%5D&filters=&numericFilters=%5B%5D&page=0&restrictSearchableAttributes=%5B%22jobTitle%22%2C%22skills%22%2C%22company%22%5D"}]}',
           '{"requests":[{"indexName":"vnw_job_v2_55","params":"query=&hitsPerPage=200&attributesToRetrieve=%5B%22*%22%2C%22-jobRequirement%22%2C%22-jobDescription%22%5D&attributesToHighlight=%5B%5D&query=&facetFilters=%5B%5B%22categoryIds%3A55%22%5D%2C%5B%22locationIds%3A29%22%5D%5D&filters=&numericFilters=%5B%5D&page=0&restrictSearchableAttributes=%5B%22jobTitle%22%2C%22skills%22%2C%22company%22%5D"}]}'
           ]
url_vnw='https://www.vietnamworks.com/senior-java-microservices-salary-up-to-2500-1-1298621-jv'

start=datetime.datetime.now()
print(start)
data=[]
for k in from_data:
    response = requests.post(request_url[0], k)
    # print(response)
    soup = BeautifulSoup(response.content, "html.parser").text
    # print(soup)
    parsed_json = (json.loads(soup))  # parse a JSON string
    length_link = len(parsed_json['results'][0]['hits'])  #
    print("Total Record :" + str(length_link))
    k = 0
    while k < length_link/1:
        alias = parsed_json['results'][0]['hits'][k]['alias']
        jobId = parsed_json['results'][0]['hits'][k]['jobId']
        link_works = 'https://www.vietnamworks.com/' + str(alias) + '-' + str(jobId) + '-' + 'jv'
        print(link_works)
        print(k+1)
        k = k + 1
        response = requests.get(link_works)
        print(response)
        soup = BeautifulSoup(response.content, "html.parser")
        # print(soup)

        try:
            title = soup.find("h1", class_="job-title")
            benefits = soup.find("div", class_="benefits")
            description = soup.find("div", class_="description")
            skill = soup.findChildren('span', class_='content')[3]
            requirement = soup.find("div", class_="requirements")
            # Check Form
            if (title or benefits or description or skill or requirement) is None:
                title = ''
                benefits = ''
                description = ''
                skill = ''
                requirement = ''
            elif 1:
                title = title.text
                benefits = benefits.text
                description = description.text
                skill = skill.text
                requirement = requirement.text
            title = ' '.join(title.split())
            benefits = ' '.join(benefits.split())
            description = ' '.join(description.split())
            skill = ' '.join(skill.split())
            requirement = ' '.join(requirement.split())
            # Xoa Ky tu Dac biet
            key_works_replay = '[•*·●]'
            title = re.sub(key_works_replay, '+', title)
            benefits = re.sub(key_works_replay, '+', benefits)
            description = re.sub(key_works_replay, '+', description)
            skill = re.sub(key_works_replay, '+', skill)
            requirement = re.sub(key_works_replay, '+', requirement)

            # print('Job:_________________________________________________'+'\n'+ title)
            # print('Benefits:____________________________________________'+'\n'+ benefits)
            # print('description:_________________________________________'+'\n'+ description)
            # print('requirement:_________________________________________'+'\n' +requirement)
            # print('skill:_______________________________________________'+'\n'+skill)
            data.append({
                "Job": title,
                "Benefits": benefits,
                "Description": description,
                "Requirement": requirement,
                "Skill": skill
            })

        except:
            print("An exception occurred")
conn = psycopg2.connect("dbname=VNW1 user=postgres password=123456")
# Open a cursor to perform database operations
cur = conn.cursor()
# Execute a command: this creates a new table
cur.execute(
    "CREATE TABLE IF NOT EXISTS VNW (id serial PRIMARY KEY, Job varchar, Benefits varchar,Description varchar,Requirement varchar,Skill varchar);")
# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
print(len(data))
for i in range(len(data)):
    cur.execute("INSERT INTO VNW (Job, Benefits,Description,Requirement,Skill) VALUES (%s, %s,%s, %s,%s)",
                (data[i]["Job"], data[i]["Benefits"], data[i]["Description"], data[i]["Requirement"], data[i]["Skill"]))
# Query the database and obtain data as Python objects
# cur.execute("SELECT * FROM VNW;")
# cur.fetchone()
# Make the changes to the database persistent
conn.commit()
# Close communication with the database
cur.close()
end=datetime.datetime.now()
total_time=end-start
print(total_time)




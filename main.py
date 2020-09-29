from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import numpy as np
import array as arr

url=['https://www.vietnamworks.com/viec-lam-internet-online-media-i57-vn',
     'https://www.vietnamworks.com/viec-lam-it-phan-mem-i35-vn',
     'https://www.vietnamworks.com/viec-lam-it-phan-cung-mang-i55-vn']
request_url=['https://jf8q26wwud-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser&x-algolia-application-id=JF8Q26WWUD&x-algolia-api-key=2bc790c0d4f44db9ab6267a597d17f1a',
             'https://jf8q26wwud-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser&x-algolia-application-id=JF8Q26WWUD&x-algolia-api-key=2bc790c0d4f44db9ab6267a597d17f1a',
             'https://jf8q26wwud-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser&x-algolia-application-id=JF8Q26WWUD&x-algolia-api-key=2bc790c0d4f44db9ab6267a597d17f1a'
             ]
from_data=['{"requests":[{"indexName":"vnw_job_v2_57","params":"query=&hitsPerPage=200&attributesToRetrieve=%5B%22*%22%2C%22-jobRequirement%22%2C%22-jobDescription%22%5D&attributesToHighlight=%5B%5D&query=&facetFilters=%5B%5B%22categoryIds%3A57%22%5D%2C%5B%22locationIds%3A29%22%5D%5D&filters=&numericFilters=%5B%5D&page=0&restrictSearchableAttributes=%5B%22jobTitle%22%2C%22skills%22%2C%22company%22%5D"}]}',
           '{"requests":[{"indexName":"vnw_job_v2_35","params":"query=&hitsPerPage=200&attributesToRetrieve=%5B%22*%22%2C%22-jobRequirement%22%2C%22-jobDescription%22%5D&attributesToHighlight=%5B%5D&query=&facetFilters=%5B%5B%22categoryIds%3A35%22%5D%2C%5B%22locationIds%3A29%22%5D%5D&filters=&numericFilters=%5B%5D&page=0&restrictSearchableAttributes=%5B%22jobTitle%22%2C%22skills%22%2C%22company%22%5D"}]}',
           '{"requests":[{"indexName":"vnw_job_v2_55","params":"query=&hitsPerPage=200&attributesToRetrieve=%5B%22*%22%2C%22-jobRequirement%22%2C%22-jobDescription%22%5D&attributesToHighlight=%5B%5D&query=&facetFilters=%5B%5B%22categoryIds%3A55%22%5D%2C%5B%22locationIds%3A29%22%5D%5D&filters=&numericFilters=%5B%5D&page=0&restrictSearchableAttributes=%5B%22jobTitle%22%2C%22skills%22%2C%22company%22%5D"}]}'
           ]
url_vnw='https://www.vietnamworks.com/senior-java-microservices-salary-up-to-2500-1-1298621-jv'



def csv_writer(data, path):
    """
    Write data to a CSV file path
    """
    with open(path, "wb") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for line in data:
            writer.writerow(line)



# Function crawling data per link
def crawl_data_content(url_vnw):
    response=requests.get(url_vnw)
    print(response)
    soup = BeautifulSoup(response.content, "html.parser")
    #print(soup)
    # job=[]
    # description=[]
    # requirement=[]
    # keywords=[]
    # skill=[]
    # career=[]
    # benefits=[]
    data = []
    title = soup.find("h1", class_="job-title").text
    benefits= soup.find("div", class_="benefits").text
    description= soup.find("div", class_="description").text
    skill = soup.findChildren('span', class_='content')[3].text
    requirement=soup.find("div", class_="requirements").text
    title=''.join(title.split())
    benefits = ''.join(benefits.split())
    description = ''.join(description.split())
    skill = ''.join(skill.split())
    requirement = ''.join(requirement.split())
    print('Job:_________________________________________________'+'\n'+ title)
    print('Benefits:____________________________________________'+'\n'+ benefits)
    print('description:_________________________________________'+'\n'+ description)
    print('requirement:_________________________________________'+'\n' +requirement)
    print('skill:_______________________________________________'+'\n'+skill)
    data.append({
        "Job":title,
        "Benefits":benefits,
        "Description":description,
        "Requirement":requirement,
        "Skill":skill
    })
    # df = pd.DataFrame(data=data)
    # df.to_csv("c:\\Users\\hieudv\\PycharmProjects\\Crawl_data_test\\vietnameworks.csv","w" ,header=True, index=True,encoding='utf-8')
    #print(data)
    return data
    #print(career)


#Function main crwal full data from filed
def crawl_data_vnw(request_url,from_data):
    response=requests.post(request_url,from_data)
    #print(response)
    soup = BeautifulSoup(response.content, "html.parser").text
    #print(soup)
    parsed_json = (json.loads(soup)) # parse a JSON string
    length_link=len(parsed_json['results'][0]['hits'])#
    print(length_link)
    k=0
    while k<length_link/40:
        alias = parsed_json['results'][0]['hits'][k]['alias']
        jobId = parsed_json['results'][0]['hits'][k]['jobId']
        link_works = 'https://www.vietnamworks.com/' + str(alias) + '-' + str(jobId) +'-' +'jv'
        print(link_works)
        crawl_data_content(link_works)
        k = k + 1
        print('Number Record :' + str(k))











#crawl_data_vnw(request_url[0],from_data[0])
crawl_data_content(url_vnw)








# -*- coding: UTF-8 -*-
import requests
import bs4
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from pyvirtualdisplay import Display
import time
import os

username = os.environ['DATABASE_USER']
password = os.environ['DATABASE_PASS']
post_url = os.environ['POST_URL']

display = Display(visible=0, size=(800, 600))
display.start()
driver = webdriver.Chrome(ChromeDriverManager().install())
driver.set_page_load_timeout(25)

def print_dataframe_information(df_event):
	print(df_event.info()) # Informations about DataFrame
	print(df_event) # DataFrame

def validate_name(event):
	name = event.find('div', class_="eds-is-hidden-accessible")
	name = name.text 
	name = name.upper()

	chars = "[](){}//\\*!-"
	for char in chars:
		name = name.replace(char, "")
	name = name.split()

	for word in name:
		if word == "TESTE":
			valid_name = False
		else:
			valid_name = True
	return valid_name

def get_links(url, actual_page):

	event_link = []

	url_pag = url + '/?page={}'.format(actual_page)
	driver.get(url_pag)

	#Search all events
	req = driver.page_source
	html_links = bs4.BeautifulSoup(req, features='html.parser')
	events = html_links.find_all('div', class_="search-event-card-wrapper")	

	for event in events:

		#Catch valid events
		if validate_name(event) == True:
			evento_link = event.find_all('a')
			for i in evento_link:
				 link = i.get('href')
				 event_link.append(link)
	event_link = list(set(event_link))
	return event_link

def test_request(url):
	res = requests.get(url)
	try:
		res.raise_for_status()
	except Exception as  exc:
		print('Um problema encontrado:%s'%(exc))

def get_number_pages(url):

	driver.get(url)

	req = driver.page_source
	html = bs4.BeautifulSoup(req, features='html.parser')
	number_pages = html.find('li', class_="eds-pagination__navigation-minimal eds-l-mar-hor-3").text
	number_pages = number_pages.split()
	number_pages = number_pages[2]

	return number_pages

def post_data(data):

	try:
		auth_post = requests.post(post_url + "/api/token/", {'username':username, 'password':password})

		auth_data = auth_post.json()
		access = auth_data['access']
		headers = {'Content-Type':'application/json', 'Authorization': 'Bearer {}'.format(access)}
	except:
		print('Token URL cannot be reached')

	# Post into database
	try:
		event = requests.post(post_url + '/event/', headers=headers, json=data)
	except:
		print('Event URL cannot be reached')


def search_link(url):
	driver.get(url)
	req = driver.page_source
	html = bs4.BeautifulSoup(req, features='html.parser')

	name = html.find('h1', class_='listing-hero-title').text
	name = name.split()
	name = ' '.join(name)

	producer = html.find('a', class_='btn btn--ico btn--target')
	if producer is not None:
		producer = producer.text
		producer = producer.split()
		producer = ' '.join(producer)
	else:
		producer = 'None'

	ticket = html.find('div', class_='js-display-price')
	if ticket is not None:
		ticket = ticket.text
		ticket = ticket.split()
		ticket = ' '.join(ticket)
	else:
		ticket = 'None'

	date = html.find('time', class_='listing-hero-date')
	if date is not None:
		date = date.attrs.get('datetime')
	else:
		date = 'None'

	location = html.find('p', class_='listing-map-card-street-address text-default')
	if location is not None:
		location = location.text
		location = location.split()
		location = ' '.join(location)
	else:
		location = 'None'

	return {'name': name, 'producer': producer, 'ticket': ticket, 'date': date, 'location': location}

def lambda_handler(event, context):

	city = event['body']['city']
	city = city.replace(' ', '-')

	initial_url = 'https://www.eventbrite.com.br/d/brazil--%s/all-events' % (city) 

	test_request(initial_url)
	max_page = get_number_pages(initial_url)

	page = 1
	while (page <= int(max_page)):

		name_list = []
		producer_list = []
		ticket_list = []
		date_list = []
		location_list = []

		links = get_links(initial_url , page)
		for link in links:
			result = search_link(link)
			name_list.append(result['name'])
			producer_list.append(result['producer'])
			ticket_list.append(result['ticket'])
			date_list.append(result['date'])
			location_list.append(result['location'])

		df_event = pd.DataFrame(
			{'name': name_list,
			'producer': producer_list,
			'ticket': ticket_list,
			'date': date_list,
			'local': location_list,
			'provider_resource': links})

		res_event = df_event.to_json(orient='records', force_ascii=False)
		res_event = res_event.replace("\/", "/")

		print_dataframe_information(df_event)
		post_data(res_event)

		page += 1

	driver.close()
	display.stop()

	return {
		'statusCode': 200,
		'headers': {
			'Content-Type': 'application/json',
			'Access-Control-Allow-Origin': '*'
			},
		'isBase64Encoded': False
		}


lambda_handler({
    "body": {
        "city": "belo horizonte"
    }
}, None)

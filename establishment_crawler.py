# -*- coding: UTF-8 -*-
import requests
import pandas as pd
import os
import json

api_key = os.environ['API_KEY']
username = os.environ['DATABASE_USER']
password = os.environ['DATABASE_PASS']
post_url = os.environ['POST_URL']

id_list = []

def post_action(url, headers, json):
	try:
		post = requests.post(post_url + '/' + url + '/', headers=headers, json=res_place)
	except:
		print(url + ' URL cannot be reached')

def transform_json(dataframe):
	response = dataframe.to_json(orient='records', force_ascii=False)
	response = response.replace("\/", "/")
	return response

def print_dataframe_information(dataframe):
	print(dataframe.info()) # Informations about DataFrame
	print(dataframe) # DataFrame

def get_data(url, type_search):
	
	# Get response in Places API
	res = requests.get(url)
	main_data = res.json()

	place_id_list = []

	names_list = []
	rating_list = []
	google_id_list = []

	latitude_list = []
	longitude_list = []
	route_list = []
	street_number_list = []
	neighborhood_list = []
	city_list = []
	country_list = []
	state_list = []
	zip_code_list = []
	google_place_id = []

	phone_list = []


	# Get place id
	for data in main_data['results']:
		place_id = data['place_id']
		place_id_list.append(place_id)

	# Place details
	for plc_id in place_id_list:
		detail_url = 'https://maps.googleapis.com/maps/api/place/details/json?key=%s&place_id=%s' % (api_key, plc_id)
		res = requests.get(detail_url)
		data = res.json()

		# Places info
		name = data['result']['name']
		if 'rating' in data['result']:
			rating = data['result']['rating']
		else: 
			rating = 0

		google_id = data['result']['id']
		names_list.append(name)
		rating_list.append(rating)
		google_id_list.append(google_id)
		id_list.append(google_id)
		google_place_id.append(plc_id)

		# Address info
		latitude = data['result']['geometry']['location']['lat']
		latitude = '{}'.format(latitude)
		longitude = data['result']['geometry']['location']['lng']
		longitude = '{}'.format(longitude)

		street_number = ''
		route = ''
		neighborhood = ''
		city = ''
		state = ''
		country = ''
		zip_code = ''

		for address in data['result']['address_components']:
			if 'street_number' in address['types']:
				street_number = address['long_name']

			if 'route' in address['types']:
				route = address['long_name']

			if 'sublocality_level_1' in address['types']:
				neighborhood = address['long_name']

			if 'administrative_area_level_2' in address['types']:
				city = address['long_name']

			if 'administrative_area_level_1' in address['types']:
				state = address['long_name']

			if 'country' in address['types']:
				country = address['long_name']

			if 'postal_code' in address['types']:
				zip_code = address['long_name']

		latitude_list.append(latitude)
		longitude_list.append(longitude)
		street_number_list.append(street_number)
		route_list.append(route)
		neighborhood_list.append(neighborhood)
		city_list.append(city)
		state_list.append(state)
		country_list.append(country)
		zip_code_list.append(zip_code)

		# Contact info
		if 'international_phone_number' in data['result']:
			phone = data['result']['international_phone_number']
			phone_list.append(phone)
		else:
			phone_list.append('None')

	df_place = pd.DataFrame(
		{
		'name':names_list,
		'google_id':google_id_list,
		'rating':rating_list
		})

	df_address = pd.DataFrame(
		{
		'google_place_id': google_place_id,
		'google_id':google_id_list,
		'street': route_list,
		'number': street_number_list,
		'neighborhood': neighborhood_list,
		'zip_code': zip_code_list,
		'city': city_list,
		'state': state_list,
		'country': country_list,
		'latitude': latitude_list,
		'longitude': longitude_list
		})

	df_contact = pd.DataFrame(
		{
		'google_id': google_id_list,
		'phone': phone_list
		})

	res_place = transform_json(df_place)

	res_address = transform_json(df_address)

	res_contact = transform_json(df_contact)

	# Uncomment those lines below for printing Dataframe information and content
	print_dataframe_information(df_place)
	print_dataframe_information(df_address)
	print_dataframe_information(df_contact)

	# Get access
	try:
		auth_post = requests.post(post_url + "/api/token/", {'username':username, 'password':password})
	
		auth_data = auth_post.json()
		access = auth_data['access']
		headers = {'Content-Type':'application/json', 'Authorization': 'Bearer {}'.format(access)}
	except:
		print('Token URL cannot be reached')

	# Post into database
	post_action(place, headers, res_place)
	post_action(address, headers, res_address)
	post_action(contact, headers, res_contact)				

	# Go to next page if exists
	if ('next_page_token' in main_data):
		next_page = main_data['next_page_token']
		url = 'https://maps.googleapis.com/maps/api/place/%s/json?key=%s&pagetoken=%s' %(type_search, api_key, next_page)
		get_data(url, type_search)
	else:
		print('All pages loaded and posted')

def lambda_handler(event, context):

	param_body = json.loads(event['body'])

	param_type = ''
	param_query = ''
	param_location = ''
	param_radius = ''
	type_search = 'nearbysearch'	

	if ('location' in param_body):
		if (param_body['location'] != '' or param_body['location'] is not None):
			param_location = '&location=%s' % param_body['location']

	if ('radius' in param_body):
		if (param_body['radius'] != '' or param_body['radius'] is not None):
			param_radius = '&radius=%s' % param_body['radius']


	if ('type' in param_body):
		if (param_body['type'] != '' or param_body['type'] is not None):
			param_type = '&type=%s' % param_body['type']

	if ('query' in param_body):
		if (param_body['query'] != '' or param_body['query'] is not None):
			param_query = '&query=%s' % param_body['query']
			param_query = param_query.replace(' ', '+')
			type_search = 'textsearch'

	parameters = param_query + param_location + param_radius + param_type

	url = 'https://maps.googleapis.com/maps/api/place/%s/json?key=%s%s' % (type_search, api_key, parameters)
	get_data(url, type_search)

	return {
		'statusCode': 200,
		'headers': {
			'Content-Type': 'application/json',
			'Access-Control-Allow-Origin': '*'
			},
		'body': json.dumps(
			{
			'event' : 'location' in event['body'],
			'return_ids': id_list
			}),
		'isBase64Encoded': False
		}

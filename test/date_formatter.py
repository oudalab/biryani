def date_formatter(raw_date):
	date_split=raw_date.split(' ')
	month=month_to_num(date_split[0].upper())
	day=date_split[1][:-1]
	year=date_split[2]
	return year+month+day
	
	
	
def month_to_num(month):
	switcher={
		'JANUARY':'01',
		'FEBRUARY':'02',
		'MARCH':'03',
		'APRIL':'04',
		'MAY':'05',
		'JUNE':'06',
		'JULY':'07',
		'AUGUST':'08',
		'SEPTEMBER':'09',
		'OCTOBER':'10',
		'NOVEMBER':'11',
		'DECEMBER':'12'

	}
	return switcher.get(month,'01')	

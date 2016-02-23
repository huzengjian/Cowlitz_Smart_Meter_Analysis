import numpy as np
import pandas as pd
import copy,sys
import datetime
#from AnomalyDetector import AnomalyDetector

AnomalyMultiplier = 5
IrrelevantColumns = ['REGISTER_LOC', 'STATUS_DESC','INSERT_DT']
DaysInMonth = {'january':31, 'feburary':28, 'march':31, 'april':30, 'may':31, 'june':30, 'july':31, 'august':31, 'september':30, 'october':31, 'november':30, 'december':31}

class SmartMeterLib(object):
	def __init__(self, url, month):
		self.monthly_meter_data = pd.read_csv(url)
		self.month= month
		relevant_columns = set(self.monthly_meter_data.columns) - set(IrrelevantColumns)
		self.monthly_meter_data = self.monthly_meter_data[list(relevant_columns)]
		
		#print "Converting original_dt to datatime format..."
		#self.monthly_meter_data.ORIGINAL_DT = pd.to_datetime(self.monthly_meter_data.ORIGINAL_DT) 
	
		# filter all estimated readings and accepted readings
		#self.monthly_meter_data = self.monthly_meter_data[self.monthly_meter_data.STATUS == 1]
		
		#self.monthly_meter_data.describe()

		def mask(df, key, value):
			return df[df[key] == value]
		pd.DataFrame.mask = mask
		
		self.meter_groups = self.monthly_meter_data.groupby(['METER_NO','REGISTER_TP'])
		#self.meter_groups.apply(lambda x: x.sort_index(by='ORIGINAL_DT', ascending=True, inplace = True))

	def get_all_unique_meter_nos(self, location_no = '', rate_class = '', account_class = ''):
		df = self.monthly_meter_data
		if(location_no != ''):
			df = df.mask('LOCATION_NO', location_no)
		if(account_class != ''):
			df = df.mask('ACCOUNT_CLASS_DEFAULT', account_class)
		if(rate_class != ''):
			df = df.mask('RATE', rate_class)
		
		return df.METER_NO.unique().tolist()
		
	def get_all_unique_location_no(self):
		return self.monthly_meter_data.LOCATION_NO.unique().tolist()

	def get_all_unique_rate_class(self):
		return self.monthly_meter_data.RATE.unique().tolist()
		
	def get_all_unique_account_class(self):
		return self.monthly_meter_data.ACCOUNT_CLASS_DEFAULT.unique().tolist()

	def get_all_meter_properties(self):
		self.monthly_meter_data.groupby('METER_NO').first()
	
	# Get the last reading of the given meter and register type
	def get_last_reading(self, meter_no, register_tp = 'KWH'):
		key = (meter_no,register_tp)
		if (key not in self.meter_groups.groups):
			return -1
		meter_data = self.meter_groups.get_group(key)
		#meter_data.ORIGINAL_DT = pd.to_datetime(meter_data.ORIGINAL_DT)
		
		return meter_data.READ_AMT[meter_data['ORIGINAL_DT'].idxmax()]
		#meter_data = meter_data.sort_index(by=['ORIGINAL_DT'], ascending=True)
		#return meter_data.READ_AMT.tolist()[-1]
		
	# Get meter monthly usage, by month or by day
	def get_meter_usage(self, meter_no, meter_usage_caption = 'METER_USAGE', meter_alert_caption = 'METER_ALERT', start_reading = -1, register_tp = 'KWH'):
		key = (meter_no,register_tp)
		if (key not in self.meter_groups.groups):
			print key
			return pd.DataFrame()
		meter_data = copy.deepcopy(self.meter_groups.get_group(key))
		if(len(meter_data.index) == 0):
			return pd.DataFrame()
		ctpt_multiplier = meter_data.CTPT_MULTIPLIER.max()
		dial = meter_data.DIAL.max()
		if dial == 0: # some meters don't have dial info. set to 99999 instead
			dial = 99999
		
		meter_data.ORIGINAL_DT = pd.to_datetime(meter_data.ORIGINAL_DT)
		meter_data = meter_data.sort_index(by=['ORIGINAL_DT'], ascending=True)
		meter_data['DAY'] = meter_data.ORIGINAL_DT.apply(lambda x: x.day)
		meter_data = meter_data.groupby(['DAY'],as_index=False).first() # remove duplicates
		if(start_reading == -1): # not given, use the first reading of the month instead
			start_reading = meter_data.READ_AMT.iloc[0]
		
		meter_data[meter_alert_caption] = ''
		meter_data['READ_AMT_ADJUSTED'] = meter_data['READ_AMT']
		if(start_reading > dial):
			start_reading = start_reading - (dial + 1)
		if(np.any(meter_data[meter_data.READ_AMT > dial])): # for those with extra '1'
			meter_data.READ_AMT_ADJUSTED[meter_data.READ_AMT > dial + 1] = meter_data.READ_AMT[meter_data.READ_AMT > dial + 1] - (dial + 1)
			meter_data[meter_alert_caption][meter_data.READ_AMT > dial + 1] = 'truncated'
		
		meter_data[meter_usage_caption] = np.diff(pd.Series(start_reading).append(meter_data['READ_AMT_ADJUSTED']))

		rollover_threshold = - dial * 9 / 10
		idx = meter_data[meter_data[meter_usage_caption] < rollover_threshold].index
		if(len(idx) > 0): 		# very likely to be roll-over
			print 'rollover'
			meter_data.set_value(idx, meter_alert_caption, 'rollover')
			meter_data.set_value(idx, meter_usage_caption, meter_data[meter_usage_caption][idx] + dial + 1)

		#print meter_data[meter_usage_caption]
		if(np.any(meter_data[meter_usage_caption] < 0)):
			print 'suspicious - meter_no: ', meter_no
			print 'start_reading = ', start_reading
			total_monthly_consumption = meter_data['READ_AMT_ADJUSTED'].tolist()[-1] - start_reading
			if(total_monthly_consumption < 0):
				total_monthly_consumption = total_monthly_consumption + dial + 1
			avg_daily_usage = total_monthly_consumption / 30
			if(avg_daily_usage > 0):
				while(True):
					#sys.stdin.read(1)
					meter_data[meter_usage_caption] = np.diff(pd.Series(start_reading).append(meter_data['READ_AMT_ADJUSTED']))
					#print meter_data['READ_AMT_ADJUSTED']
					meter_data['DAILY_USAGE_AVG'] = meter_data[meter_usage_caption] / (np.diff(pd.Series(0).append(meter_data['DAY'])))
					#print meter_data.DAILY_USAGE_AVG
					if(not np.any(meter_data[meter_usage_caption] < 0)): #success
						meter_data.drop('DAILY_USAGE_AVG', axis=1, inplace=True)
						break
					idx_neg = meter_data.DAILY_USAGE_AVG.idxmin() # get the negative usage
					idx_max = meter_data.DAILY_USAGE_AVG.idxmax()
					if(meter_data.DAILY_USAGE_AVG.max() < AnomalyMultiplier * avg_daily_usage): # if no clear envidence, try 
						pos = meter_data.index.get_loc(idx_neg)
						if(pos <= len(meter_data.index)/2):
							# usage < 0. e.g., 15, 2,3,4,5. (15 is abnormal). replace 15 by 2.
							reading_current_day = meter_data['READ_AMT_ADJUSTED'].iloc[pos]
							if(pos > 0):
								meter_data.set_value(meter_data.index[pos-1], meter_alert_caption, 'suspicious')
								meter_data.set_value(meter_data.index[pos-1], 'READ_AMT_ADJUSTED', reading_current_day)
							else:
								start_reading = reading_current_day
						else:
							# 100,101,104,106,1. here 1 is abnormal. replace 1 by 106.
							reading_prev_day = meter_data['READ_AMT_ADJUSTED'].iloc[pos-1]
							meter_data.set_value(meter_data.index[pos], meter_alert_caption, 'suspicious')
							meter_data.set_value(meter_data.index[pos], 'READ_AMT_ADJUSTED', reading_prev_day)
					else: # pretty clear that the negative consumption is due to some abnormal readings. fix them all in one batch.
						# 100, 9999,101,104,105. here 9999 is abnormal. replace it by 100. or 1000, 4, 5,6, 1002, 1004. here 4,5,6 are abnormal. replace them by 1000.
						idx_max = meter_data.DAILY_USAGE_AVG.idxmax()
						start = meter_data.index.get_loc(idx_max)
						end = meter_data.index.get_loc(idx_neg)
						if(start > end):
							start, end = end, start
						reading_prev_day = meter_data['READ_AMT_ADJUSTED'].iloc[start - 1] if start > 0 else start_reading
						meter_data.set_value(meter_data.index[start:end], meter_alert_caption, 'suspicious')
						meter_data.set_value(meter_data.index[start:end], 'READ_AMT_ADJUSTED', reading_prev_day)
		
		#meter_usage = [(x + dial + 1) if x < 0 else x for x in meter_usage]
		#meter_data[meter_usage_caption] = pd.Series(meter_usage, index = meter_data.index)
		#meter_data = AnomalyDetector.GuassianDetector(df = meter_data, feat_cols = [meter_usage_caption], epsilon = 1e-7, meter_alert_caption = meter_alert_caption)
		#return meter_data
		
		"""
		meter_alert = [('rollover' if x + dial+1 < threshold and x + dial + 1 >= 0 else 'suspicious') 
						if x < 0 
						else ('' if x < threshold  else 'suspicious') for x in meter_usage]
		meter_usage = [(x + dial + 1 if x + dial + 1 < threshold and x + dial + 1 >= 0 else 0) 
						if x < 0 
						else (x if x < threshold else 0) for x in meter_usage]
		
		meter_data[meter_usage_caption] = pd.Series(meter_usage, index = meter_data.index)
		meter_data[meter_alert_caption] = pd.Series(meter_alert, index = meter_data.index)
		"""

		# apply multiplier
		meter_data[meter_usage_caption] = meter_data[meter_usage_caption] * ctpt_multiplier
		return meter_data

		#return pd.DataFrame(meter_usage, index = pd.DatetimeIndex(meter_data.READ_DT).day, columns = [meter_no])
		
	# Get daily meter usage for all meters within the given rate class
	def get_total_daily_meter_usage_within_rate_class(self, rclass):
		total_meter_usage = pd.DataFrame()
		meter_no_pool = self.monthly_meter_data[self.monthly_meter_data.LOCATION_NO == rclass].METER_NO.unique()
		for meter_no in meter_no_pool:
			meter_usage = get_meter_usage(meter_no, False)
			total_meter_usage = pd.concat([total_meter_usage, meter_usage], axis = 1)
		return total_meter_usage
		
	

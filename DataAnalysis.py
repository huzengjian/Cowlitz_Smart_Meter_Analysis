import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime,time,os,sys,ast
from SmartMeterLib import SmartMeterLib
from joblib import Parallel, delayed  
import multiprocessing

dir = "data\\"
result_dir = dir + "result\\"
location_file_name = "consumption_by_location.csv"
meter_result_file_name = "meter_result.csv"
rate_account_result_file_name = "consumption_by_account_class.csv"
consumption_by_meter_file_name = "consumption_by_meter.csv"
consumption_by_location_file_name = "consumption_by_location.csv"
#meter_nos = [572200,572743,574196,573860,550106,573129]
#meter_nos = [200790,151651,134523,132613,134860,206251,206009]
#meter_nos = [550106]
meter_nos = [573903]
#meter_nos = [206135]




def str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")
  
def plotDataFrame(df):
	ax = df.plot(kind='bar')
	ax.set_xlabel(df.columns[0])
	ax.set_ylabel(df.columns[1])
	ax.set_xticklabels(df.ix[:,0].tolist())
	plt.gcf().autofmt_xdate() 
	plt.show()

# Write list of data to csv	
def export_list_to_disk(list_meter_data, file_path):
	if(len(list_meter_data) > 0):
		df = pd.concat(list_meter_data)
		export_df_to_disk(df, file_path)

#Write data frame to csv
def export_df_to_disk(df, file_path):
	print 'Writing meter result to {0}...'.format(file_path)
	df.to_csv(file_path, index = False)

# plot the total meter usage for a given rate class
def plot_total_daily_meter_usage_within_rate_class(total_meter_usage, rclass):
	print 'Total # of meters: ' + str(len(total_meter_usage.columns))
	total_meter_usage = total_meter_usage.transpose()
	aggregated_total_meter_usage = total_meter_usage.sum()
	print 'Total Consumption: ' + str(aggregated_total_meter_usage.sum())
	aggregated_total_meter_usage.plot(kind = 'bar')
	plt.xlabel('Day')
	plt.ylabel('Daily Consumption')
	plt.title(rclass)
	plt.show()

def add_to_dict(dict, key, value):
	if(key in dict):
		dict[key] += value
	else:
		dict[key] = value


def analyze(year_months, register_tps, plot):
	if(not os.path.exists(result_dir)):
		os.mkdir(result_dir)

	num_cores = multiprocessing.cpu_count()
	#print "Found {0} cores.".format(num_cores)

	last_Month_Data = dict()
	for year, month in year_months:
		t0 = time.time()
		for rtp in register_tps:
			processed = 0
			df_meter_data = pd.DataFrame()

			prefix = '{0}_{1}'.format(year, month)
			url = '{0}raw\\{1}.csv'.format(dir, prefix)
			current_Month_Data = SmartMeterLib(url,month)
			
			list_meter_data = []
			list_suspicious_data = []
			list_rollover_data = []
			meter_nos = current_Month_Data.get_all_unique_meter_nos()
			print "For {0} {1}, found {2} meter records.".format(month, year, len(meter_nos))

			#Parallel(n_jobs=2)(delayed(process_meter_info)(meter_no) for meter_no in meter_nos)
			
			for meter_no in meter_nos:
				processed = processed + 1
				key = (meter_no, rtp)
				start_reading = last_Month_Data[key] if key in last_Month_Data else -1
				if(start_reading != -1):
					print "last month's last reading = {0}".format(start_reading)
				meter_data = current_Month_Data.get_meter_usage(meter_no = meter_no, start_reading = start_reading, register_tp = rtp)
				if(len(meter_data.index) > 0):
					#rate = meter_data.RATE.unique()[0]
					#account_class = meter_data.ACCOUNT_CLASS_DEFAULT.unique()[0]
					#location = meter_data.LOCATION_NO.unique()[0]
					meter_monthly_usage = meter_data.METER_USAGE.sum()
					
					print 'Step {0} - {4} usage of meter {1} is {2}, time elapsed = {3}'.format(processed, meter_no, meter_monthly_usage, time.time() - t0, month)
					
					meter_alert = meter_data.METER_ALERT.tolist()
					alert = 'suspicious' in meter_alert or 'truncated' in meter_alert
					rollover = 'rollover' in meter_alert
					
					if(alert):
						list_suspicious_data.append(meter_data)
					if(rollover):
						list_rollover_data.append(meter_data)
					list_meter_data.append(meter_data)
					last_Month_Data[key] = meter_data.READ_AMT[meter_data['ORIGINAL_DT'].idxmax()]

			print "Collecting results..."
			
			df_meter_data = pd.concat(list_meter_data)
			exp_prefix = "{0}_{1}".format(prefix, rtp)
			export_df_to_disk(df_meter_data, '{0}{1}_{2}'.format(result_dir, exp_prefix, meter_result_file_name))
			export_list_to_disk(list_suspicious_data, '{0}suspicious_{1}_{2}'.format(result_dir, exp_prefix, meter_result_file_name))
			export_list_to_disk(list_rollover_data, '{0}rollover_{1}_{2}'.format(result_dir, exp_prefix, meter_result_file_name))

			rate_account_result = df_meter_data[['RATE','ACCOUNT_CLASS_DEFAULT','METER_USAGE']].groupby(['ACCOUNT_CLASS_DEFAULT','RATE']).sum().reset_index()
			rate_account_result_file_path = '{0}{1}_{2}'.format(result_dir, exp_prefix, rate_account_result_file_name)
			print 'Writing aggregated rate class/account class result to {0}...'.format(rate_account_result_file_path)
			rate_account_result.to_csv(rate_account_result_file_path, index = False)

			account_class_result = rate_account_result.groupby(['ACCOUNT_CLASS_DEFAULT']).sum().reset_index()
			rate_class_result = rate_account_result.groupby(['RATE']).sum().reset_index()
			total_meter_usage = rate_class_result['METER_USAGE'].sum()
			
			print "\n=====Final Stats for {0} {1}======".format(month, year)
			print "Total Meter Usage ({0}) = {1}".format(rtp, total_meter_usage)
			print "Count of Roll Overs = ", len(list_rollover_data)
			print "Count of Suspicious records = ", len(list_suspicious_data)
			print "Elapsed time: %f (s)" % (time.time() - t0)
			print "====================================\n"
			
			print "\nBreak down by account class:\n====================================\n", account_class_result
			print "\nBreak down by rate class:\n====================================\n", rate_class_result

			if(plot == True):
				plotDataFrame(account_class_result)
				plotDataFrame(rate_class_result)
		
def main(argv):
	if len(argv) < 3:
		print '\nSorry - invalid parameters.\nUsage:\n==========Params=========\nYear_Months \n Register_Tps \n Plot \n====================='
		return
	year_months = ast.literal_eval(sys.argv[1])
	register_tps = ast.literal_eval(sys.argv[2])
	plot = str2bool(argv[3])
	print '\n==========Params=========\nYear, Months = {0}\nRegister_Tps = {1} \nPlot = {2} \n=========================\n'.format(year_months, register_tps, plot)
	analyze(year_months, register_tps, plot)

if __name__ == "__main__":
    main(sys.argv)
	

"""
# Get usage of all meters in given location
#location_no = 20497
#location_no = 54601
#meter_nos = ng_data[ng_data['LOCATION_NO'] == location_no].METER_NO.unique()
#for meter_no in meter_nos :
    #meter_usage = smartMeterlib.get_meter_usage(meter_no)
    #print meter_usage.sum()
    #smartMeterlib.plot_daily_meter_usage(meter_usage, meter_no)

# Plot for each rate class
#for rclass in ng_data.LOCATION_NO.unique():
#rclass = 'SIND'
#total_meter_usage = smartMeterlib.get_total_daily_meter_usage_within_rate_class(rclass)
#smartMeterlib.plot_total_daily_meter_usage_within_rate_class(total_meter_usage, rclass)
#print total_meter_usage.iloc[:,np.arange(5)]



#		for location_no in smartMeterlib.get_all_unique_location_no():
#			loc_usage = 0
#			meter_nos = smartMeterlib.get_all_unique_meter_nos(location_no = location_no)
#			print "Analyzing all meters at LOCATION_NO {0} : {1}...".format(location_no, meter_nos)

#		consumption_by_account_class = dict()
#		consumption_by_rate_class = dict()
#		consumption_by_meter = dict()
#		consumption_by_location = dict()
		#add_to_dict(consumption_by_account_class, account_class, meter_monthly_usage)
		#add_to_dict(consumption_by_rate_class, rate, meter_monthly_usage)
		#add_to_dict(consumption_by_location, location, meter_monthly_usage)

		consumption_by_rate_class_df = pd.DataFrame(list(consumption_by_rate_class.items()), columns = ['Rate', 'Consumption(KWH)'])
		consumption_by_account_class_df = pd.DataFrame(list(consumption_by_account_class.items()), columns = ['Account Class', 'Consumption(KWH)'])
		consumption_by_meter_df = pd.DataFrame(list(consumption_by_meter.items()), columns = ['METER_NO', 'Consumption(KWH)'])
		consumption_by_location_df = pd.DataFrame(list(consumption_by_location.items()), columns = ['LOCATION_NO', 'Consumption(KWH)'])
		
		consumption_by_rate_class_df.to_csv('{0}{1}_{2}'.format(result_dir, prefix, consumption_by_rate_file_name), index = False)
		consumption_by_account_class_df.to_csv('{0}{1}_{2}'.format(result_dir, prefix, consumption_by_account_class_file_name), index = False)
		consumption_by_meter_df.to_csv('{0}{1}_{2}'.format(result_dir, prefix, consumption_by_meter_file_name), index = False)
		consumption_by_location_df.to_csv('{0}{1}_{2}'.format(result_dir, prefix, consumption_by_location_file_name), index = False)
		
# plot the meter_daily_usage
def plot_daily_meter_usage(meter_data, meter_no):
	#print meter_daily_usage
	#meter_daily_usage.plot(kind='bar')
	plt.plot(pd.to_datetime(meter_data.READ_DT), meter_data.METER_USAGE)
	plt.xlabel('Day')
	plt.ylabel('Usage')
	plt.title('Meter ' + str(meter_no))
	plt.gcf().autofmt_xdate() 
	plt.show()
"""

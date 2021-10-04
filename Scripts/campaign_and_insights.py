#!/usr/bin/python
# -*- coding: utf-8 -*-

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.ad import Ad
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import datetime
import time
import requests
import json
import os,sys
from utils.utils import log_handler
from json import load
from email_alert import file_creation,notification

cur_date= datetime.datetime.now()
dt = cur_date.strftime("%Y%m%d")
path=os.getcwd()
file_path= path+"/Data/"
start_time = datetime.datetime.now()

#change the value as per you requirment, i have passed date for automation perspective
dt=datetime.datetime.strptime((datetime.datetime.now()- datetime.timedelta(2)).strftime("%Y-%m-%d"),"%Y-%m-%d").date()
execution_date=(datetime.datetime.now()- datetime.timedelta(2)).strftime("%Y%m%d")

print(dt)
try:
    if os.path.exists(file_path):
        pass
    else:
        os.mkdir(file_path)
except OSError:
    print ("Creation of the directory %s failed" % path)


class FaceBookAds:
    def __init__(self):
        self.logger = log_handler(__name__ + '.' + self.__class__.__name__)
        self.f=file_creation()  #calling email function here to addfailure logs into a failure file
        try:
                with open(path+'/resources/FB_Cred.json', 'r') as f:
                    self.logger.info('*************************')
                    self.logger.info("Facebook Grpah API for campaigns execution has been strated at :"+str(cur_date) +"\n")
                    cred = load(f)
                    self.app_id = cred['app_id']
                    self.app_secret = cred['app_secret']
                    self.ad_account_id = cred['ad_account_id']
                    self.access_token = cred['access_token']
                self.logger.info('Facebook Ads Credentails has been loaded'+" \n")
            
        except Exception as e:
                self.logger.exception("Error in loading FaceBook Ads Credentails file")

    def connection(self,ad_account_id): #added argument where we have multiple ads account
        try:
            self.logger.info('Connecting to Facebook AdAccount')
            FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
            my_account = AdAccount(ad_account_id)
            return my_account
            
        except Exception as e:
                self.logger.exception("Error in connecting AdAccount Please check access_token value")

    #added Bigquery Connect part where user can push the data into BQ using service account creds
    def gbq_conn(self):
        try:
            self.logger.info('Connecting to Google BigQuery Account')
            gbq_account = service_account.Credentials.from_service_account_file(path+'/resources/GBQ_Cred.json')
            project_id = '<gcp_project>'
            client = bigquery.Client(credentials= gbq_account,project=project_id)
            return client
            
        except Exception as e:
                self.logger.exception("Error in connecting google bigquery, Please check access")

    def get_campaign_overview(self):
        '''Here we will fetch all campaigns associated with multiple ad_Account id, i have passed effective_status_enum value is ACTIVE only on this code.
        Also values passed in params variable can be changed as per requirement.
        I have passed yesterday day in time range to fetch one day data but this value can be changed as per requirement. 
        '''
        try:
            self.logger.info('Getting Campaign Details From FaceBook Ads Account')
            results=[]
            for ad_id in self.ad_account_id:

                my_account=self.connection(ad_id)
                fields = ['account_id','boosted_object_id','budget_remaining','buying_type','can_use_spend_cap','configured_status','created_time','daily_budget','effective_status','id','lifetime_budget','name ','objective','source_campaign_id','spend_cap','start_time','status','stop_time','topline_id','updated_time']
                params = {
                'effective_status_enum': ['ACTIVE'],
                'limit':'2000',
                'Content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
                'time_range':{'since': str(dt), 'until': str(dt)}
                }
                campaigns_raw=my_account.get_campaigns(params=params,fields=fields)
                for campaign in range(len(campaigns_raw)):
                    data = dict(campaigns_raw[campaign])
                    results.append(data)
            campaign_df= pd.DataFrame(results)
            campaign_df.to_csv (file_path+'campaign_df_'+str(dt)+'.csv',encoding="utf-8-sig",index=False)
            self.logger.info('Campaign Details have been fetched successfully')
            return campaign_df
        except Exception as e:
            self.logger.exception("Error in getting campaign details From FaceBook Ads Account")
            self.f.write(str(e)) #adding failure logs 



    def get_campaign_insights(self):
        '''Here we will fetch all campaigns insights data associated with above ad_Account id's at ad level for active campaign id's only.
        Also values passed in params variable can be changed as per requirement.
        I have passed yesterday day in timerange to fetch one day data but this value can be changed as per requirement.
        '''
        try:
            results=[]
            for ad_id in self.ad_account_id:
                my_account=self.connection(ad_id) #calling connection method here
                gbq_client=self.gbq_conn()  #calling bigquery connection method here

                self.logger.info('Execution to fetch Campaign insights data have been started')

                #reading csv file created for all campaigns for multiple ads account
                df = pd.read_csv(file_path+'campaign_df_'+str(dt)+'.csv')
                df=df[df.account_id == int(ad_id.replace('act_',''))] #replacing act_ with blank to fetch only calling ad account id here

                df['created_time'] = pd.to_datetime(df.created_time, utc=True).dt.date #changing created time column value to date to get active or last 90days creaetd campaigns
                base_date=datetime.datetime.strptime((datetime.date.today().replace(day=1) - datetime.timedelta(days=90)).strftime("%Y-%m-%d") ,"%Y-%m-%d").date()
                data = df[(df.effective_status == "ACTIVE") | (df['created_time'] >=base_date ) ]

                #defining fields and params
                fields = ['account_currency','account_id','account_name','action_values','actions','ad_id','ad_name','adset_id','adset_name','attribution_setting','buying_type','campaign_id','campaign_name','canvas_avg_view_percent','canvas_avg_view_time','catalog_segment_value','clicks','conversion_rate_ranking','conversion_values','conversions','converted_product_quantity','converted_product_value','cost_per_action_type','cost_per_conversion','cost_per_estimated_ad_recallers','cost_per_inline_link_click','cost_per_inline_post_engagement','cost_per_outbound_click','cost_per_thruplay','cost_per_unique_action_type','cost_per_unique_click','cost_per_unique_inline_link_click','cost_per_unique_outbound_click','cpc','cpm','cpp','ctr','date_start','date_stop','engagement_rate_ranking','estimated_ad_recall_rate','estimated_ad_recallers','frequency','full_view_impressions','full_view_reach','impressions','inline_link_click_ctr','inline_link_clicks','inline_post_engagement','instant_experience_clicks_to_open','instant_experience_clicks_to_start','mobile_app_purchase_roas','objective','optimization_goal','outbound_clicks','outbound_clicks_ctr','place_page_name','purchase_roas','qualifying_question_qualify_answer_rate','quality_ranking','reach','social_spend','spend','unique_actions','unique_clicks','unique_ctr','unique_inline_link_click_ctr','unique_inline_link_clicks','unique_link_clicks_ctr','unique_outbound_clicks','unique_outbound_clicks_ctr','video_30_sec_watched_actions','video_avg_time_watched_actions','video_p100_watched_actions','video_p25_watched_actions','video_p50_watched_actions','video_p75_watched_actions','video_p95_watched_actions','video_play_actions','video_play_curve_actions','website_ctr','website_purchase_roas']
                params = {
                    'level':'ad',
                    'limit': '1000',
                    'time_range':{'since': str(dt), 'until': str(dt)},
                }
                count = 0
                for i in data['id']:
                    campaign = Campaign(i)
                    insights = campaign.get_insights(fields=fields,params=params)
                    for item in insights:
                        new_dict={}
                        data = dict(item)
                        for key,value in data.items():    
                            if type(value) is list:
                                for sub in value:
                                    new_dict[key+"_"+sub['action_type']]=sub['value']
                            else:
                                new_dict[key]=value
                        results.append(new_dict)
                        count +=1
                        print("Campaign id- " +str(i)+" data have been fetched successfully and counter number is:- "+ str(count))
                        time.sleep(3)  #sleep for 3-5 seconds otherwise api calls will be more than 200 in single run and error will come
            campaign_ads_df= pd.DataFrame(results)
            campaign_ads_df['extractionDate'] = dt
            campaign_ads_df.columns = campaign_ads_df.columns.str.replace('.', '_',regex=True)

            #drop records in case of true duplicates 

            campaign_ads_df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
            campaign_ads_df.to_csv (file_path+'campaign_insight_df_'+str(dt)+'.csv',encoding="utf-8-sig",index=False)
            self.logger.info('Total '+str(len(campaign_ads_df))+' active campaign insights data have been fetched successfully')

            # #Pushing data into GCP BigQuery, if not needed please comment it out

            table_id='<gcp_project>.<dataset_name>.<table_name>'         
            job = gbq_client.load_table_from_dataframe(campaign_ads_df, table_id)  # Make an API request.
            job.result()
            table = gbq_client.get_table(table_id)  # Make an API request.
            self.logger.info("Loaded {} rows and {} columns to {}".format( table.num_rows, len(table.schema), table_id))
            self.logger.info('Campaign insights data have been pushed into bigquery successfully')
        except Exception as e:
            self.logger.exception("Error in getting campaign details From  FaceBook Ads Account")
            self.f.write(str(e))


    def get_campaign_ads_insights(self):
        '''Here we will fetch all campaigns ads insights data associated with a ad_Account id at ad level for active campaign id's only.
        Also values passed in params variable can be changed as per requirement.
        I have passed yesterday day in timerange to fetch one day data but this value can be changed as per requirement.
        This method has been designed for all ads fields
        '''
        try:
            results=[]
            for ad_id in self.ad_account_id:
                my_account=self.connection(ad_id) #calling connection method here
                gbq_client=self.gbq_conn() #calling bigquery connection method here

                self.logger.info('Fetching Campaign ads insights data')

                #reading csv file created for all campaigns for multiple ads account
                df = pd.read_csv(file_path+'campaign_df_'+str(dt)+'.csv')
                df=df[df.account_id == int(ad_id.replace('act_',''))] #replacing act_ with blank to fetch only calling ad account id here

                df['created_time'] = pd.to_datetime(df.created_time, utc=True).dt.date #changing created time column value to date to get active or last 90days creaetd campaigns
                base_date=datetime.datetime.strptime((datetime.date.today().replace(day=1) - datetime.timedelta(days=90)).strftime("%Y-%m-%d") ,"%Y-%m-%d").date()
                data = df[(df.effective_status == "ACTIVE") | (df['created_time'] >=base_date ) ]
 
                adsetId = list(data['id'])
                params = {
                    'level':'ad',
                    'limit': '1200',
                    'breakdowns':['country'],
                    'use_account_attribution_setting':'true',
                    'time_range':{'since': str(dt), 'until': str(dt)},
                }
                count = 0
                for i in adsetId:
                    campaign_ads = AdSet(i).get_insights(fields=['account_currency','account_id','account_name','action_values','actions','ad_id','ad_impression_actions','ad_name','adset_id','adset_name','age_targeting','attribution_setting','auction_bid','auction_competitiveness','auction_max_competitor_bid','buying_type','campaign_id','campaign_name','canvas_avg_view_percent','canvas_avg_view_time','catalog_segment_actions','catalog_segment_value','catalog_segment_value_mobile_purchase_roas','catalog_segment_value_omni_purchase_roas','catalog_segment_value_website_purchase_roas','clicks','conversion_values','conversions','converted_product_quantity','converted_product_value','cost_per_15_sec_video_view','cost_per_2_sec_continuous_video_view','cost_per_action_type','cost_per_ad_click','cost_per_conversion','cost_per_dda_countby_convs','cost_per_inline_link_click','cost_per_inline_post_engagement','cost_per_one_thousand_ad_impression','cost_per_outbound_click','cost_per_thruplay','cost_per_unique_action_type','cost_per_unique_click','cost_per_unique_inline_link_click','cost_per_unique_outbound_click','cpc','cpm','cpp','created_time','ctr','date_start','date_stop','dda_countby_convs','estimated_ad_recall_rate_lower_bound','estimated_ad_recall_rate_upper_bound','estimated_ad_recallers_lower_bound','estimated_ad_recallers_upper_bound','frequency','full_view_impressions','full_view_reach','gender_targeting','impressions','inline_link_click_ctr','inline_link_clicks','inline_post_engagement','instant_experience_clicks_to_open','instant_experience_clicks_to_start','instant_experience_outbound_clicks','interactive_component_tap','labels','location','mobile_app_purchase_roas','objective','optimization_goal','outbound_clicks','outbound_clicks_ctr','place_page_name','purchase_roas','qualifying_question_qualify_answer_rate','reach','social_spend','spend','unique_actions','unique_clicks','unique_ctr','unique_inline_link_click_ctr','unique_inline_link_clicks','unique_link_clicks_ctr','unique_outbound_clicks','unique_outbound_clicks_ctr','unique_video_view_15_sec','updated_time','video_15_sec_watched_actions','video_30_sec_watched_actions','video_avg_time_watched_actions','video_continuous_2_sec_watched_actions','video_p100_watched_actions','video_p25_watched_actions','video_p50_watched_actions','video_p75_watched_actions','video_p95_watched_actions','video_play_actions','video_play_curve_actions','video_play_retention_0_to_15s_actions','video_play_retention_20_to_60s_actions','video_play_retention_graph_actions','video_time_watched_actions','website_ctr','website_purchase_roas'], params=params)
                    for ads in range(len(campaign_ads)):
                        data = dict(campaign_ads[ads])
                        #print(data)
                        new_dict={}
                        for key,value in data.items():  #To itrate over adset data to next level
                            if  'facebook_business' in  str(type(value)): #where value have class in targeting_flexible and other columns
                                data[key]= dict(value)
                                for item_sub_key,item_sub_value in data[key].items(): #itartion for dict over class object
                                    if type(item_sub_value) is list:                #check if type of values is dict within list 
                                        for last_var in item_sub_value:                
                                            if type(last_var) is dict:                #check if type of values is dict within list than break to next level for column like targeting_flexible
                                                for item_next_sub_key,item_next_sub_value in last_var.items():
                                                    if type(item_next_sub_value) is list:
                                                        for last_var_next in item_next_sub_value:
                                                            if type(last_var_next) is dict:    #check if type of values is dict within list again for ex:targeting_flexible_spec column 
                                                                for targeting_flexible_spec_key,targeting_flexible_spec_value in last_var_next.items():
                                                                    new_dict[key+"_"+item_sub_key+"_"+item_next_sub_key+"_" +targeting_flexible_spec_key]=targeting_flexible_spec_value
                                                            else:
                                                                new_dict[key+"_"+item_sub_key+"_"+item_next_sub_key]=item_next_sub_value
                                                    else:
                                                        new_dict[key+"_"+item_sub_key+"_"+item_next_sub_key]=item_next_sub_value

                                            else:
                                                new_dict[key+"_"+item_sub_key]=item_sub_value

                                    elif 'facebook_business' in  str(type(item_sub_value)): #for target geo location and some same type of column which have dict under list 
                                        for target_geo_loc_key,target_geo_loc_value in dict(item_sub_value).items():    
                                            if type(target_geo_loc_value) is list:
                                                for last_var_next in target_geo_loc_value:
                                                    if type(last_var_next) is dict:    #check if type of values is dict within list again for ex:targeting_geo_location_places column 
                                                        for targeting_flexible_spec_key,targeting_flexible_spec_value in last_var_next.items():
                                                            new_dict[key+"_"+item_sub_key+"_"+target_geo_loc_key+"_" +targeting_flexible_spec_key]=targeting_flexible_spec_value
                                                    else:
                                                        new_dict[key+"_"+item_sub_key+"_"+target_geo_loc_key]=target_geo_loc_value
                                            else:
                                                new_dict[key+"_"+item_sub_key+"_"+target_geo_loc_key]=target_geo_loc_value

                                    else:
                                        new_dict[key+"_"+item_sub_key]=item_sub_value

                            elif type(value) is list:
                                for last_var in value:
                                    if type(last_var) is dict:
                                        for item_next_sub_key,item_next_sub_value in last_var.items():
                                            new_dict[key+"_"+item_next_sub_key]=item_next_sub_value
                                    else:
                                        new_dict[key]=value
                            else:
                                new_dict[key]=value
                        results.append(new_dict)
                    time.sleep(3) #sleep for 3-5 seconds otherwise api calls will be more than 200 in single call and error will come
                    count +=1   #added count value varible to get view loop is running for active campagin id fetched on above method
                    print("Campaign id- " +str(i)+" data have been fetched successfully and counter number is:- "+ str(count))
            print('Total '+str(count)+' ads data have been fetched')
            self.logger.info('Total '+str(count)+' ads data have been fetched')
            campaign_ads_df= pd.DataFrame(results)
            campaign_ads_df['extractionDate'] = dt
            campaign_ads_df.to_csv (file_path+'campaign_ads_insights_'+str(dt)+'.csv',index=False)#,encoding="utf-8-sig",index=False)
            self.logger.info('ads insights data for all Campaigns ids and ads accounts have been fetched successfully')

            #Pushing data into GCP BigQuery

            table_id='<gcp_project>.<dataset_name>.<table_name>'            
            job = gbq_client.load_table_from_dataframe(campaign_ads_df, table_id)  # Make an API request.
            job.result()
            table = gbq_client.get_table(table_id)  # Make an API request.
            print ("Loaded {} rows and {} columns to {}".format( table.num_rows, len(table.schema), table_id))
            
            self.logger.info("Loaded {} rows and {} columns to {}".format( table.num_rows, len(table.schema), table_id))
            self.logger.info('Campaign ads insights data have been pushed into bigquery successfully')
            self.logger.info("Program Executed in "+str(datetime.datetime.now()-start_time))
        except Exception as e:
            self.logger.exception("Error in getting ads insights data for all Campaigns ids From FaceBook Ads Account")
            self.f.write(str(e))
            self.f.close() #cloing failure log file


FB=FaceBookAds()
FB.get_campaign_overview()
FB.get_campaign_insights()
FB.get_campaign_ads_insights()
notification() #calling email methos in case of any failure mentioned email will get notify

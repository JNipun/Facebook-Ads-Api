#!/usr/bin/python
# -*- coding: utf-8 -*-

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.ad import Ad
import pandas as pd
import datetime
import time
import requests
import json
import os,sys
from utils.utils import log_handler
from json import load

cur_date= datetime.datetime.now()
dt = cur_date.strftime("%Y%m%d")
path=os.getcwd()
file_path= path+"/Data/"
start_time = datetime.datetime.now()

#change the value as per you requirment, i have passed one day delta to get previous day data for automation perspective
dt=(datetime.datetime.now()- datetime.timedelta(1)).strftime("%Y-%m-%d")
execution_date=(datetime.datetime.now()- datetime.timedelta(1)).strftime("%Y%m%d")


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
        try:
                with open('resources/FB_Cred.json', 'r') as f:
                    self.logger.info('*************************')
                    self.logger.info("Facebook Grpah API for campaigns execution has been strated at :"+str(cur_date) +"\n")
                    cred = load(f)
                    self.app_id = cred['app_id']
                    self.app_secret = cred['app_secret']
                    self.ad_account_id = cred['ad_account_id']
                    self.access_token = cred['access_token']
                self.logger.info('Facebook Ads Account Credentails have been loaded successfully'+" \n")
            
        except Exception as e:
                self.logger.exception("Error in loading FaceBook Ads Credentails file")

    def connection(self):
        try:
            self.logger.info('Connecting to Facebook AdAccount')
            FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
            my_account = AdAccount(self.ad_account_id)
            return my_account
            
        except Exception as e:
                self.logger.exception("Error in connecting AdAccount Please check access_token value")

    def get_campaign_overview(self):
        '''Here we will fetch all campaigns associated with a ad_Account id, i have passed effective_status_enum value is ACTIVE only on this code.
        Also values passed in params variable can be changed as per requirement.
        I have passed yesterday day in time range to fetch one day data but this value can be changed as per requirement. 
        '''
        try:
            self.logger.info('Getting Campaign Details From FaceBook Ads Account')
            my_account=self.connection()
            fields = ['account_id','boosted_object_id','budget_remaining','buying_type','can_use_spend_cap','configured_status','created_time','daily_budget','effective_status','id','lifetime_budget','name ','objective','source_campaign_id','spend_cap','start_time','status','stop_time','topline_id','updated_time']
            params = {
            'effective_status_enum': ['ACTIVE'],
            'limit':'2000',
            'Content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'time_range':{'since': str(dt), 'until': str(datetime.date.today())}
            }
            campaigns_raw=my_account.get_campaigns(params=params,fields=fields)
            results=[]
            for campaign in range(len(campaigns_raw)):
                data = dict(campaigns_raw[campaign])
                results.append(data)
            campaign_df= pd.DataFrame(results)
            campaign_df.to_csv (file_path+'campaign_df_'+execution_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.info('Campaigns Details have been fetched successfully')
            return campaign_df
        except Exception as e:
            self.logger.exception("Error in getting campaign details From  FaceBook Ads Account")
    def get_campaign_insights(self):
    '''Here we will fetch all campaigns insights data associated with a ad_Account id at ad level for active campaign id's only.
        Also values passed in params variable can be changed as per requirement.
        I have passed yesterday day in timerange to fetch one day data but this value can be changed as per requirement.
        '''
        try:
            my_account=self.connection() #calling connection method here
            self.logger.info('Execution to fetch Campaign insights data have been started')
            df = pd.read_csv(file_path+'campaign_df_'+execution_date+'.csv') #reading campaign id and name generated from campaign overview method
            data = df[(df.effective_status == "ACTIVE")]
            fields = ['account_currency','account_id','account_name','action_values','actions','ad_id','ad_name','adset_id','adset_name','attribution_setting','buying_type','campaign_id','campaign_name','canvas_avg_view_percent','canvas_avg_view_time','catalog_segment_value','clicks','conversion_rate_ranking','conversion_values','conversions','converted_product_quantity','converted_product_value','cost_per_action_type','cost_per_conversion','cost_per_estimated_ad_recallers','cost_per_inline_link_click','cost_per_inline_post_engagement','cost_per_outbound_click','cost_per_thruplay','cost_per_unique_action_type','cost_per_unique_click','cost_per_unique_inline_link_click','cost_per_unique_outbound_click','cpc','cpm','cpp','ctr','date_start','date_stop','engagement_rate_ranking','estimated_ad_recall_rate','estimated_ad_recallers','frequency','full_view_impressions','full_view_reach','impressions','inline_link_click_ctr','inline_link_clicks','inline_post_engagement','instant_experience_clicks_to_open','instant_experience_clicks_to_start','mobile_app_purchase_roas','objective','optimization_goal','outbound_clicks','outbound_clicks_ctr','place_page_name','purchase_roas','qualifying_question_qualify_answer_rate','quality_ranking','reach','social_spend','spend','unique_actions','unique_clicks','unique_ctr','unique_inline_link_click_ctr','unique_inline_link_clicks','unique_link_clicks_ctr','unique_outbound_clicks','unique_outbound_clicks_ctr','video_30_sec_watched_actions','video_avg_time_watched_actions','video_p100_watched_actions','video_p25_watched_actions','video_p50_watched_actions','video_p75_watched_actions','video_p95_watched_actions','video_play_actions','video_play_curve_actions','website_ctr','website_purchase_roas']
            params = {
                'level':'ad',
                'limit': '1000',
                'time_range':{'since': str(dt), 'until': str(datetime.date.today())},
            }
            results=[]
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
                    count +=1 #added count value varible to get view loop is running for active campagin id fetched on above method
                    print("Campaign id- " +str(i)+" data have been fetched successfully and counter number is:- "+ str(count))
                    time.sleep(3) #sleep for 3-5 seconds otherwise api calls will be more than 200 in single run and error will come
            campaign_ads_df= pd.DataFrame(results)
            campaign_ads_df.to_csv (file_path+'campaign_insights_'+execution_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.info('Campaign insights data have been fetched successfully')
        except Exception as e:
            self.logger.exception("Error in getting campaign details From FaceBook Ads Account")


    def get_campaign_ads_insights(self):
        '''Here we will fetch all campaigns ads insights data associated with a ad_Account id at ad level for active campaign id's only.
        Also values passed in params variable can be changed as per requirement.
        I have passed yesterday day in timerange to fetch one day data but this value can be changed as per requirement.
        '''
        try:
            my_account=self.connection()
            self.logger.info('Fetching Campaign ads insights data')
            df = pd.read_csv(file_path+'campaign_df_'+execution_date+'.csv')
            data = df[(df.effective_status == "ACTIVE")]
            adsetId = list(data['id'])
            params = {
                'level':'ad',
                'limit': '1000',
                'time_range':{'since': str(dt), 'until': str(dt)},
            }
            results=[]
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
                count +=1  #added count value varible to get view loop is running for active campagin id fetched on above method
                print("Campaign id- " +str(i)+" data have been fetched successfully and counter number is:- "+ str(count))
            campaign_ads_df= pd.DataFrame(results)
            campaign_ads_df.to_csv (file_path+'campaign_ads_insights_'+execution_date+'.csv',encoding="utf-8-sig",index=False)
            self.logger.info('ads insights data for all Campaigns ids have been fetched successfully')
            self.logger.info("Program Executed in "+str(datetime.datetime.now()-start_time)+" \n")
            print("Program Executed in "+str(datetime.datetime.now()-start_time)+" successfully")
        except Exception as e:
            self.logger.exception("Error in getting ads insights data for all Campaigns ids From FaceBook Ads Account")


FB=FaceBookAds()
FB.get_campaign_overview()
FB.get_campaign_insights()
FB.get_campaign_ads_insights()
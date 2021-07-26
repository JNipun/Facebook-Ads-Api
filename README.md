# Facebbok-Ads-Api
**Requirements For Facebook-Ads-API Code**


1.Python 3.7<br />

**About Code**

To access Facebbok Ads API A developer needs to create profile on Facebbok developer portalhttps://developers.facebook.com/ .
After that create an app and generate app_id and app_secret. Make sure you have access to Required Facebook Profile ID on which data needs to be pulled. Using these params generate a access_token (works for 60 days) or you can generate Long Lived access_token everytime which is valid fr 1 hr. Steps to generate Long Lived access token can be found on https://developers.facebook.com/docs/facebook-login/access-tokens/refreshing .
I have created ~5-6 scripts here. In Facebook Ads Campaign is parent so main script is campaign.py from which all the campaign id can be fetched an dusing these id's relevant ads, ads_insights data can be extracted.
I have created all the scripts seperatly as of now and these scripts can be combined into one if anyone wants.<br />



**Installation Steps**


1.On Terminal clone the Repository "git clone git@github.com:JNipun/Facebook-Ads-Api.git"<br />
2.pip install -r requirements.txt<br />



**Execution:-**


```
python <script_name>.py 

```

```
In case of any error in any of this script, please go to in Logs folder and check the detailed logs. After successful run output will be generated in csv format under Data folder.

```


**Facebook Ads API Cred Config(Location:resources/FB_Cred.json)**
```
{
  "app_id" : "",
  "app_secret" : "",
  "ad_account_id" : "",
  "access_token" : ""

}
```
Add credentials here and make sure you have access of all the params.

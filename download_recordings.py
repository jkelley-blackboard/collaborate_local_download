"""Download recordings from Recording Report csv

consumes a csv file that looks like the native Recording_Report
required/expected columns = SessionOwner,RecordingLink and ContextIdentifier
gets the recording_uid from the RecordingLink column
builds a filename from recording attributes - leading with created date so they list in chronological order
downloads recordings into course directories as determined by ContextIdentifier column
uses the {{CSA_URL}}/recordings/{recordingId}/url?disposition=download endpont to download them

TODO: Generate a log file
TODO: Verify input file before starting
TODO: Investigate downloading captions

"""


import csv
import requests
import json
import datetime
import jwt
import os.path
import re
import unicodedata
import configparser



config = configparser.ConfigParser()
config.sections()
config.read('download_config.ini')

#set variables from config file   
region_host = config['COLLAB']['RegionHost']
lti_key = config['COLLAB']['LtiKey']
lti_secret = config['COLLAB']['LtiSecret']
recording_report = config['COLLAB']['RecordingReport']
download_path = config['COLLAB']['DownloadPath']
#####################

def main():
    oAuth = get_token()
    
    recordingData = get_input(recording_report)

    for recording in recordingData:
        if is_token_exp(oAuth):    #check if token expired
            oAuth = get_token()    #get a new token
        recording_uid = recording.get('RecordingLink').replace(region_host+"/recording/","")
        owner = recording.get('SessionOwner')
        #print("[main()]: Working on: "+recording_uid)

        #handle recordings not from lti owner
        if owner != lti_key:
            print("[main()] Skipped: "+recording_uid+" not owned by "+lti_key)
            continue
        
        #get the download url if you can
        url = get_download_url(recording_uid.strip(), oAuth)
        if url == 'FAILED':
            continue
        
        filename = filename_from_report(recording)
        download_dir = define_dir(download_path, recording.get('ContextIdentifier'))
        #print("[main()]: download_dir+"/"+filename)
        
        download_recording(url,filename,download_dir)
        print("[main()] Downloaded:" + recording_uid+" >> " + download_dir + "/" + filename)
#####################

def get_input(csvfile):
    #convert CSV into list of dictionaries

    dict_from_csv = {}
    with open(csvfile, mode='r', encoding='utf-8-sig') as inp:
        reader = csv.DictReader(inp)
        list_from_csv = list(reader)
    inp.close()

    #print("[get_input()]: Success: " + list_from_csv)
    return list_from_csv
#####################

def filename_from_report(recording):

    datetime_obj = datetime.datetime.strptime(recording.get('RecordingCreated'), '%m/%d/%Y %H:%M')
    created = datetime_obj.strftime("%Y%m%d_%H%M")
    
    session = slugify(recording.get('SessionName'))
    recname = slugify(recording.get('RecordingName'))
    filename = created + "_" + session + "_" + recname + ".mp4"
    return filename
#####################

def define_dir(root, folder):
    #build dir path and handle directory for recordings without context/course

    if folder == "":
        download_dir = slugify(root) + "/_none/"
    else:
        download_dir = slugify(root) + "/" + slugify(folder)

    return download_dir
#####################

def get_download_url(recording_uid, oAuth):
    #get the download url for the recording from the API using the recording uid value
    
    resp = requests.get(oAuth["endpoint"]+"/recordings/"+recording_uid+"/url?disposition=download",
        headers={"Authorization":"Bearer "+oAuth["token"],
                 "Content-Type":"application/json"
                 }
        )
    
    if resp.status_code == 200:
        parsed_json = json.loads(resp.text)
        download_url = parsed_json["url"]
        #print("[get_download_url()]: Success for: "+recording_uid+": URL: "+download_url) 

    else:
        print("[get_download_url()]: Failed for: " +recording_uid)
        download_url = "FAILED"

    return download_url
#####################

def download_recording(download_url, download_filename, download_dir):
    #execute the download to a course/context based directory

    requests.get(download_url)
    download_path = os.path.join(os.path.dirname(__file__), download_dir)
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        with open(os.path.join(download_path, download_filename), "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    #print("[download_recording()]: Downloaded: "+ download_filename+" to "+download_path)
#####################

def get_token():
    #authenticate and build the oAuth object
    
    oAuth = {
        "key" : lti_key,
        "secret" : lti_secret,
        "endpoint" : region_host+"/collab/api/csa"
    }

    grant_type = "urn:ietf:params:oauth:grant-type:jwt-bearer"

    exp = datetime.datetime.utcnow() + datetime.timedelta(minutes = 5)

    claims = {
        "iss" : oAuth["key"] ,
        "sub" : oAuth["key"] ,
        "exp" : exp
    }

    assertion = jwt.encode(claims, oAuth["secret"], "HS256")

    payload = {
        "grant_type": grant_type,
        "assertion" : assertion
    }

    rest = requests.post(
        oAuth["endpoint"]+"/token",
        data = payload,
        auth = (oAuth["key"], oAuth["secret"])
        )

    print("[auth:setToken()] STATUS CODE: " + str(rest.status_code) )
    res = json.loads(rest.text)
    #print("[auth:setToken()] RESPONSE: \n" + json.dumps(res,indent=4, separators=(",", ": ")))

    if rest.status_code == 200:
        parsed_json = json.loads(rest.text)
        #print(parsed_json)
        oAuth["token"] = parsed_json['access_token']
        oAuth["token_expires"] = parsed_json['expires_in']
    else:
        print("[auth:setToken()] ERROR: " + str(rest))

    return oAuth
###################

def is_token_exp(oAuth):

    expired = True
    exp_datetime = datetime.datetime.strptime(oAuth["token_expires"],'%Y/%m/%d %H:%M:%S.%f')
    if exp_datetime < datetime.datetime.now():
        print('[auth:is_token_exp()] Token Expired at ' + oAuth["token_expires"])
        expired = True
    else:
        #print('[auth:is_token_exp()] Token will expire at ' + oAuth["token_expires"])
        expired = False

    return expired
#####################

def slugify(value, allow_unicode=True):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')
#####################


main()

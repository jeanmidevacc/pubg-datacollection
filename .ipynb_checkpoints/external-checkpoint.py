import requests
import time

import pandas as pd

def get_matchdetails(area,matchid,headers):
    
    #Collect the data
    url_matchdetails = f"https://api.pubg.com/shards/{area}/matches/{matchid}"
    response = requests.get(url_matchdetails, headers=headers)
    match_details = response.json()
    
    #Get some informations on the match
    match_informations = match_details['data']["attributes"]
    type_match = match_informations['gameMode']
    
    #Clean the data
    participants_details = []
    rosters_details = []
    
    #Get info on the participants and the rosters 
    elements_details = match_details["included"]
    if len(elements_details) > filter_players :
        for element in elements_details:
            if element["type"] == 'participant':
                stats = element['attributes']['stats']
                stats['participantid'] = element['id']
                participants_details.append(stats)
            elif element["type"] == 'roster':
                stats = element['attributes']['stats']
                stats['rosterid'] = element['id']
                for participant in element['relationships']['participants']["data"]:
                    stats['participantid'] = participant['id']
                    rosters_details.append(stats)

        # Build dataframes
        df_participants = pd.DataFrame(participants_details)
        df_rosters = pd.DataFrame(rosters_details)

        #Make a join between rosters and particpants
        df_participants_augmented = pd.merge(df_participants,df_rosters,how = "inner",on = ["participantid"])
        df_participants_augmented["area"] = area

        df_participants_augmented["totalduration"] = match_informations['duration']
        df_participants_augmented["mode"] = type_match
        df_participants_augmented["map"] = match_informations['mapName']
        df_participants_augmented["is_custom"] = match_informations['isCustomMatch']

        #Add logit 1 if rank 1 0 in the other case
        df_participants_augmented['is_winner'] = df_participants_augmented.apply(lambda row: 1 if row["rank"] == 1 else 0,axis=1)

        #Lower all the columns name (autist)
        df_participants_augmented.columns = [column.lower() for column in df_participants_augmented.columns]
        
        #Get the url of the events
        for element in match_details['included']:
            if "type" in element:
                if element["type"] == "asset":
                    #print(element)
                    url_eventsmatch = element['attributes']["URL"]
                    break
                    
        df_participants_augmented.drop_duplicates(['playerid'], inplace = True)

    else:
        df_participants_augmented = pd.DataFrame()
        url_eventsmatch = ""
        type_match = "unknow"
        
    return df_participants_augmented,url_eventsmatch,type_match


def get_eventsdetails(url_eventsmatch):
    response_eventmatch = requests.get(url_eventsmatch)

    list_events = []
    for event in response_eventmatch.json():
        clean_event = [event["_D"], event["_T"]]

        #Drop some fields
        for column in ["_D","_T"]:
            del event[column]
        clean_event.append(event)
        list_events.append(clean_event)

    df_events = pd.DataFrame(list_events,columns = ['tstp','type_event','details'])
    df_events["tstp"] = pd.to_datetime(df_events["tstp"])
    df_events.sort_values(['tstp'], ascending= True, inplace = True)
        
    return df_events

def get_platform(area):
    
    if "kakao" in area:
        return "kakao"
    elif "xbox" in area:
        return "xbox"
    elif "pc" in area:
        return "steam"
    
def get_detailsplayers(headers,platform,accountids):
    
    parameter = ",".join(block)
    
    url_detailsplayers = f"https://api.pubg.com/shards/{platform}/players?filter[playerIds]={parameter}"
    
    response = requests.get(url_detailsplayers, headers=headers)
    
    
    
def get_cleandict(raw_details):
    is_dictpresent = True
    while is_dictpresent:
        is_dictpresent = False
        for key in list(raw_details.keys()):
            #print(key)
            elt = raw_details[key]
            if isinstance(elt,dict):
                for key1 in elt:
                    newkey = f"{key}_{key1}"
                    newkey = newkey.lower()
                    #print(newkey)
                    raw_details[newkey] = elt[key1]
                del raw_details[key]
                is_dictpresent = True       
    return raw_details

def get_detailsmatch(df_events):
    
    #Get general details on the match
    startmatch_event = df_events[df_events["type_event"] == 'LogMatchStart']
    endmatch_event = df_events[df_events["type_event"] == 'LogMatchEnd']
    duration_match = float((endmatch_event["tstp"].values[0] - startmatch_event["tstp"].values[0]))/1000000000
    
    details_match = {
        "start_date": pd.to_datetime(str(startmatch_event["tstp"].values[0])).strftime('%Y-%m-%d %H:%M:%S'),
        "end_date": pd.to_datetime(str(endmatch_event["tstp"].values[0])).strftime('%Y-%m-%d %H:%M:%S'),
        "nbr_players": 0,
        "duration":int(duration_match),
        #"global_events":df_events_match_periodic
    }
    
    details_event = df_events[df_events["type_event"] == 'LogMatchStart']["details"].values[0]
    
    for key in details_event:
        if key not in ['bluezonecustomoptions','common','characters']:
            details_match[key.lower()] = details_event[key]
        elif key == 'characters':
            details_match["nbr_players"] = len(details_event[key])
            
    details_event = df_events[df_events["type_event"] == 'LogMatchDefinition']["details"].values[0]
    
    for key in details_event:
        if key not in ['bluezonecustomoptions','common','characters']:
            details_match[key.lower()] = details_event[key]
        elif key == 'characters':
            details_match["nbr_players"] = len(details_event[key])
            
    for key in details_match:
        if isinstance(details_match[key],float):
            details_match[key] = decimal.Decimal(str(details_match[key]))
    return details_match
    
    
    
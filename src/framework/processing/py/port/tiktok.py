#browser history: date, "ik heb een video gekeken", link
#search history: date, "ik heb iets gezocht", Search Term
#login history, date, "ik heb de app geopend", Device

"""
DDP tiktok module
"""

from pathlib import Path
from typing import Any
import logging
import zipfile

import pandas as pd

import port.unzipddp as unzipddp
from port.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)

months = ["January", "February", "March", "April", "May", "June", 
          "July", "August", "September", "October", "November", "December"]

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "user_data.json"
        ],
    ),
    DDPCategory(
        id="text_file_json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "user_data.json"
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Not a valid DDP", message=""),
    StatusCode(id=2, description="Bad zip", message=""),
    StatusCode(id=3, description="Data conditions not met", message=""),
]

def validate(file: Path) -> ValidateInput:
    """
    Validates the input of a TikTok submission
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    # submission was a zipfile
    try:
        paths = []
        with zipfile.ZipFile(file, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        if validation.ddp_category.id is None:
            validation.set_status_code(1)
        else: 
            validation.set_status_code(0)

    # submission was something else
    except zipfile.BadZipFile:
        if file == "user_data.json":
            validation.set_ddp_category("text_file_json_en")
            validation.set_status_code(0)
        else:
            validation.set_status_code(2)

    return validation

def read_tiktok_file(tiktok_file: str, validation: ValidateInput) -> dict[Any, Any] | list[Any]:
    if validation.ddp_category.id == "text_file_json_en":
        out = unzipddp.read_json_from_file(tiktok_file)
    else:
        buf = unzipddp.extract_file_from_zip(tiktok_file, "user_data.json")
        out = unzipddp.read_json_from_bytes(buf)
    return out

def video_browsing_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Video Browsing History"].get("VideoList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt een video gekeken", item.get("Link", None)))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan", "Link van de video die je hebt gekeken"])
    except Exception as e:
        logger.error("Could not extract tiktok history: %s", e)

    return out

def favorites_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        favvideos = d["Activity"]["Favorite Videos"].get("FavoriteVideoList", [])
        for item in favvideos:
            datapoints.append((item.get("Date", None), "Je hebt een video toegevoegd aan je favorieten", item.get("Link", None)))
    except Exception as e_videos:
        logger.error("Could not extract: %s", e_videos)

    try:
        favsounds = d["Activity"]["Favorite Sounds"].get("FavoriteSoundList", [])
        for item in favsounds:
            datapoints.append((item.get("Date", None), "Je hebt muziek toegevoegd aan je favorieten", item.get("Link", None)))
    except Exception as e_sounds:
        logger.error("Could not extract: %s", e_sounds)

    try:
        faveffects = d["Activity"]["Favorite Effects"].get("FavoriteEffectsList", [])
        for item in faveffects:
            datapoints.append((item.get("Date", None), "Je hebt een filter/effect toegevoegd aan je favorieten", item.get("EffectLink", None)))
    except Exception as e_effects:
        logger.error("Could not extract: %s", e_effects)

    try:
        favhashtags = d["Activity"]["Favorite Hashtags"].get("FavoriteHashtagList", [])
        for item in favhashtags:
            datapoints.append((item.get("Date", None), "Je hebt een hashtag toegevoegd aan je favorieten", item.get("Link", None)))
    except Exception as e_hashtags:
        logger.error("Could not extract: %s", e_hashtags)
        
    out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?", "Link naar je favoriete item"])
    out.sort_values(by="Datum", inplace = True)

    return out


# Extract Favorite videos
def favorite_videos_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Favorite Videos"].get("FavoriteVideoList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt deze video toegevoegd aan je favorieten", item.get("Link", None)))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?", "Link naar je favoriete item"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract following
def following_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Following List"].get("Following", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt iemand gevolgd"))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# Extract following
def follower_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Follower List"].get("FansList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Iemand heeft je gevolgd"))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract like VideoList
def like_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Like List"].get("ItemFavoriteList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt een video geliket", item.get("Link", None)))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan", "Link naar de video"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract searchers
def search_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Search History"].get("SearchList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt iets gezocht", item.get("SearchTerm", None)))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?", "Zoekterm"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

def search_history_to_df_anon(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Search History"].get("SearchList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt iets gezocht"))

        out = pd.DataFrame(datapoints, columns=["Datum", "Zoekterm"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract share history
def share_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Share History"].get("ShareHistoryList", [])
        for item in history:
            datapoints.append((item.get("Date", None), item.get("SharedContent"), item.get("Link"), item.get("Method")))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedeeld?", "Link van wat je hebt gedeeld", "Hoe heb je het gedeeld?"])
        out["Hoe heb je het gedeeld?"] = out["Hoe heb je het gedeeld?"].replace("chat_head", "chat")
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# Extract comments
def comment_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Comment"]["Comments"].get("CommentsList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt een comment geplaatst", item.get("Comment", None)))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?", "Comment"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# Extract watch live history
def watch_live_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Tiktok Live"]["Watch Live History"].get("WatchLiveMap", {})
        for _, v in history.items():
            datapoints.append((v.get("WatchTime", None), "Je hebt een livestream gekeken", v.get("Comments", None)))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?", "Comments die je hebt geplaatst"])
        out["Comments die je hebt geplaatst"].fillna("Je hebt geen comment(s) geplaatst", inplace = True)
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# Extract go live history
def go_live_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Tiktok Live"]["Go Live History"].get("GoLiveMap", {})
        for _, v in history.items():
            datapoints.append((v.get("GoTime", None), "Je bent live gegaan", "Je hebt geen comment geplaatst"))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?", "Comments die je hebt geplaatst"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# Extract logging in history
def logging_in_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Login History"].get("LoginHistoryList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt de app geopend", item.get("DeviceModel", None), item.get("DeviceSystem", None)))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan?", "Op welk apparaat?", "Op welk systeem?"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# def logging_in_to_df2(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

#     d = read_tiktok_file(tiktok_zip, validation)
#     datapoints = []
#     out = pd.DataFrame()

#     try: 
#         history = d["Activity"]["Login History"].get("LoginHistoryList", [])
#         for item in history:
#             datapoints.append((item.get("Date", None), "You logged in", item.get("DeviceSystem", None)))

#         out = pd.DataFrame(datapoints, columns=["Date", "Action", "OperatingSystem"])
#         out['Date'] = pd.to_datetime(out['Date'])
#         out = out.sort_values(by="Date")
#         out['Maand'] = out.Date.dt.month_name()
#         out = out.groupby(['Maand']).count().reset_index()
#         out['Maand'] = pd.Categorical(out['Maand'], categories=months, ordered=True)
#         out.sort_values('Maand', inplace = True)
#         out = out.rename(columns = {"Action": "Hoe vaak je bent ingelogd"})
#         out = pd.DataFrame(out, columns=["Maand", "Hoe vaak je bent ingelogd"]).reset_index(drop = True)
#         #out = pd.DataFrame(out, columns=["Maand", "Action", "OperatingSystem"])
#         #out = out.groupby(['Maand']).value_counts(['Action']).reset_index()
#         #out = out.rename(columns = {0: "Hoe vaak je bent ingelogd"})
#         #out = pd.DataFrame(out, columns=["Maand", "Hoe vaak je bent ingelogd"]).reset_index()
#         #out = pd.DataFrame(out, columns=["Month", "count"])
#     except Exception as e:
#         logger.error("Could not extract: %s", e)

#     return out


# Extract blocking history
def blocking_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["App Settings"]["Block"].get("BlockList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "You blocked someone", None, None, None))

        out = pd.DataFrame(datapoints, columns=["Date", "Action", "Url", "OperatingSystem", "likes"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# Extract chatting history
'''
Needs some adaption: list of dicts of single chat converstations
'''
def chat_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Direct Messages"]["Chat History"].get("ChatHistory", {})
        for _,chats in history.items():
            for chat in chats:
                datapoints.append((chat.get("Date", None), chat.get("From", None), "You sent/received a private message"))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wie stuurde het bericht", "Wat is er gebeurd?"])
        
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# Extract posting history
def posting_history_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Video"]["Videos"].get("VideoList", [])
        for item in history:
            datapoints.append((item.get("Date", None), "Je hebt iets gepost", item.get("Link", None), item.get("Likes", None)))

        out = pd.DataFrame(datapoints, columns=["Datum", "Wat heb je gedaan", "Link naar item", "Hoeveel likes had je op deze video?"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

def create_live_history(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Extacts all activities on tiktok with a timestamp

    TODO: CHECK IF ALL ACTIVITIES ARE COVERED
    
    > one df or 4 different ones? - for simplicity one with sparse additional attributes?
    ACTIVITIES (timestamp, activity, video to link[watching & favourites & watch live], operating system[only logging in], likes[only video posted])
    - marked with x = done, v = works
    -----------------------------------------------------------------------
    - Watching (timestamp, "you watched a video", link to video, nan, nan) x v
    - Following (timestamp, "you followed someone", nan, nan, nan) x v
    - favorites (timestamp, "you favourited a video, link to video, nan, nan) x v
    - logging in (timestamp, "you logged in", nan, operating system, nan) x v
    - searching (timestamp, "you searched something", nan, nan, nan) x v
    - sharing (timestamp, "you shared something", nan, nan, nan) x v
    - blocking (timestamp, you blocked someone, nan, nan, nan) x v
    - commenting (timestamp, you commented something, nan, nan, nan) x v
    - chatting (timestamp, you chatted with someone, nan, nan, nan) x v
    - going live (timestamp, "you went live", nan, nan, nan) x ?
    . watching live streams (timestamp, "you watched a live stream", link, nan, nan) x v
    - posting videos (timestamp, "you posted a video", nan, nan, likes) x v
    
    
    """
    out = pd.DataFrame()

    funs = [ 
        watch_live_history_to_df,
        go_live_history_to_df,
        
    ]

    dfs = [fun(tiktok_zip, validation) for fun in funs]
    dfs = [df for df in dfs if not df.empty]

    if len(dfs) > 0:
        out = pd.concat(dfs, axis=0, ignore_index=True)
        out = out.sort_values(by="Datum")

        # Check conditions that need to meet
        # 5 of activity OR 200 single activities
        # days_active_condition_met = len({date[:10] for date in out["Date"]}) >= 5
        days_active_condition_met = True
        number_of_entries_greater_than_200_met = len(out) >= 1

        if days_active_condition_met or number_of_entries_greater_than_200_met:
            pass
        else:
            out = pd.DataFrame()
            validation.set_status_code(3)
    else:
        validation.set_status_code(3)

    return out

# Extract watch live history
def settings_to_df(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:

    d = read_tiktok_file(tiktok_zip, validation)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["App Settings"]["Settings"].get("SettingsMap", {})
        datapoints.append((history.get("Private Account", None), history.get("Personalized Ads", None), history.get("Interests", None)))

        out = pd.DataFrame(datapoints, columns=["Prive Account", "Gepersonaliseerde Advertenties", "Interesses"])
        out["Prive Account"].replace("Disabled", "Je hebt geen prive account", inplace=True)
        out["Prive Account"].replace("Enabled", "Je hebt een prive account", inplace = True)

        out["Gepersonaliseerde Advertenties"].replace("Disabled", "Je ontvangt geen persoonlijke advertenties", inplace=True)
        out["Gepersonaliseerde Advertenties"].replace("Enabled", "Je ontvangt persoonlijke advertenties", inplace=True)                
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


def create_follow_history(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Extacts all activities on tiktok with a timestamp

    TODO: CHECK IF ALL ACTIVITIES ARE COVERED
    
    > one df or 4 different ones? - for simplicity one with sparse additional attributes?
    ACTIVITIES (timestamp, activity, video to link[watching & favourites & watch live], operating system[only logging in], likes[only video posted])
    - marked with x = done, v = works
    -----------------------------------------------------------------------
    - Watching (timestamp, "you watched a video", link to video, nan, nan) x v
    - Following (timestamp, "you followed someone", nan, nan, nan) x v
    - favorites (timestamp, "you favourited a video, link to video, nan, nan) x v
    - logging in (timestamp, "you logged in", nan, operating system, nan) x v
    - searching (timestamp, "you searched something", nan, nan, nan) x v
    - sharing (timestamp, "you shared something", nan, nan, nan) x v
    - blocking (timestamp, you clocked someone, nan, nan, nan) x v
    - commenting (timestamp, you commented something, nan, nan, nan) x v
    - chatting (timestamp, you chatted with someone, nan, nan, nan) x v
    - going live (timestamp, "you went live", nan, nan, nan) x ?
    . watching live streams (timestamp, "you watched a live stream", link, nan, nan) x v
    - posting videos (timestamp, "you posted a video", nan, nan, likes) x v
    
    
    """
    out = pd.DataFrame()

    funs = [following_to_df, follower_to_df]

    dfs = [fun(tiktok_zip, validation) for fun in funs]
    dfs = [df for df in dfs if not df.empty]

    if len(dfs) > 0:
        out = pd.concat(dfs, axis=0, ignore_index=True)
        out = out.sort_values(by="Datum")

        # Check conditions that need to meet
        # 5 of activity OR 200 single activities
        # days_active_condition_met = len({date[:10] for date in out["Date"]}) >= 5
        days_active_condition_met = True
        number_of_entries_greater_than_200_met = len(out) >= 200

        if days_active_condition_met or number_of_entries_greater_than_200_met:
            pass
        else:
            out = pd.DataFrame()
            validation.set_status_code(3)
    else:
        validation.set_status_code(3)

    return out

def create_activity_history2(tiktok_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Extacts all activities on tiktok with a timestamp

    TODO: CHECK IF ALL ACTIVITIES ARE COVERED
    
    > one df or 4 different ones? - for simplicity one with sparse additional attributes?
    ACTIVITIES (timestamp, activity, video to link[watching & favourites & watch live], operating system[only logging in], likes[only video posted])
    - marked with x = done, v = works
    -----------------------------------------------------------------------
    - Watching (timestamp, "you watched a video", link to video, nan, nan) x v
    - Following (timestamp, "you followed someone", nan, nan, nan) x v
    - favorites (timestamp, "you favourited a video, link to video, nan, nan) x v
    - logging in (timestamp, "you logged in", nan, operating system, nan) x v
    - searching (timestamp, "you searched something", nan, nan, nan) x v
    - sharing (timestamp, "you shared something", nan, nan, nan) x v
    - blocking (timestamp, you clocked someone, nan, nan, nan) x v
    - commenting (timestamp, you commented something, nan, nan, nan) x v
    - chatting (timestamp, you chatted with someone, nan, nan, nan) x v
    - going live (timestamp, "you went live", nan, nan, nan) x ?
    . watching live streams (timestamp, "you watched a live stream", link, nan, nan) x v
    - posting videos (timestamp, "you posted a video", nan, nan, likes) x v
    
    
    """
    out = pd.DataFrame()

    funs = [ 
        #video_browsing_history_to_df,
        #favorite_videos_to_df,
        #following_to_df,
        #like_to_df,
        #search_history_to_df,
        #share_history_to_df,
        #comment_to_df,
        #watch_live_history_to_df,
        logging_in_to_df2,
        #blocking_history_to_df,
        #chat_history_to_df,
        #go_live_history_to_df,
        #posting_history_to_df,
        
    ]

    dfs = [fun(tiktok_zip, validation) for fun in funs]
    dfs = [df for df in dfs if not df.empty]

    if len(dfs) > 0:
        out = pd.concat(dfs, axis=0, ignore_index=True)
        #out = out.sort_values(by="Date")

        # Check conditions that need to meet
        # 5 of activity OR 200 single activities
        # days_active_condition_met = len({date[:10] for date in out["Date"]}) >= 5
        days_active_condition_met = True
        number_of_entries_greater_than_200_met = len(out) >= 200

        if days_active_condition_met or number_of_entries_greater_than_200_met:
            pass
        else:
            out = pd.DataFrame()
            validation.set_status_code(3)
    else:
        validation.set_status_code(3)

    return out

"""
DDP tiktok module
"""

from pathlib import Path
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

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "user_data.json"
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid zip", message="Valid zip"),
    StatusCode(id=1, description="Bad zipfile", message="Bad zipfile"),
]

def validate_zip(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Instagram zipfile
    """

    validate = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validate.set_status_code(0)
        validate.infer_ddp_category(paths)
    except zipfile.BadZipFile:
        validate.set_status_code(1)

    return validate

   
def video_browsing_history_to_df(tiktok_zip: str) -> pd.DataFrame:

    buf = unzipddp.extract_file_from_zip(tiktok_zip, "user_data.json")
    d = unzipddp.read_json_from_bytes(buf)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Video Browsing History"].get("VideoList", [])
        for item in history:
            datapoints.append((item.get("Date", None), item.get("Link", None)))

        out = pd.DataFrame(datapoints, columns=["Date", "Url"])
    except Exception as e:
        logger.error("Could not extract tiktok history: %s", e)

    return out


# Extract Favorite videos
def favorite_videos_to_df(tiktok_zip: str) -> pd.DataFrame:

    buf = unzipddp.extract_file_from_zip(tiktok_zip, "user_data.json")
    d = unzipddp.read_json_from_bytes(buf)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Favorite Videos"].get("FavoriteVideoList", [])
        for item in history:
            datapoints.append((item.get("Date", None), item.get("Link", None)))

        out = pd.DataFrame(datapoints, columns=["Date", "Url"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract following
def following_to_df(tiktok_zip: str) -> pd.DataFrame:

    buf = unzipddp.extract_file_from_zip(tiktok_zip, "user_data.json")
    d = unzipddp.read_json_from_bytes(buf)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Following List"].get("Following", [])
        for item in history:
            datapoints.append((item.get("Date", None), item.get("UserName", None)))

        out = pd.DataFrame(datapoints, columns=["Date", "Username"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract like VideoList
def like_to_df(tiktok_zip: str) -> pd.DataFrame:

    buf = unzipddp.extract_file_from_zip(tiktok_zip, "user_data.json")
    d = unzipddp.read_json_from_bytes(buf)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Like List"].get("ItemFavoriteList", [])
        for item in history:
            datapoints.append((item.get("Date", None), item.get("Link", None)))

        out = pd.DataFrame(datapoints, columns=["Date", "Url"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract searchers
def search_history_to_df(tiktok_zip: str) -> pd.DataFrame:

    buf = unzipddp.extract_file_from_zip(tiktok_zip, "user_data.json")
    d = unzipddp.read_json_from_bytes(buf)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Search History"].get("SearchList", [])
        for item in history:
            datapoints.append((
                item.get("Date", None),
                item.get("SearchTerm", None)
            ))

        out = pd.DataFrame(datapoints, columns=["Date", "Search Term"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract share history
def share_history_to_df(tiktok_zip: str) -> pd.DataFrame:

    buf = unzipddp.extract_file_from_zip(tiktok_zip, "user_data.json")
    d = unzipddp.read_json_from_bytes(buf)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Activity"]["Share History"].get("ShareHistoryList", [])
        for item in history:
            datapoints.append((
                item.get("Date", None), 
                item.get("SharedContent", None),
                item.get("Link", None),
                item.get("Method", None)
            ))

        out = pd.DataFrame(datapoints, columns=["Date", "Shared Content", "Url", "Method"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out


# Extract comments
def comment_to_df(tiktok_zip: str) -> pd.DataFrame:

    buf = unzipddp.extract_file_from_zip(tiktok_zip, "user_data.json")
    d = unzipddp.read_json_from_bytes(buf)
    datapoints = []
    out = pd.DataFrame()

    try: 
        history = d["Comment"]["Comments"].get("CommentsList", [])
        for item in history:
            datapoints.append((
                item.get("Date", None), 
                item.get("Comment", None)
            ))

        out = pd.DataFrame(datapoints, columns=["Date", "Comment"])
    except Exception as e:
        logger.error("Could not extract: %s", e)

    return out

# TODO
# Extract watch live history

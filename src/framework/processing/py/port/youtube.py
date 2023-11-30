"""
DDP extract Youtube
"""
from pathlib import Path
import logging
import zipfile
import re
import io

import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree

import port.unzipddp as unzipddp
import port.helpers as helpers

from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)

logger = logging.getLogger(__name__)


VIDEO_REGEX = r"(?P<video_url>^http[s]?://www\.youtube\.com/watch\?v=[a-z,A-Z,0-9,\-,_]+)(?P<rest>$|&.*)"
CHANNEL_REGEX = r"(?P<channel_url>^http[s]?://www\.youtube\.com/channel/[a-z,A-Z,0-9,\-,_]+$)"

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "archive_browser.html",
            "watch-history.json",
            "my-comments.html",
            "my-live-chat-messages.html",
            "subscriptions.csv",
        ],
    ),
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "archive_browser.html",
            "watch-history.html",
            "my-comments.html",
            "my-live-chat-messages.html",
            "subscriptions.csv",
        ],
    ),
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "archive_browser.html",
            "kijkgeschiedenis.json",
            "mijn-reacties.html",
            "abonnementen.csv",
        ],
    ),
    DDPCategory(
        id="html_nl",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.NL,
        known_files=[
            "archive_browser.html",
            "kijkgeschiedenis.html",
            "mijn-reacties.html",
            "abonnementen.csv",
        ],
    ),
]


STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Valid DDP unhandled format", message=""),
    StatusCode(id=2, description="Not a valid DDP", message=""),
    StatusCode(id=3, description="Bad zipfile", message=""),
]


def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Youtube zipfile

    This function sets a validation object generated with ValidateInput
    This validation object can be read later on to infer possible problems with the zipfile
    I dont like this design myself, but I also havent found any alternatives that are better
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json", ".csv", ".html"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        if validation.ddp_category.ddp_filetype == DDPFiletype.HTML:
            validation.set_status_code(0)
        elif validation.ddp_category.id is None:
            validation.set_status_code(2)
        else:
            validation.set_status_code(1)

    except zipfile.BadZipFile:
        validation.set_status_code(3)

    return validation


def try_to_convert_datetime_column(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    try:
        df[date_column] = df[date_column].apply(helpers.convert_datetime_str)
    except Exception as e:
        logger.debug("Exception was caught:  %s", e)

    return df

# Extract my-comments.html
def bytes_to_soup(buf: io.BytesIO) -> BeautifulSoup:
    """
    Remove undecodable bytes from utf-8 string
    BeautifulSoup will hang otherwise
    """

    utf_8_str = buf.getvalue().decode("utf-8", errors="ignore")
    utf_8_str = re.sub(r'[^\x00-\x7F]+', ' ', utf_8_str)
    soup = BeautifulSoup(utf_8_str, "lxml")
    return soup


def my_comments_to_df(youtube_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Parses my-comments.html or mijn-reacties.html from Youtube DDP

    input string to zipfile output df 
    with the comment, type of comment, and a video url
    """

    data_set = []
    video_pattern = re.compile(VIDEO_REGEX)
    df = pd.DataFrame()

    # Determine the language of the file name
    file_name = "my-comments.html"
    if validation.ddp_category.language == Language.NL:
        file_name = "mijn-reacties.html"

    comments = unzipddp.extract_file_from_zip(youtube_zip, file_name)
        
    try:
        soup = bytes_to_soup(comments)
        items = soup.find_all("li")
        for item in items:
            data_point = {}

            # Extract comments
            content = item.get_text(separator="<SEP>").split("<SEP>")
            message = content.pop()
            action = "".join(content)
            data_point["Comment"] = message
            data_point["Type of comment"] = action

            # Search through all references
            # if a video can be found:
            # 1. extract video url
            # 2. add data point
            for ref in item.find_all("a"):
                regex_result = video_pattern.match(ref.get("href"))
                if regex_result:
                    data_point["Video url"] = regex_result.group("video_url")
                    data_set.append(data_point)
                    break

        df = pd.DataFrame(data_set)

    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    return df



# Extract Watch later.csv
def watch_later_to_df(youtube_zip: str) -> pd.DataFrame:
    """
    Parses 'Watch later.csv' from Youtube DDP
    Filename is the same for Dutch and English Language settings

    Note: 'Watch later.csv' is NOT a proper csv it 2 csv's in one
    """

    ratings_bytes = unzipddp.extract_file_from_zip(youtube_zip, "Watch later.csv")

    # remove the first 3 lines from the .csv
    #ratings_bytes = io.BytesIO(re.sub(b'^(.*)\n(.*)\n\n', b'', ratings_bytes.read()))
    ratings_bytes = io.BytesIO(re.sub(b'^((?s).)*?\n\n', b'', ratings_bytes.read()))

    df = unzipddp.read_csv_from_bytes_to_df(ratings_bytes)
    try:
        df['Video-ID'] = 'https://www.youtube.com/watch?v=' + df['Video-ID']
    except Exception as e:
        logger.debug("Exception was caught:  %s", e)

    return df



# Extract subscriptions.csv
def subscriptions_to_df(youtube_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Parses 'subscriptions.csv' or 'abonnementen.csv' from Youtube DDP
    """

    # Determine the language of the file name
    file_name = "subscriptions.csv"
    if validation.ddp_category.language == Language.NL:
        file_name = "abonnementen.csv"

    ratings_bytes = unzipddp.extract_file_from_zip(youtube_zip, file_name)
    df = unzipddp.read_csv_from_bytes_to_df(ratings_bytes)
    return df


# Extract watch history
def watch_history_extract_html(bytes: io.BytesIO) -> pd.DataFrame:
    """
    watch-history.html bytes buffer to pandas dataframe
    """

    out = pd.DataFrame()
    datapoints = []

    try:
        tree = etree.HTML(bytes.read())
        watch_history_container_class = "content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"
        r = tree.xpath(f"//div[@class='{watch_history_container_class}']")

        for e in r:
            child_all_text_list = e.xpath("text()")

            datetime = child_all_text_list.pop()
            atags = e.xpath("a")

            try:
                title = atags[0].text
                video_url = atags[0].get("href")
            except:
                if len(child_all_text_list) != 0:
                    title = child_all_text_list[0]
                else:
                    title = None
                video_url = None
                logger.debug("Could not find a title")
            try:
                channel_name = atags[1].text
            except:
                channel_name = None
                logger.debug("Could not find the channel name")

            datapoints.append(
                (title, video_url, channel_name, datetime)
            )
        out = pd.DataFrame(datapoints, columns=["Title", "Url", "Channel", "Date"])

    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    return out


def watch_history_to_df(youtube_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Works for watch-history.html and kijkgeschiedenis.html
    """
    if validation.ddp_category.ddp_filetype == DDPFiletype.HTML:
        # Determine the language of the file name
        file_name = "watch-history.html"
        if validation.ddp_category.language == Language.NL:
            file_name = "kijkgeschiedenis.html"

        html_bytes_buf = unzipddp.extract_file_from_zip(youtube_zip, file_name)
        out = watch_history_extract_html(html_bytes_buf)
        out["Date standard format"] = out["Date"].apply(helpers.try_to_convert_any_timestamp_to_iso8601)

    else:
        out = pd.DataFrame([("Er zit wel data in jouw data package, maar we hebben het er niet uitgehaald")], columns=["Extraction not implemented"])

    return out



# Extract my-live-chat-messages.html
def my_live_chat_messages_to_df(youtube_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    my-live-chat-messages.html to df
    mijn-live-chat-berichten.html
    """
    file_name = "my-live-chat-messages.html"
    if validation.ddp_category.language == Language.NL:
        file_name = "mijn-live-chat-berichten.html"

    live_chats_buf = unzipddp.extract_file_from_zip(youtube_zip, file_name)

    out = pd.DataFrame()
    datapoints = []
    pattern = r"^(.*?\.)(.*)"

    try: 
        tree = etree.HTML(live_chats_buf.read())
        r = tree.xpath(f"//li")
        for e in r:
            # get description and chat message
            full_text = ''.join(e.itertext())
            matches = re.match(pattern, full_text)
            try:
                description = matches.group(1)
                message = matches.group(2)
            except:
                description = message = None

            # extract video url
            message = e.xpath("text()").pop()
            atags = e.xpath("a")
            if atags:
                url = atags[0].get("href")
            else:
                url = None
            
            datapoints.append((description, message, url))
        out = pd.DataFrame(datapoints, columns=["Beschrijving", "Bericht", "Url"])

    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    return out



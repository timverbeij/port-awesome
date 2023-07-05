import logging
import json
import io

import pandas as pd

import port.api.props as props
import port.helpers as helpers
import port.youtube as youtube
import port.validate as validate
import port.tiktok as tiktok
import port.twitter as twitter
import port.facebook as facebook
import port.chrome as chrome
import port.instagram as instagram
import port.linkedin as linkedin

from port.api.commands import (CommandSystemDonate, CommandUIRender)

LOG_STREAM = io.StringIO()

logging.basicConfig(
    #stream=LOG_STREAM,
    level=logging.DEBUG,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")


def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platforms = [
        ("LinkedIn", extract_linkedin, linkedin.validate),
        ("Instagram", extract_instagram, instagram.validate),
        ("Chrome", extract_chrome, chrome.validate),
        ("Facebook", extract_facebook, facebook.validate),
        ("Youtube", extract_youtube, youtube.validate),
        ("TikTok", extract_tiktok, tiktok.validate),
        ("Twitter", extract_twitter, twitter.validate),
    ]

    # progress in %
    subflows = len(platforms)
    steps = 2
    step_percentage = (100 / subflows) / steps
    progress = 0

    # For each platform
    # 1. Prompt file extraction loop
    # 2. In case of succes render data on screen
    for platform in platforms:
        platform_name, extraction_fun, validation_fun = platform

        table_list = None
        progress += step_percentage

        # Prompt file extraction loop
        while True:
            LOGGER.info("Prompt for file for %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Render the propmt file page
            promptFile = prompt_file("application/zip, text/plain, application/json", platform_name)
            file_result = yield render_donation_page(platform_name, promptFile, progress)

            if file_result.__type__ == "PayloadString":
                validation = validation_fun(file_result.value)

                # DDP is recognized: Status code zero
                if validation.status_code.id == 0: 
                    LOGGER.info("Payload for %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")

                    table_list = extraction_fun(file_result.value, validation)
                    break

                # DDP is not recognized: Different status code
                if validation.status_code.id != 0: 
                    LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                    yield donate_logs(f"{session_id}-tracking")
                    retry_result = yield render_donation_page(platform_name, retry_confirmation(platform_name), progress)

                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        LOGGER.info("Skipped during retry %s", platform_name)
                        yield donate_logs(f"{session_id}-tracking")
                        break
            else:
                LOGGER.info("Skipped %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                break

        progress += step_percentage

        # Render data on screen
        if table_list is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Check if extract something got extracted
            if len(table_list) == 0:
                table_list.append(create_empty_table(platform_name))

            prompt = assemble_tables_into_form(table_list)
            consent_result = yield render_donation_page(platform_name, prompt, progress)

            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                yield donate(platform_name, consent_result.value)
            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")

    yield render_end_page()



##################################################################

def assemble_tables_into_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, [])


def create_consent_form_tables(unique_table_id: str, title: props.Translatable, df: pd.DataFrame) -> list[props.PropsUIPromptConsentFormTable]:
    """
    This function chunks extracted data into tables of 5000 rows that can be renderd on screen
    """

    df_list = helpers.split_dataframe(df, 5000)
    out = []

    if len(df_list) == 1:
        table = props.PropsUIPromptConsentFormTable(unique_table_id, title, df_list[0])
        out.append(table)
    else:
        for i, df in enumerate(df_list):
            index = i + 1
            title_with_index = props.Translatable({lang: f"{val} {index}" for lang, val in title.translations.items()})
            table = props.PropsUIPromptConsentFormTable(f"{unique_table_id}_{index}", title_with_index, df)
            out.append(table)

    return out


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Er ging niks mis, maar we konden niks vinden",
       "nl": "Er ging niks mis, maar we konden niks vinden"
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table


##################################################################
# Extraction functions

def extract_youtube(youtube_zip: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    """
    Main data extraction function
    Assemble all extraction logic here
    """
    tables_to_render = []

    # Extract comments
    df = youtube.my_comments_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube comments", "nl": "Youtube comments"})
        tables = create_consent_form_tables("youtube_comments", table_title, df) 
        tables_to_render.extend(tables)

    # Extract Watch later.csv
    df = youtube.watch_later_to_df(youtube_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube watch later", "nl": "Youtube watch later"})
        tables = create_consent_form_tables("youtube_watch_later", table_title, df) 
        tables_to_render.extend(tables)

    # Extract subscriptions.csv
    df = youtube.subscriptions_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube subscriptions", "nl": "Youtube subscriptions"})
        tables = create_consent_form_tables("youtube_subscriptions", table_title, df) 
        tables_to_render.extend(tables)

    # Extract subscriptions.csv
    df = youtube.watch_history_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube watch history", "nl": "Youtube watch history"})
        tables = create_consent_form_tables("youtube_watch_history", table_title, df) 
        tables_to_render.extend(tables)

    # Extract live chat messages
    df = youtube.my_live_chat_messages_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube my live chat messages", "nl": "Youtube my live chat messages"})
        tables = create_consent_form_tables("youtube_my_live_chat_messages", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def extract_tiktok(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.video_browsing_history_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok video browsing history", "nl": "Tiktok video browsing history"})
        tables = create_consent_form_tables("tiktok_video_browsing_history", table_title, df) 
        tables_to_render.extend(tables)

    df = tiktok.favorite_videos_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok favorite videos", "nl": "Tiktok favorite videos"})
        tables = create_consent_form_tables("tiktok_favorite_videos", table_title, df) 
        tables_to_render.extend(tables)

    df = tiktok.following_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok following", "nl": "Tiktok following"})
        tables = create_consent_form_tables("tiktok_following", table_title, df) 
        tables_to_render.extend(tables)

    df = tiktok.like_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok likes", "nl": "Tiktok likes"})
        tables = create_consent_form_tables("tiktok_like", table_title, df) 
        tables_to_render.extend(tables)

    df = tiktok.search_history_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok search history", "nl": "Tiktok search history"})
        tables = create_consent_form_tables("tiktok_search_history", table_title, df) 
        tables_to_render.extend(tables)

    df = tiktok.share_history_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok share history", "nl": "Tiktok share history"})
        tables = create_consent_form_tables("tiktok_share_history", table_title, df) 
        tables_to_render.extend(tables)

    df = tiktok.comment_to_df(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Tiktok comment history", "nl": "Tiktok comment history"})
        tables = create_consent_form_tables("tiktok_comment", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def extract_twitter(twitter_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = twitter.following_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter following", "nl": "Twitter following"})
        tables = create_consent_form_tables("twitter_following", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.like_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter likes", "nl": "Twitter likes"})
        tables = create_consent_form_tables("twitter_like", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.tweets_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter tweets", "nl": "Twitter tweets"})
        tables = create_consent_form_tables("twitter_tweets", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.user_link_clicks_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter user link clicks", "nl": "Twitter user link clicks"})
        tables = create_consent_form_tables("twitter_user_link_clicks", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.block_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter block", "nl": "Twitter block"})
        tables = create_consent_form_tables("twitter_block", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.mute_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Twitter mute", "nl": "Twitter mute"})
        tables = create_consent_form_tables("twitter_mute", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def extract_facebook(facebook_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = facebook.group_interactions_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook group interactions", "nl": "Facebook group interactions"})
        tables = create_consent_form_tables("facebook_group_interactions", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.comments_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook comments", "nl": "Facebook comments"})
        tables = create_consent_form_tables("facebook_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.likes_and_reactions_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook likes and reactions", "nl": "Facebook likes and reactions"})
        tables = create_consent_form_tables("facebook_likes_and_reactions", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.your_badges_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your badges", "nl": "Facebook your badges"})
        tables = create_consent_form_tables("facebook_your_badges", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.your_posts_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your posts", "nl": "Facebook your posts"})
        tables = create_consent_form_tables("facebook_your_posts", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.your_search_history_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your searh history", "nl": "Facebook your search history"})
        tables = create_consent_form_tables("facebook_your_search_history", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.recently_viewed_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook recently viewed", "nl": "Facebook recently viewed"})
        tables = create_consent_form_tables("facebook_recently_viewed", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.recently_visited_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook recently visited", "nl": "Facebook recently visited"})
        tables = create_consent_form_tables("facebook_recently_visited", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.feed_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook feed", "nl": "Facebook feed"})
        tables = create_consent_form_tables("facebook_feed", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.controls_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook controls", "nl": "Facebook controls"})
        tables = create_consent_form_tables("facebook_controls", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def extract_chrome(chrome_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = chrome.browser_history_to_df(chrome_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Chrome browser history", "nl": "Chrome browser history"})
        tables = create_consent_form_tables("chrome_browser_history", table_title, df) 
        tables_to_render.extend(tables)

    df = chrome.bookmarks_to_df(chrome_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Chrome bookmarks", "nl": "Chrome bookmarks"})
        tables = create_consent_form_tables("chrome_bookmarks", table_title, df) 
        tables_to_render.extend(tables)

    df = chrome.omnibox_to_df(chrome_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Chrome omnibox", "nl": "Chrome omnibox"})
        tables = create_consent_form_tables("chrome_omnibox", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def extract_instagram(instagram_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = instagram.accounts_not_interested_in_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram accounts not interested in", "nl": "Instagram accounts not interested in"})
        tables = create_consent_form_tables("instagram_accounts_not_interested_in", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.ads_viewed_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram ads viewed", "nl": "Instagram ads viewed"})
        tables = create_consent_form_tables("instagram_ads_viewed", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.posts_viewed_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram posts viewed", "nl": "Instagram posts viewed"})
        tables = create_consent_form_tables("instagram_posts_viewed", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.posts_not_interested_in_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram posts not interested in", "nl": "Instagram posts not interested in"})
        tables = create_consent_form_tables("instagram_posts_not_interested_in", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.videos_watched_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram videos_watched", "nl": "Instagram posts videos_watched"})
        tables = create_consent_form_tables("instagram_videos_watched", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.post_comments_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram post_comments", "nl": "Instagram posts post_comments"})
        tables = create_consent_form_tables("instagram_post_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.following_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram following", "nl": "Instagram posts following"})
        tables = create_consent_form_tables("instagram_following", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.liked_comments_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram liked_comments", "nl": "Instagram posts liked_comments"})
        tables = create_consent_form_tables("instagram_liked_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.liked_posts_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Instagram liked_posts", "nl": "Instagram posts liked_posts"})
        tables = create_consent_form_tables("instagram_liked_posts", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def extract_linkedin(zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = linkedin.company_follows_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin company_follows", "nl": "Linkedin company_follows"})
        tables = create_consent_form_tables("linkedin_company_follows", table_title, df) 
        tables_to_render.extend(tables)

    df = linkedin.member_follows_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin member_follows", "nl": "Linkedin member_follows"})
        tables = create_consent_form_tables("linkedin_member_follows", table_title, df) 
        tables_to_render.extend(tables)

    df = linkedin.connections_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin connections", "nl": "Linkedin connections"})
        tables = create_consent_form_tables("linkedin_connections", table_title, df) 
        tables_to_render.extend(tables)

    df = linkedin.reactions_to_df(zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Linkedin reactions", "nl": "Linkedin reactions"})
        tables = create_consent_form_tables("linkedin_reactions", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


##########################################
# Functions provided by Eyra did not change

def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform}))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)

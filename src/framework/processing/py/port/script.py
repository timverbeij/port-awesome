##EMPTY DICS NOT WORKING!###

import logging
import json
import io

import pandas as pd

import port.api.commands as commands
import port.api.props as props
import port.helpers as helpers
import port.validate as validate
import port.tiktok as tiktok

from port.api.commands import (CommandSystemDonate, CommandUIRender)

from typing import TypedDict, Optional, Literal

page = None
LOG_STREAM = io.StringIO()

logging.basicConfig(
    stream=LOG_STREAM,
    level=logging.DEBUG,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")

#file input maar 1 keer dus uit de loop

def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platform_name = "TikTok"
    foutje = "Er gaat iets mis"
    validation_fun = tiktok.validate

    extract_funs = [extract_tiktok_essential]

    # extract_funs = [extract_tiktok_follow, #2 
    #                 extract_tiktok_follower, #3
    #                 extract_tiktok_following, #4
    #                 extract_tiktok_browsing, #5
    #                 extract_tiktok_search, #6
    #                 extract_tiktok_log, #7
    #                 extract_tiktok_live] #8
    
    extract_funs_anon = [None, #1
                         None, #2
                         None, #3
                         None, #4
                         None, #5
                         extract_tiktok_search_anon, #6 
                         None, #7
                         None] #8
    
    descr_funs = ["all tiktok data", #1
    "follow", #2
    "follower", #3
    "following", #4
    "browsing-geschiedenis", #5
    "zoekgeschiedenis", #6
    "login-geschiedenis", #7
    "live"] #8

    # # progress in %
    subflows = 1
    steps = 3
    step_percentage = (100 / subflows) / steps
    progress = 0

    #data = None
    validity = "invalid"
    progress += step_percentage
        
    while validity == "invalid":
        LOGGER.info("Prompt for file for %s", platform_name)
        yield donate_logs(f"{session_id}-tracking")

        # Render the propmt file page
        promptFile = prompt_file("application/zip, text/plain, application/json", platform_name)
        file_result = yield render_donation_page("Uploaden van TikTok Data", promptFile, progress)

        if file_result.__type__ == "PayloadString":
            validation = validation_fun(file_result.value)
            print(validation.status_code.id)

        # DDP is recognized: Status code zero
            if validation.status_code.id == 0:
                validity = "valid"
                LOGGER.info("Payload for %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")

        # DDP Data conditions are not met
            if validation.status_code.id == 3: 
                LOGGER.info("Data conditions not met", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                retry_result = yield render_donation_page(platform_name, retry_confirmation_data_conditions_not_met(platform_name), progress)

                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    LOGGER.info("Skipped during data conditions not met retry %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")
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

    if validity == "valid":

        # overview = chart_test(file_result.value, validation)
            
        # prompt_chart = assemble_tables_into_form(overview)
        # yield render_donation_page("Overzicht van jouw TikTok data", prompt_chart, progress)

        #yield render_donation_page(platform_name, summary(file_result.value, validation), progress)
        #for extract_fun, extract_fun_anon, descr_fun in zip(extract_funs, extract_funs_anon, descr_funs):
        LOGGER.info("Prompt consent; %s", platform_name)
        yield donate_logs(f"{session_id}-tracking")

        tables_to_donate = extract_tiktok_all(file_result.value, validation) #extract alle tiktok data

        if len(tables_to_donate) == 0:
            tables_to_donate.append(create_empty_table(platform_name))
            
        prompt = assemble_tables_into_form(tables_to_donate)
        consent_result = yield render_donation_page("Het doneren van je TikTok data", prompt, progress)

        if consent_result.__type__ == "PayloadJSON":
            LOGGER.info("Data donated; %s", extract_tiktok_essential)
            yield donate_logs(f"{session_id}-tracking")
            yield donate(platform_name, consent_result.value)
            progress += step_percentage
        else:
            LOGGER.info("Skipped ater reviewing consent: %s", extract_tiktok_essential)
            yield donate_logs(f"{session_id}-tracking")
            stukkies = yield render_donation_page("Belangrijke TikTok Data", in_stukjes("Belangrijke TikTok Data"), progress)
            
            if stukkies.__type__ == "PayloadTrue":
                for extract_fun in extract_funs:
                    LOGGER.info("Prompt consent; %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")

                    tables_to_donate = extract_tiktok_essential(file_result.value, validation) #extract essential tiktok data

                    if len(tables_to_donate) == 0:
                        tables_to_donate.append(create_empty_table(platform_name))
                        
                    prompt = assemble_tables_into_form(tables_to_donate)
                    consent_result = yield render_donation_page("Belangrijke TikTok Data", prompt, progress)

                    if consent_result.__type__ == "PayloadJSON":
                        LOGGER.info("Data donated; %s", extract_fun)
                        yield donate_logs(f"{session_id}-tracking")
                        yield donate(platform_name, consent_result.value)
                        progress += step_percentage

                        extra_data = yield render_donation_page("Extra TikTok Data", extra(platform_name), progress)

                        if extra_data.__type__ == "PayloadTrue":
                            LOGGER.info("Prompt consent; %s", platform_name)
                            yield donate_logs(f"{session_id}-tracking")

                            tables_to_donate = extract_tiktok_extra(file_result.value, validation) #wil je nog extra data doneren?

                            if len(tables_to_donate) == 0:
                                tables_to_donate.append(create_empty_table(platform_name))
                                    
                            prompt = assemble_tables_into_form(tables_to_donate)
                            consent_result = yield render_donation_page("Extra TikTok Data", prompt, progress)

                            if consent_result.__type__ == "PayloadJSON":
                                LOGGER.info("Data donated; %s", extract_fun)
                                yield donate_logs(f"{session_id}-tracking")
                                yield donate(platform_name, consent_result.value)
                                progress += step_percentage
                            else:
                                LOGGER.info("Skipped ater reviewing consent: %s", extract_fun)
                                yield donate_logs(f"{session_id}-tracking")
                                
                    else:
                        LOGGER.info("Skipped ater reviewing consent: %s", extract_tiktok_essential)
                        yield donate_logs(f"{session_id}-tracking")
                        yield render_donation_page("Je kan helaas niet meedoen", helaas(platform_name), progress) #jammer je kan niet meedoen
                        progress = 100
                        
            else:
                LOGGER.info("Niet in stukkies: %s", extract_tiktok_essential)
                yield donate_logs(f"{session_id}-tracking")

                ###TESTING###
                # if extract_fun == extract_tiktok_search and extract_fun_anon != None:
                #     tables_to_donate = extract_fun_anon(file_result.value, validation)
                #     if len(tables_to_donate) > 0:
                #         prompt = assemble_tables_into_form(tables_to_donate)
                #         consent_result = yield render_donation_page(platform_name, prompt, progress)
            
            progress += step_percentage
                    
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
        out.extend(table)
    else:
        for i, df in enumerate(df_list):
            index = i + 1
            title_with_index = props.Translatable({lang: f"{val} {index}" for lang, val in title.translations.items()})
            table = props.PropsUIPromptConsentFormTable(f"{unique_table_id}_{index}", title_with_index, df)
            out.extend(table)

    return out

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
       "en": "Nothing went wrong, but we couldn't find anything in the uploaded file.",
        "nl":"Es ist nichts schief gelaufe, aber die hochgeladene Datei ist leer."
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table

def create_wordcloud(nl_title: str, en_title: str, column: str, 
                     tokenize: bool = False, 
                     value_column: Optional[str] = None, 
                     extract: Optional[Literal["url_domain"]] = None):
    return props.PropsUITextVisualization(title = props.Translatable({"en": en_title, "nl": nl_title}),
                                          type='wordcloud',
                                          text_column=column,
                                          value_column=value_column,
                                          tokenize=tokenize,
                                          extract=extract)

##################################################################
# Extraction functions
#Extracting Essential Functions
def extract_tiktok_all(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:

    tables_to_render = []

    df = tiktok.video_browsing_history_to_df(tiktok_file, validation)
    if not df.empty:
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van de video's die jij hebt gekeken op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van de video's die jij hebt gekeken op TikTok"})
        table_title = props.Translatable({"en": "Browsing Geschiedenis", "nl": "Browsing Gesciedenis"})
        table =  props.PropsUIPromptConsentFormTable("Browsing Geschiedenis", table_title, df, description)
        tables_to_render.append(table)

    df = tiktok.search_history_to_df(tiktok_file, validation)

    if not df.empty :
        table_title = props.Translatable({"en": "Zoek Geschiedenis",  "nl": "Zoek Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van waarop je hebt gezocht op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van waarop je hebt gezocht op TikTok"})        
        tables = props.PropsUIPromptConsentFormTable("Zoek Geschiedenis", table_title, df, description)
        tables_to_render.append(tables)

    df = tiktok.logging_in_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Login Geschiedenis",  "nl": "Login Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van wanneer je de TikTok hebt geopend", 
                                          "nl": "Deze tabel geeft een overzicht van wanneer je de TikTok hebt geopend"})        
        tables = props.PropsUIPromptConsentFormTable("Login Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.share_history_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Share Geschiedenis",  "nl": "Share Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van wat je hebt gedeeld op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van wat je hebt gedeeld op TikTok"})        
        tables = props.PropsUIPromptConsentFormTable("Share Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.create_follow_history(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Follow Geschiedenis", "nl": "Follow Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van jouw volgers en de mensen die jij volgt", 
                                          "nl": "Deze tabel geeft een overzicht van wat je hebt gedeeld op TikTok"})             
        table =  props.PropsUIPromptConsentFormTable("Following", table_title, df, description) 
        tables_to_render.append(table)

    df = tiktok.create_live_history(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Live Geschiedenis",  "nl": "Live Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van jouw live geschiedenis", 
                                          "nl": "Deze tabel geeft een overzicht van []"})     
        tables = props.PropsUIPromptConsentFormTable("Live Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.posting_history_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Posting Geschiedenis",  "nl": "Posting Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van jouw posts op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van []"})     
        tables = props.PropsUIPromptConsentFormTable("Posting Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.favorites_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Favorieten Geschiedenis",  "nl": "Favorieten Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van jouw favoriete items op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van []"})
        tables = props.PropsUIPromptConsentFormTable("Favorieten Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.chat_history_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "DM Geschiedenis",  "nl": "DM Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van de Direct Messages (DMs) die je hebt gestuurd/ontvangen", 
                                          "nl": "Deze tabel geeft een overzicht van []"})
        tables = props.PropsUIPromptConsentFormTable("DM Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.comment_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Comment Geschiedenis",  "nl": "Comment Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van de comments die je hebt geplaatst op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van []"})
        tables = props.PropsUIPromptConsentFormTable("Comment Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.settings_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Settings",  "nl": "Settings"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van wat belangrijke instellingen die we graag willen weten", 
                                          "nl": "Deze tabel geeft een overzicht van []"})
        tables = props.PropsUIPromptConsentFormTable("Settings", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.like_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Like Geschiedenis",  "nl": "Like Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van de video's die je hebt geliket op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van []"})
        tables = props.PropsUIPromptConsentFormTable("Settings", table_title, df, description) 
        tables_to_render.append(tables)

    return tables_to_render

def extract_tiktok_essential(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:

    tables_to_render = []

    df = tiktok.video_browsing_history_to_df(tiktok_file, validation)
    if not df.empty:
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van de video's die jij hebt gekeken op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van de video's die jij hebt gekeken op TikTok"})
        table_title = props.Translatable({"en": "Browsing Geschiedenis", "nl": "Browsing Gesciedenis"})
        table =  props.PropsUIPromptConsentFormTable("Browsing Geschiedenis", table_title, df, description)
        tables_to_render.append(table)

    df = tiktok.search_history_to_df(tiktok_file, validation)

    if not df.empty :
        table_title = props.Translatable({"en": "Zoek Geschiedenis",  "nl": "Zoek Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van waarop je hebt gezocht op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van waarop je hebt gezocht op TikTok"})        
        tables = props.PropsUIPromptConsentFormTable("Zoek Geschiedenis", table_title, df, description)
        tables_to_render.append(tables)

    df = tiktok.logging_in_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Login Geschiedenis",  "nl": "Login Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van wanneer je de TikTok hebt geopend", 
                                          "nl": "Deze tabel geeft een overzicht van wanneer je de TikTok hebt geopend"})        
        tables = props.PropsUIPromptConsentFormTable("Login Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    return tables_to_render

def extract_tiktok_extra(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:

    tables_to_render = []

    df = tiktok.share_history_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Share Geschiedenis",  "nl": "Share Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van wat je hebt gedeeld op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van wat je hebt gedeeld op TikTok"})        
        tables = props.PropsUIPromptConsentFormTable("Share Geschiedenis", table_title, df, description) 
        tables_to_render.append(tables)

    df = tiktok.create_follow_history(tiktok_file, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Follow Geschiedenis", "nl": "Follow Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van wat je hebt gedeeld op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van wat je hebt gedeeld op TikTok"})             
        table =  props.PropsUIPromptConsentFormTable("Following", table_title, df) 
        tables_to_render.append(table)

    df = tiktok.create_live_history(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Live Geschiedenis",  "nl": "Live Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van []", 
                                          "nl": "Deze tabel geeft een overzicht van []"})     
        tables = props.PropsUIPromptConsentFormTable("Live Geschiedenis", table_title, df) 
        tables_to_render.append(tables)

    df = tiktok.posting_history_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Posting Geschiedenis",  "nl": "Posting Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van []", 
                                          "nl": "Deze tabel geeft een overzicht van []"})     
        tables = props.PropsUIPromptConsentFormTable("Posting Geschiedenis", table_title, df) 
        tables_to_render.append(tables)

    df = tiktok.favorites_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Favorieten Geschiedenis",  "nl": "Favorieten Geschiedenis"})
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van []", 
                                          "nl": "Deze tabel geeft een overzicht van []"})
        tables = props.PropsUIPromptConsentFormTable("Favorieten Geschiedenis", table_title, df) 
        tables_to_render.append(tables)

    return tables_to_render

def extract_tiktok_extractall(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:

    tables_to_render = []

    df = tiktok.video_browsing_history_to_df(tiktok_file, validation)
    if not df.empty:
        description = props.Translatable({"en": "Deze tabel geeft een overzicht van de video's die jij hebt gekeken op TikTok", 
                                          "nl": "Deze tabel geeft een overzicht van de video's die jij hebt gekeken op TikTok"})
        table_title = props.Translatable({"en": "Browsing Geschiedenis", "nl": "Browsing Gesciedenis"})
        table =  props.PropsUIPromptConsentFormTable("Browsing", table_title, df, description)
        tables_to_render.append(table)

    # df = tiktok.following_to_df(tiktok_file, validation)
    # if not df.empty:
    #     table_title = props.Translatable({"en": "Following", "nl": "Following"})
    #     table =  props.PropsUIPromptConsentFormTable("Following", table_title, df) 
    #     tables_to_render.append(table)

    # df = tiktok.follower_to_df(tiktok_file, validation)
    
    # if not df.empty:
    #     table_title = props.Translatable({"en": "Followers",  "nl": "Followers"})
    #     tables = props.PropsUIPromptConsentFormTable("Followers", table_title, df)
    #     tables_to_render.append(tables)

    df = tiktok.search_history_to_df(tiktok_file, validation)

    if not df.empty :
        table_title = props.Translatable({"en": "Zoekgeschiedenis",  "nl": "Zoekgeschiedenis"})
        tables = props.PropsUIPromptConsentFormTable("Zoekgeschiedenis", table_title, df) 
        tables_to_render.append(tables)

    df = tiktok.logging_in_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Login Geschiedenis",  "nl": "Login Geschiedenis"})
        tables = props.PropsUIPromptConsentFormTable("Login Geschiedenis", table_title, df) 
        tables_to_render.append(tables)

    # df = tiktok.create_live_history(tiktok_file, validation)

    # if not df.empty:
    #     table_title = props.Translatable({"en": "Live Geschiedenis",  "nl": "Live Geschiedenis"})
    #     tables = props.PropsUIPromptConsentFormTable("Live Geschiedenis", table_title, df) 
    #     tables_to_render.append(tables)

    # df = tiktok.posting_history_to_df(tiktok_file, validation)

    # if not df.empty:
    #     table_title = props.Translatable({"en": "Posting Geschiedenis",  "nl": "Posting Geschiedenis"})
    #     tables = props.PropsUIPromptConsentFormTable("Posting Geschiedenis", table_title, df) 
    #     tables_to_render.append(tables)

    df = tiktok.share_history_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Wat je hebt gedeeld",  "nl": "Wat je hebt gedeeld"})
        tables = props.PropsUIPromptConsentFormTable("Share History", table_title, df) 
        tables_to_render.append(tables)

    return tables_to_render

    # df = tiktok.favorites_to_df(tiktok_file, validation)

    # if not df.empty:
    #     table_title = props.Translatable({"en": "Favorieten",  "nl": "Favorieten"})
    #     tables = props.PropsUIPromptConsentFormTable("Favorieten", table_title, df) 
    #     tables_to_render.append(tables)

    #return tables_to_render

def extract_tiktok_follow(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.create_follow_history(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Follow Geschiedenis",  "nl": "Follow Geschiedenis"})
        tables = create_consent_form_tables("Follow Geschiedenis", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def extract_tiktok_follower(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.follower_to_df(tiktok_file, validation)
    
    if not df.empty:
        table_title = props.Translatable({"en": "Browsing-geschiedenis",  "nl": "Browsing-geschiedenis"})
        tables = create_consent_form_tables("Browsing History", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render

#1. browsing history
def extract_tiktok_following(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.following_to_df(tiktok_file, validation)
    
    if not df.empty:
        table_title = props.Translatable({"en": "Browsing-geschiedenis",  "nl": "Browsing-geschiedenis"})
        tables = create_consent_form_tables("Browsing History", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


####testing
    df = youtube.watch_history_to_df(youtube_zip, validation)
    if not df.empty:
        table_title = props.Translatable({"en": "Youtube watch history", "nl": "Youtube watch history"})
        vis = [create_chart("area", "Youtube videos bekeken", "Youtube videos watched", "Date standard format", y_label="Aantal videos", date_format="auto"),
               create_chart("bar", "Activiteit per uur van de dag", "Activity per hour of the day", "Date standard format", y_label="Aantal videos", date_format="hour_cycle"),
               create_wordcloud("Meest bekeken kanalen", "Most watched channels", "Channel")]
        table =  props.PropsUIPromptConsentFormTable("youtube_watch_history", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

def extract_tiktok_browsing(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.video_browsing_history_to_df(tiktok_file, validation)
    
    if not df.empty:
        table_title = props.Translatable({"en": "Browsing-geschiedenis",  "nl": "Browsing-geschiedenis"})
        tables = create_consent_form_tables("Browsing History", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def create_wordcloud(nl_title: str, en_title: str, column: str, 
                     tokenize: bool = False, 
                     value_column: Optional[str] = None, 
                     extract: Optional[Literal["Link naar video"]] = None):
    return props.PropsUITextVisualization(title = props.Translatable({"en": en_title, "nl": nl_title}),
                                          type='wordcloud',
                                          text_column=column,
                                          value_column=value_column,
                                          tokenize=tokenize,
                                          extract=extract)

def chart_test(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:

    # df = tiktok.logging_in_to_df(tiktok_file, validation)

    # app_opened = len(df["Datum"])

    # text1 = props.Translatable(
    #     {
    #         "en": f"Hoe vaak de app geopend: {app_opened}",
    #         "nl": f"",
    #     }
    # )
    
    # ok = props.Translatable({"en": "",  "nl": ""})
    # cancel = props.Translatable({"en": "Ik wil verder gaan",  "nl": "Ik wil verder gaan"})

    # summary = props.PropsUIPromptConfirm(text1, ok, cancel)

    tables_to_render = []

    df1 = tiktok.video_browsing_history_to_df(tiktok_file, validation)

    if not df1.empty:
        table_title = props.Translatable({"en": "Browsing",  "nl": "Browsing"})
        table_description = props.Translatable({
            "nl": "Check hier hoeveel filmpjes je hebt gekeken op TikTok! Kijk jij vooral filmpjes overdag of in de avond?", 
            "en": "Check hier hoeveel filmpjes je hebt gekeken op TikTok! Kijk jij vooral filmpjes overdag of in de avond?"
        })
        vis = [create_chart(type = "bar", 
                            nl_title="In welke maanden kijk jij de meeste filmpjes?", 
                            en_title="In welke maanden kijk jij de meeste filmpjes?", 
                            x="Datum",
                            x_label="Maand",
                            y_label = "Hoe vaak livestreams", 
                            date_format="month_cycle"),
                create_chart(type = "bar", 
                            nl_title="En op welk uur van de dag?", 
                            en_title="En op welk uur van de dag?", 
                            x="Datum",
                            x_label="Uur",
                            y_label = "Hoe vaak livestreams", 
                            date_format="hour_cycle", addZeroes=True)]
        tables = props.PropsUIPromptConsentFormTable("Zoekgeschiedenis", table_title, df1, description = table_description, visualizations=vis) 
        tables_to_render.append(tables)

    df = tiktok.search_history_to_df(tiktok_file, validation)
    
    if not df.empty:
        table_title = props.Translatable({"en": "Zoekgeschiedenis",  "nl": "Zoekgeschiedenis"})
        table_description = props.Translatable({
            "nl": "Check hier hoe vaak je hebt gezocht op TikTok over de tijd heen! In welke maanden en uren zocht jij het meest?", 
            "en": "Check hier hoe vaak je hebt gezocht op TikTok over de tijd heen! In welke maanden en uren zocht jij het meest?"
        })
        vis = [create_wordcloud("Meest gebruikte zoektermen", "Meest gebruikte zoektermen", "Zoekterm")]
        tables = props.PropsUIPromptConsentFormTable("Zoekgeschiedenis", table_title, df, description = table_description, visualizations=vis) 
        tables_to_render.append(tables)

    return tables_to_render

    # type: Literal["bar", "line", "area"], 
    #              nl_title: str, en_title: str, 
    #              x: str, y: Optional[str] = None, 
    #              x_label: Optional[str] = None, y_label: Optional[str] = None,
    #              date_format: Optional[str] = None, aggregate: str = "count", addZeroes: bool = False

#2. search history
def extract_tiktok_search(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.search_history_to_df(tiktok_file, validation)

    if not df.empty :
        table_title = props.Translatable({"en": "Zoekgeschiedenis",  "nl": "Zoekgeschiedenis"})
        tables = create_consent_form_tables("Search History", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render

#3. login history
def extract_tiktok_log(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.logging_in_to_df(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Login geschiedenis",  "nl": "Login geschiedenis"})
        tables = create_consent_form_tables("Login geschiedenis", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render

#4. live history
def extract_tiktok_live(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.create_live_history(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Live geschiedenis",  "nl": "Live geschiedenis"})
        tables = create_consent_form_tables("Live geschiedenis", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


#search history - anonymous
def extract_tiktok_search_anon(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.search_history_to_df_anon(tiktok_file, validation)

    if not df.empty :
        table_title = props.Translatable({"en": "Zoekgeschiedenis",  "nl": "Zoekgeschiedenis"})
        tables = create_consent_form_tables("Search History", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render


def extract_tiktok_log2(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = tiktok.logging_in_to_df2(tiktok_file, validation)

    if not df.empty:
        table_title = props.Translatable({"en": "Login geschiedenis",  "nl": "Login geschiedenis"})
        tables = create_consent_form_tables("Login geschiedenis", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render

#test
# def summary(tiktok_file: str, validation: validate.ValidateInput) -> list[props.PropsUIPromptConsentFormTable]:
#     tables_to_render = []

#     df = tiktok.logging_in_to_df2(tiktok_file, validation)

#     if not df.empty:
#         table_title = props.Translatable({"en": "Login geschiedenis",  "nl": "Login geschiedenis"})
#         tables = create_consent_form_tables("Login geschiedenis", table_title, df) 
#         tables_to_render.extend(tables)

#     return tables_to_render

def summary(tiktok_file: str, validation: validate.ValidateInput):

    df = tiktok.logging_in_to_df(tiktok_file, validation)

    app_opened = len(df["Datum"])

    text1 = props.Translatable(
        {
            "en": f"Hoe vaak de app geopend: {app_opened}",
            "nl": f"",
        }
    )
    
    ok = props.Translatable({"en": "",  "nl": ""})
    cancel = props.Translatable({"en": "Ik wil verder gaan",  "nl": "Ik wil verder gaan"})
    vis = [create_chart(type = "line", nl_title="test", en_title="test", x="Date", y_label = "Aantal videos", date_format="auto")]
    return props.PropsUIPromptConfirm(text1, ok, cancel, vis)

from typing import Optional, Literal
#chart test

def create_chart(type: Literal["bar", "line", "area"], 
                 nl_title: str, en_title: str, 
                 x: str, y: Optional[str] = None, 
                 x_label: Optional[str] = None, y_label: Optional[str] = None,
                 date_format: Optional[str] = None, aggregate: str = "count", addZeroes: bool = False):
    if y is None:
        y = x
        if aggregate != "count": 
            raise ValueError("If y is None, aggregate must be count if y is not specified")
        
    return props.PropsUIChartVisualization(
        title = props.Translatable({"en": en_title, "nl": nl_title}),
        type = type,
        group = props.PropsUIChartGroup(column= x, label= x_label, dateFormat= date_format),
        values = [props.PropsUIChartValue(column= y, label= y_label, aggregate= aggregate, addZeroes= addZeroes)]       
    )

##########################################
# Functions provided by Eyra did not change

def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)

def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({"en": platform,  "nl": platform}))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)
    
def retry_confirmation(foutje):
    text = props.Translatable(
        {
            "en": f"Er is iets mis met je TikTok data. Neem even contact met ons op via WhatsApp (06-12345678)",
            "nl": f"Er is iets mis met je TikTok data. Neem even contact met ons op via WhatsApp (06-12345678)"
        }
    )
    ok = props.Translatable({"en": "Probeer opnieuw",  "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Ik wil toch niets doneren",  "nl": "Ik wil toch niets doneren"})
    return props.PropsUIPromptConfirm(text, ok, cancel)

def retry_confirmation_data_conditions_not_met(platform):
    text = props.Translatable(
        {
            "en": f"Er is iets mis met je TikTok data. Neem even contact met ons op via WhatsApp (06-12345678)",
            "nl": f"Er is iets mis met je TikTok data. Neem even contact met ons op via WhatsApp (06-12345678)",
        }
    )
    ok = props.Translatable({"en": "",  "nl": ""})
    cancel = props.Translatable({"en": "Continue",  "nl": "Weiter"})
    return props.PropsUIPromptConfirm(text, ok, cancel)

def helaas(platform_name):
    text = props.Translatable(
        {
            "en": f"Helaas kan je niet meedoen met ons onderzoek, omdat je niet de belangrijke TikTok data met ons wilde delen. Wilde je wel doneren maar was dit een foutje? Neem dan even contact met ons op via WhatsApp",
            "nl": f"Helaas kan je niet meedoen met ons onderzoek, omdat je niet de belangrijke TikTok data met ons wilde delen. Wilde je wel doneren maar was dit een foutje? Neem dan even contact met ons op via WhatsApp",
        }
    )
    ok = props.Translatable({"en": "",  "nl": ""})
    cancel = props.Translatable({"en": "",  "nl": ""})
    return props.PropsUIPromptConfirm(text, ok, cancel)

def in_stukjes(platform):
    text = props.Translatable(
        {
            "en": f'Jammer dat je niet al deze TikTok data met ons wilt delen. Er zijn wel wat onderdelen van je TikTok data die belangrijk zijn, die zie je op de volgende pagina. Als jij deze onderdelen niet wilt doneren, kan je dus ook helaas niet mee doen met ons onderzoek.',
            "nl": f'Jammer dat je niet al deze TikTok data met ons wilt delen. Er zijn wel wat onderdelen van je TikTok data die belangrijk zijn, die zie je op de volgende pagina. Als jij deze onderdelen niet wilt doneren, kan je dus ook helaas niet mee doen met ons onderzoek.'
        }
    )
    ok = props.Translatable({"en": "Ga verder", "nl": "Ga verder"})
    cancel = props.Translatable({"en": "", "nl": ""})
    return props.PropsUIPromptConfirm(text, ok, cancel)

def extra(platform):
    text = props.Translatable(
        {
            "en": f'Bedankt dat je deze data met ons wilde delen! Er is ook wat data die je niet verplicht met ons hoeft te delen. We zouden het wel heel fijn vinden als je deze data met ons zou willen delen, omdat we dan nog beter de effecten van social media kunnen onderzoeken',
            "nl": f'Bedankt dat je deze data met ons wilde delen! Er is ook wat data die je niet verplicht met ons hoeft te delen. We zouden het wel heel fijn vinden als je deze data met ons zou willen delen, omdat we dan nog beter de effecten van social media kunnen onderzoeken'
        }
    )
    ok = props.Translatable({"en": "Ga verder", "nl": "Ga verder"})
    cancel = props.Translatable({"en": "", "nl": ""})
    return props.PropsUIPromptConfirm(text, ok, cancel)

# def in_stukjes(platform):
#     text = props.Translatable(
#         {
#             "en": f"Jammer dat je niet al je TikTok data met ons wilt delen. Er zijn wel wat delen die essentieel zijn. Zou je deze wel met ons willen delen",
#             "nl": f"Jammer dat je niet al je TikTok data met ons wilt delen. Er zijn wel wat delen die essentieel zijn. Zou je deze wel met ons willen delen"
#         }
#     )
#     ok = props.Translatable({"en": "Ja, hoor!", "nl": "Ja, hoor!"})
#     cancel = props.Translatable({"en": "Nee, dat wil ik ook niet", "nl": "Nee, dat wil ik ook niet"})
#     return props.PropsUIPromptConfirm(text, ok, cancel)

def prompt_file(description, extensions):
    description = props.Translatable(
        {
            "en": f"Selecteer hieronder het bestand dat je van TikTok hebt ontvangen",
            "nl": f"Selecteer hieronder het bestand dat je van TikTok hebt ontvangen"
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)

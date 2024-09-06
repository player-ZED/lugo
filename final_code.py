from tkinter import N
from rocketapi.exceptions import BadResponseException
from datetime import datetime, timedelta
from rocketapi import InstagramAPI
from supabase import create_client
import pandas as pd
import math
import time
import logging
import json
import re
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

url = "https://wnekuwbqcukhtcftrdfb.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InduZWt1d2JxY3VraHRjZnRyZGZiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MTU3NTg2OTcsImV4cCI6MjAzMTMzNDY5N30.nA5yGKLJNPD6tdf3T1Lt8f0DfDO-kdNP2BqrDlugdOA"
instagram_api = InstagramAPI(token="E-sLdFWuUk42ymmZJPKGsA")
supabase = create_client(url, key)

logging.basicConfig(
    filename='app.log',         # Log file name
    filemode='a',               # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO          # Minimum logging level
)

#! ----- getting all account ids from supabase -----
response_supa = (
    supabase.table("accounts_ig_creators")
    .select("account_id")
    .execute()
)
data0 = response_supa.data
df = pd.DataFrame(data0)
account_ids = df["account_id"].tolist()

with open('no_acc.txt', 'r') as f:
    skipped_ids = [line.strip() for line in f.readlines()]

try:
    
    for acc_id in account_ids:
    
        #! ----- getting an account from supabase -----
        account_username = None
        account_id = None
        account_SN_number = None
        creator_name = None
        try:    
            response_supa = supabase.table("accounts_ig_creators").select("*").eq("account_id", acc_id).execute()
            data1 = response_supa.data
            df = pd.DataFrame(data1)

            account_SN_number = data1[0]["account_id"]
            creator_name = data1[0]["creator_name"]
            account_username = data1[0]["account_username"]
            if account_username in skipped_ids:
                    print(f"Skipping account ID {acc_id} as it is in the no_acc.txt file.")
                    continue 
            
            phone_id = data1[0]["phone_id"]
            print(account_username)
        except Exception as e:
            logging.error(f"Error in getting account from supabase: {e}")
            print(f"Error in getting account from supabase: {e}")
            continue

        #! ----- getting accounts info from Rocket API -----
        try:
            for attempt in range(3):
                try:
                    profile_data_raw = instagram_api.get_user_info(account_username)
                    continue
                except BadResponseException:
                    print(f"REEL : Bad response from RocketAPI. Attempt {attempt + 1} of 3. Retrying in 1 second...")
                    logging.error(f"{account_username} :  REEL : Bad response from RocketAPI. Attempt {attempt + 1} of 3. Retrying in 1 second... : %s", e)
                    print(f"{account_username} :  REEL : Bad response from RocketAPI. Attempt {attempt + 1} of 3. Retrying in 1 second... : %s", e)
                    time.sleep(1)
                except Exception as e:
                    print(f"Error: {e}")
                    logging.error(f"{account_username} :  profile API error...skipping: %s", e)
                    print(f"{account_username} :  profile API error...skipping: %s", e)
                    continue
            
            try:
                followers_count = profile_data_raw["data"]["user"]["edge_followed_by"]["count"]
            except Exception as e:
                logging.error(f"{account_username} : No account data...skipping: %s", e)
                print(f"{account_username} : No account data...skipping: %s", e)
                continue
                
            num_of_media = profile_data_raw["data"]["user"]["edge_owner_to_timeline_media"]["count"]
            account_id = profile_data_raw["data"]["user"]["id"]
            print(account_id)
        except Exception as e:
            logging.error(f"{account_username} :  Error while getting account info from rocket API ...skipping")
            print(f"{account_username} :  Error while getting account info from rocket API ...skipping", )
            continue

        #! getting num of stories
        try:   
            for attempt2 in range(3):
                try:
                    story_data_raw = instagram_api.get_user_stories(int(account_id))
                    continue
                except BadResponseException:
                    print(f"STORY : Bad response from RocketAPI. Attempt {attempt2 + 1} of 3. Retrying in 1 second...")
                    time.sleep(1)
                except Exception as e:
                    print(f"Error: {e}")
                    continue

            if not story_data_raw["reels"]:    
                print("No Story")
                num_story = 0
            else:
                num_story = story_data_raw["reels"][account_id]["media_count"]

            num_of_reels_to_show = 12
            reels_data_raw = instagram_api.get_user_clips(account_id, count= num_of_reels_to_show )
        except Exception as e:
            logging.error(f"{account_username} :  Error while getting num of stories: %s", e)
            print(f"{account_username} :  Error while getting num of stories: %s", e)

        #! getting reels data
        try:
            current_date_time = datetime.now()
            date_to_check = (current_date_time.day) 
            reels_counter = 0
            reels_today = []
            if reels_data_raw["items"]:  # Check if the list is not empty
                for reel_num in range(min(num_of_reels_to_show, len(reels_data_raw["items"]))):
                    taken_at_raw = reels_data_raw["items"][reel_num]["media"]
                    taken_at = taken_at_raw["taken_at"]
                    date_time = datetime.fromtimestamp(taken_at)
                    date_only = date_time.day
                    
                    if current_date_time.day == 1:
                        date_to_check = (current_date_time - timedelta(days=1)).day
                        month_to_change = True
                    else:
                        date_to_check = current_date_time.day - 1
                        month_to_change = False
                    
                    if (date_only == date_to_check):
                        reels_counter += 1 
                        play_count = taken_at_raw["play_count"]
                        likes_reel = taken_at_raw["like_count"]
                        comments_reel = taken_at_raw["comment_count"]

                        if play_count != 0:
                            average_engagement_rate_raw = ((likes_reel + comments_reel) / play_count) * 100
                        else:
                            average_engagement_rate_raw = 0
                        average_engagement_rate = round(average_engagement_rate_raw, 2)
                        if likes_reel != 0:
                            ratio = comments_reel / likes_reel
                        else:
                            ratio = 0

                        media_id = taken_at_raw["caption"]["media_id"]
                        def simplify_ratio(num, denom):
                            if denom == 0:
                                return num, 0
                            gcd = math.gcd(num, denom)
                            if gcd == 0:
                                return num, denom
                            return num // gcd, denom // gcd
                        
                        x, y = simplify_ratio(comments_reel, likes_reel)
                        reels_today.append({
                            "date": date_time.strftime('%Y-%m-%d'),
                            "media_id" : media_id,
                            "account_id" : account_id,
                            "account_name" : account_username,
                            "play_count": play_count,
                            "likes" : likes_reel,
                            "comments" : comments_reel,
                            "avg_eng_rate" : average_engagement_rate,
                            "comments_to_likes_ratio" : f"{x}:{y}"
                        })
        except Exception as e:
            logging.error(f"{account_username} :  Error while getting reels data: %s", e)
            print(f"{account_username} :  Error while getting reels data: %s", e)

        #! code to check for previous day data and compare.
        try:
            # Check if the record with the given profile_name exists
            response = (
                supabase.table("profiles")
                .select("profile_name, total_stories_today, total_media_today")
                .eq("profile_name", account_username)
                .single()  # Use .single() since profile_name is unique
                .execute()
            )

            existing_data = response.data
        except:
            existing_data = None    

        try:
            if existing_data:
                # Record exists, perform an update
                total_stories_yesterday = existing_data.get("total_stories_today", 0)
                total_media_yesterday = existing_data.get("total_media_today", 0)
                reels_id_list = [reels_d["media_id"] for reels_d in reels_today]
                update_response = (
                    supabase.table("profiles")
                    .update({
                        "squad": creator_name,
                        "phone_number": phone_id,
                        "total_posts": reels_counter,
                        "total_stories_today": num_story,
                        "total_stories_yesterday": total_stories_yesterday,
                        "media_ids": reels_id_list,
                        "total_media_today": reels_counter,
                        "total_media_yesterday": total_media_yesterday,
                    })
                    .eq("profile_name", account_username)
                    .execute()
                )
                print(f"Record with profile_name '{account_username}' updated.")
            else:
                # Record does not exist, perform an insert
                insert_response = (
                    supabase.table("profiles")
                    .insert({
                        "profile_name": account_username,
                        "squad": creator_name,
                        "phone_number": phone_id,
                        "account_id": account_id,
                        "total_posts": reels_counter,
                        "total_stories_today": num_story,
                        "total_stories_today": 0,  
                        "total_media_today": 0, 
                        "total_stories_yesterday": 0, 
                        "total_media_yesterday": 0,  # New record, so previous values are zero
                    })
                    .execute()
                )
                print(f"New record with profile_name '{account_username}' inserted.")

            # Retrieve and print the updated data
            response = (
                supabase.table("profiles")
                .select("total_stories_today, total_media_today")
                .eq("account_id", account_id)
                .single()  # Use .single() to get a single record
                .execute()
            )

            if response.data:
                data_st = response.data
                total_stories_yesterday = data_st.get("total_stories_today", 0)
                total_media_yesterday = data_st.get("total_media_today", 0)
            else:
                total_stories_yesterday = 0
                total_media_yesterday = 0
        except Exception as e:
            logging.error(f"{account_username} :  Error while updating profiles table: %s", e)
            print(f"{account_username} : Error while updating profiles table: %s", e)

        #! sending reels data to supabase
        try:
            if reels_today:
                for single_media in reels_today:
                    
                    # Values to check
                    column1_value = single_media["date"]
                    column2_value = single_media["media_id"]
                    
                    # Query the table to see if any row has these two column values
                    response = (
                        supabase.table("media_table")
                        .select("*")
                        .eq("date", column1_value)
                        .eq("media_id", column2_value)
                        .execute()
                    )

                    # If no rows are returned, insert the new row
                    if len(response.data) == 0:
                        # Add the new row
                        new_row = {
                            "date": single_media["date"],
                            "account_id": single_media["account_id"],
                            "account_name" :single_media["account_name"],
                            "media_id" : single_media["media_id"],
                            "comment_today" :single_media["comments"],
                            "likes_today" : single_media["likes"],
                            "media_play_today" : single_media["play_count"],
                            "avg_eng_rate_today" : single_media["avg_eng_rate"],
                            "comments_to_likes_ratio" : single_media["comments_to_likes_ratio"]
                        }
                        
                        insert_response = supabase.table("media_table").insert(new_row).execute()
                        
                    else:
                        print("Row with these values already exists.")
            else:
                print("No reels for today !!")

        except Exception as e:
            logging.error(f"{account_username} :  Error while sending reels data to supabase: %s", e)
            print(f"{account_username} : Error while sending reels data to supabase: %s", e)

        #! check if previous 2 days data is present
        try:
            response = (
                supabase.table("media_table")
                .select("date")
                .eq("account_id", account_id)
                .execute()
            )

            data1 = response.data
            today = datetime.now()
            days = [int(item['date'].split('-')[2]) for item in data1]  # Convert days to integers

            yesterday = today - timedelta(days=1)
            date_list = [(yesterday - timedelta(days=i)).day for i in range(1)]
        except Exception as e:
            logging.error(f"{account_username} :  Error while checking previous 2 days data: %s", e)
            print(f"{account_username} : Error while checking previous 2 days data: %s", e)

        try:
            if today.day == 1:
                month_to_change = True
                date_to_check = (today - timedelta(days=1)).day
            else:
                date_to_check = today.day - 1
            #! shifting only works if there is media data of previous day
            if all(day in days for day in date_list):

                #! getting today data and shifting today -> yearstaday in supabase
                for reel_num2 in range(num_of_reels_to_show):
                    taken_at_raw2 = reels_data_raw["items"][reel_num2]["media"]
                    taken_at2 = taken_at_raw2["taken_at"]
                    date_time2 = datetime.fromtimestamp(taken_at2)
                    date_only2 = date_time2.day
                    if (date_only2 == date_to_check - 1):
                        play_count = taken_at_raw2["play_count"]
                        likes_reel = taken_at_raw2["like_count"]
                        comments_reel = taken_at_raw2["comment_count"]
                        if play_count != 0:
                            average_engagement_rate_raw = ((likes_reel + comments_reel) / play_count) * 100
                            average_engagement_rate = round(average_engagement_rate_raw, 2)
                        else:
                            average_engagement_rate = 0 

                        #! Shifting data from today -> yesterday
                        today = datetime.now()
                        if month_to_change:
                            # If month changed, adjust the full_date accordingly
                            full_date = datetime(today.year, today.month - 1, date_to_check -1 )
                        else:
                            full_date = datetime(today.year, today.month, date_to_check -1 )

                        full_date_str = full_date.strftime("%Y-%m-%d")

                        try:
                            response_comp = (
                                supabase.table("media_table")
                                .select("id,comment_today, likes_today, media_play_today, avg_eng_rate_today")
                                .eq("date",full_date_str )  
                                .eq("media_id",  taken_at_raw2["caption"]["media_id"])
                                .single()  
                                .execute()
                            )
                            data_comp_raw = response_comp.data
                        except:
                            data_comp_raw = None

                        if data_comp_raw:
                            # Check if values are not null, None, or 0 before updating
                            try:
                                update_data = {
                                    "comments_yesterday": data_comp_raw[0]["comment_today"] if data_comp_raw[0]["comment_today"] not in [None, 0] else None,
                                    "likes_yesterday": data_comp_raw[0]["likes_today"] if data_comp_raw[0]["likes_today"] not in [None, 0] else None,
                                    "media_play_yesterday": data_comp_raw[0]["media_play_today"] if data_comp_raw[0]["media_play_today"] not in [None, 0] else None,
                                    "avg_eng_rate_yesterday": data_comp_raw[0]["avg_eng_rate_today"] if data_comp_raw[0]["avg_eng_rate_today"] not in [None, 0] else None,
                                }
                            except:
                                update_data = {
                                    "comments_yesterday": data_comp_raw["comment_today"] if data_comp_raw["comment_today"] not in [None, 0] else None,
                                    "likes_yesterday": data_comp_raw["likes_today"] if data_comp_raw["likes_today"] not in [None, 0] else None,
                                    "media_play_yesterday": data_comp_raw["media_play_today"] if data_comp_raw["media_play_today"] not in [None, 0] else None,
                                    "avg_eng_rate_yesterday": data_comp_raw["avg_eng_rate_today"] if data_comp_raw["avg_eng_rate_today"] not in [None, 0] else None,
                                }

                            # Update the row by shifting the value only if the value is valid
                            update_data = {k: v for k, v in update_data.items() if v is not None}

                            if update_data:
                                update_data.update({
                                    "comment_today": comments_reel,
                                    "likes_today": likes_reel,
                                    "media_play_today": play_count,
                                    "avg_eng_rate_today": average_engagement_rate,
                                })
                                
                                try:
                                    update_response = (
                                        supabase.table("media_table")
                                        .update(update_data)
                                        .eq("id", data_comp_raw[0]["id"])
                                        .execute()
                                    )
                                except:
                                    update_response = (
                                        supabase.table("media_table")
                                        .update(update_data)
                                        .eq("id", data_comp_raw["id"])
                                        .execute()
                                    )
                            else:
                                print("No valid data to update.")
                        else:
                            print("Row not found in media table.")
            else:
                
                print("yesterday media data is not present")

        except Exception as e:
            logging.error(f"{account_username} :  Error while shifting data: %s", e)
            print(f"{account_username} :  Error while shifting data: %s", e)

        #! deleting reels data older than week from supabase
        try:
            date_2_del = datetime(today.year, today.month, date_to_check) - timedelta(days=4)
            four_day_ago = date_2_del.strftime("%Y-%m-%d")
            response_del = (
                supabase.table("media_table")
                .delete()
                .lt("date", four_day_ago)
                .execute()
            )
        except Exception as e:
            logging.error(f"{account_username} :  Error while deleting old reels data: %s", e)
            print(f"{account_username} : Error while deleting old reels data: %s", e)

        #! getting followers change
        try:
        # Fetch current followers data from Supabase
            response = (
                supabase.table("profiles")
                .select("followers_today, followers_yesterday, followers_change_today, followers_change_yesterday")
                .eq("account_id", account_id)
                .single()
                .execute()
            )

            if response.data:
                data = response.data
                followers_today = data.get("followers_today")
                followers_yesterday = data.get("followers_yesterday")
                followers_change_today = data.get("followers_change_today")

                new_followers_yesterday = followers_today if followers_today is not None else 0
                new_followers_today = followers_count if followers_count is not None else 0 
                followers_change_today_new = new_followers_today - new_followers_yesterday

                update_data = {
                    "followers_yesterday": new_followers_yesterday,
                    "followers_today": new_followers_today,
                    "followers_change_today": followers_change_today_new,
                    "followers_change_yesterday": followers_change_today,
                }

                # Update the row in Supabase
                update_response = (
                    supabase.table("profiles")
                    .update(update_data)
                    .eq("account_id", account_id)
                    .execute()
                )

            else:
                print("No data found for the given account ID.")

        except Exception as e:
            logging.error(f"{account_username} : Error while getting followers change: %s", e)
            print(f"{account_username} : Error while getting followers change: %s", e)

        #! getting evg_eng_rate, media_views etc from media_table to fill profile table
        try:
            def parse_ratio(ratio_str):
                if ratio_str:
                    parts = ratio_str.split(':')
                    if len(parts) == 2:
                        return float(parts[0]), float(parts[1])
                return 0, 0

            today = datetime.now()
            if today.day == 1:
                previous_day = today - timedelta(days=1)
            else:
                previous_day = today.replace(day=today.day - 1)

            formatted_date = previous_day.strftime("%Y-%m-%d")

            response = (
                supabase.table("media_table")
                .select("avg_eng_rate_today, avg_eng_rate_yesterday, comments_to_likes_ratio, media_play_yesterday, media_play_today")
                .eq("account_id", account_id)
                .eq("date", formatted_date)
                .execute()
            )

            media_data = response.data
            total_avg_eng_rate_today = 0
            total_avg_eng_rate_yesterday = 0
            total_comments_to_likes_ratio = 0
            numerator_comments = 0
            denominator_likes = 0
            total_media_play_today = 0
            total_media_play_yesterday = 0
            count = 0

            # Sum up the values
            for item in media_data:
                if item["avg_eng_rate_today"] is not None:
                    total_avg_eng_rate_today += item["avg_eng_rate_today"]
                if item["avg_eng_rate_yesterday"] is not None:
                    total_avg_eng_rate_yesterday += item["avg_eng_rate_yesterday"]
                if item["comments_to_likes_ratio"] is not None:
                    numerator, denominator = parse_ratio(item["comments_to_likes_ratio"])
                    numerator_comments += numerator
                    denominator_likes += denominator
                if item["media_play_today"] is not None:
                    total_media_play_today += item["media_play_today"]
                if item["media_play_yesterday"] is not None:
                    total_media_play_yesterday += item["media_play_yesterday"]
                count += 1

            # Calculate averages if needed
            if count > 0:
                avg_avg_eng_rate_today = round(total_avg_eng_rate_today / count,2) 
                avg_avg_eng_rate_yesterday = round(total_avg_eng_rate_yesterday / count,2)
                avg_comments_to_likes_ratio = f"{round(numerator_comments / count)}:{round(denominator_likes / count)}"
            else:
                avg_avg_eng_rate_today = 0
                avg_avg_eng_rate_yesterday = 0
                avg_comments_to_likes_ratio = 0

            # Step 3: Update the `profiles` Table
            profile_update_data = {
                "avg_engagement_rate_today": avg_avg_eng_rate_today,
                "avg_engagement_rate_yesterday": avg_avg_eng_rate_yesterday,
                "comment_to_like_ratio": avg_comments_to_likes_ratio,
                "post_views_today": total_media_play_today,
                "post_views_yesterday": total_media_play_yesterday
            }

            update_response = (
                supabase.table("profiles")
                .update(profile_update_data)
                .eq("account_id", account_id)
                .execute()
            )
                        
            #! getting media_views, total_comments, likes from media tbale to profile table
            if month_to_change:
                target_date1 = datetime(today.year, today.month - 1, date_to_check)
            else:
                target_date1 = datetime(today.year, today.month, date_to_check)

            formatted_date1 = target_date1.strftime("%Y-%m-%d")

            response = (
                supabase.table("media_table")
                .select("comment_today, likes_today")
                .eq("account_id", account_id)
                .eq("date", formatted_date1)
                .execute()
            )

            media_data = response.data

            total_comments_today = 0
            total_likes_today = 0

            # Sum up the values
            for item in media_data:
                if item["comment_today"] is not None:
                    total_comments_today += item["comment_today"]
                if item["likes_today"] is not None:
                    total_likes_today += item["likes_today"]

            # Step 3: Update the `profiles` Table
            profile_update_data = {
                "total_comments_today": total_comments_today,
                "total_likes_today": total_likes_today
            }

            update_response = (
                supabase.table("profiles")
                .update(profile_update_data)
                .eq("account_id", account_id)
                .execute()
            )

        except Exception as e:
            logging.error(f"{account_username} :  Error while getting evg_eng_rate, media_views etc from media_table: %s", e)
            print(f"{account_username} : Error while getting evg_eng_rate, media_views etc from media_table: %s", e)   

        #! for loop ends here

except Exception as e:
        logging.error(f"Erorr on whole main loop: %s", e)
        print(f"Erorr on whole main loop: %s", e)

try:
    # Retrieve phone numbers from profile_creators
    profile_creators = supabase.table('phones_creators').select('phone_number').execute().data
    phone_numbers = [row['phone_number'] for row in profile_creators]

    # Initialize data storage for phone numbers
    phone_data = {}

    # Retrieve phone numbers and creator names from profile_creators
    profile_creators = supabase.table('phones_creators').select('phone_number, creator_name').execute().data

    # Initialize data storage for phone numbers with creator names
    phone_data = {}
    for row in profile_creators:
        phone_number = row['phone_number']
        creator_name = row['creator_name']
        phone_data[phone_number] = {
            'creator_name': creator_name,
            'totals': {
                'total_posts': 0,
                'total_stories': 0,
                'new_followers': 0,
                'avg_eng_rate': 0,
                'post_views_today': 0,
                'total_comments_today': 0,
                'total_likes_today': 0,
                'total_accounts': 0
            }
        }
except Exception as e:
    logging.error("error after the loop ( 2nd try-except)", e)
    print("error after the loop ( 2nd try-except)", e)

try:
    # Collect data for each phone number
    for phone_number in phone_data.keys():
        profiles = (
            supabase.table('profiles')
            .select('*')
            .eq('phone_number', phone_number)
            .execute()
            .data
        )

        totals = {
            'total_posts': 0,
            'total_stories': 0,
            'new_followers': 0,
            'avg_eng_rate': 0,
            'post_views_today': 0,
            'total_comments_today': 0,
            'total_likes_today': 0,
            'total_accounts': len(profiles)  # Number of profiles
        }
        engagement_rate_sum = 0
        profile_count = 0

        for profile in profiles:
            totals['total_posts'] += profile.get('total_posts', 0) or 0
            totals['total_stories'] += profile.get('total_stories_today', 0) or 0
            totals['new_followers'] += profile.get('followers_change_today', 0) or 0
            totals['post_views_today'] += profile.get('post_views_today', 0) or 0 
            totals['total_comments_today'] += profile.get('total_comments_today', 0) or 0
            totals['total_likes_today'] += profile.get('total_likes_today', 0) or 0

            if profile.get('avg_engagement_rate_today') is not None:
                engagement_rate_sum += profile.get('avg_engagement_rate_today', 0)
                profile_count += 1

        if profile_count > 0:
            totals['avg_eng_rate'] = round(engagement_rate_sum / profile_count, 2)
        else:
            totals['avg_eng_rate'] = 0

        phone_data[phone_number]['totals'] = totals

except Exception as e:
    logging.error("error while collecting data for each phone number ( 3nd try-except)", e)
    print("error while collecting data for each phone number ( 3nd try-except)", e)
    
try:
    # Update phone_wise_data table
    for phone_number, data in phone_data.items():
        # Retrieve creator name
        creator_name = data['creator_name']
        totals = data['totals']

        # Shift today's data to yesterday's columns
        existing_record = (
            supabase.table('phone_wise_data')
            .select('*')
            .eq('phone_number', phone_number)
            .execute()
            .data
        )

        if existing_record:
            # Update the existing record by shifting today's data to yesterday's
            update_response = (
                supabase.table('phone_wise_data')
                .update({
                    'total_posts_yesterday': existing_record[0].get('total_posts_today', 0),
                    'total_stories_yesterday': existing_record[0].get('total_stories_today', 0),
                    'new_followers_yesterday': existing_record[0].get('new_followers_today', 0),
                    'avg_eng_rate_yesterday': existing_record[0].get('avg_eng_rate_today', 0),
                    'post_views_yesterday': existing_record[0].get('post_views_today', 0),
                    'total_comments_yesterday': existing_record[0].get('total_comments_today', 0),
                    'total_likes_yesterday': existing_record[0].get('total_likes_today', 0),
                })
                .eq('phone_number', phone_number)
                .execute()
            )

        # Insert or Update Today's Data
        if existing_record:
            # Update the existing record with today's new data
            response = (
                supabase.table('phone_wise_data')
                .update({
                    'total_posts_today': totals['total_posts'],
                    'total_stories_today': totals['total_stories'],
                    'new_followers_today': totals['new_followers'],
                    'avg_eng_rate_today': totals['avg_eng_rate'],
                    'post_views_today': totals['post_views_today'],
                    'total_comments_today': totals['total_comments_today'],
                    'total_likes_today': totals['total_likes_today'],
                    'total_accounts': totals['total_accounts'],
                    'creator_name': creator_name
                })
                .eq('phone_number', phone_number)
                .execute()
            )
        else:
            # Insert a new record with today's data
            response = (
                supabase.table('phone_wise_data')
                .insert({
                    'phone_number': phone_number,
                    'total_posts_today': totals['total_posts'],
                    'total_stories_today': totals['total_stories'],
                    'new_followers_today': totals['new_followers'],
                    'avg_eng_rate_today': totals['avg_eng_rate'],
                    'post_views_today': totals['post_views_today'],
                    'total_comments_today': totals['total_comments_today'],
                    'total_likes_today': totals['total_likes_today'],
                    'total_accounts': totals['total_accounts'],
                    'creator_name': creator_name
                })
                .execute()
            )

except Exception as e:
    logging.error("error while Updateing phone_wise_data table ( 4nd try-except)", e)
    print("error while Updateing phone_wise_data table ( 4nd try-except)", e)

try:           
    #! populating squad_wise_data table with phone_wise_data table data
    # Retrieve phone numbers and creator names from phone_creators
    phone_creators = supabase.table('phones_creators').select('phone_number', 'creator_name').execute().data

    # Initialize a dictionary to store creator names and associated phone numbers
    creator_data = {}

    # Populate the dictionary with creator names as keys and phone numbers as lists
    for entry in phone_creators:
        phone_number = entry['phone_number']
        creator_name = entry['creator_name']
        if creator_name in creator_data:
            creator_data[creator_name].append(phone_number)
        else:
            creator_data[creator_name] = [phone_number]



    # Insert or update squad_wise_data table with creator names and phone numbers
    for creator_name, phones in creator_data.items():
        existing_record = (
            supabase.table('squad_wise_data')
            .select('*')
            .eq('creator_name', creator_name)
            .execute()
            .data
        )
        
        phones_str = '{' + ','.join(phones) + '}'

        if existing_record:
            # Update existing record
            response = (
                supabase.table('squad_wise_data')
                .update({
                    'phones': phones_str
                })
                .eq('creator_name', creator_name)
                .execute()
            )
        else:
            # Insert a new record
            response = (
                supabase.table('squad_wise_data')
                .insert({
                    'creator_name': creator_name,
                    'phones': phones_str
                })
                .execute()
        )
except Exception as e:
    logging.error("error while populating squad_wise_data table with phone_wise_data table data ( 5nd try-except)", e)
    print("error while populating squad_wise_data table with phone_wise_data table data ( 5nd try-except)", e)

try:
    # Now get creator names from squad_wise_data
    squad_creators = supabase.table('squad_wise_data').select('creator_name').execute().data

    for squad_creator in squad_creators:
        creator_name = squad_creator['creator_name']

        # Retrieve rows from phone_wise_data for this creator_name
        phone_wise_rows = (
            supabase.table('phone_wise_data')
            .select('*')
            .eq('creator_name', creator_name)
            .execute()
            .data
        )

        # Aggregate the data for each creator
        totals = {
            'ammount_of_phones': len(phone_wise_rows),
            'total_accounts': 0,
            'total_posts': 0,
            'total_stories_posted': 0,
            'new_followers_today': 0,
            'total_likes_today': 0,
            'total_comments_today': 0,
            'avg_eng_rate_today': 0,
            'total_post_views_today': 0
        }
        profile_count = 0
        engagement_rate_sum = 0

        for row in phone_wise_rows:
            totals['total_accounts'] += row.get('total_accounts', 0) or 0
            totals['total_posts'] += row.get('total_posts_today', 0) or 0
            totals['total_stories_posted'] += row.get('total_stories_today', 0) or 0
            totals['new_followers_today'] += row.get('new_followers_today', 0) or 0
            totals['total_likes_today'] += row.get('total_likes_today', 0) or 0
            totals['total_comments_today'] += row.get('total_comments_today', 0) or 0
            totals['total_post_views_today'] += row.get('post_views_today', 0) or 0

            if row.get('avg_eng_rate_today') is not None:
                engagement_rate_sum += row.get('avg_eng_rate_today', 0)
                profile_count += 1

        if profile_count > 0:
            totals['avg_eng_rate_today'] = round(engagement_rate_sum / profile_count, 2)

        # Shift today's data to yesterday's in squad_wise_data
        existing_squad_record = (
            supabase.table('squad_wise_data')
            .select('*')
            .eq('creator_name', creator_name)
            .execute()
            .data
        )

        if existing_squad_record:
            shift_response = (
                supabase.table('squad_wise_data')
                .update({
                    'new_followers_yesterday': existing_squad_record[0].get('new_followers_today', 0),
                    'total_likes_yesterday': existing_squad_record[0].get('total_likes_today', 0),
                    'total_comments_yesterday': existing_squad_record[0].get('total_comments_today', 0),
                    'avg_eng_rate_yesterday': existing_squad_record[0].get('avg_eng_rate_today', 0),
                    'total_post_views_yesterday': existing_squad_record[0].get('post_views_today', 0),

                })
                .eq('creator_name', creator_name)
                .execute()
            )

            # Update squad_wise_data with new aggregated data
            update_response = (
                supabase.table('squad_wise_data')
                .update(totals)
                .eq('creator_name', creator_name)
                .execute()
            )

except Exception as e:
    logging.error("error while get creator names from squad_wise_dataa ( 6nd try-except )", e)
    print("error while get creator names from squad_wise_dataa ( 6nd try-except )", e)

try:            
    #! preparing slack response
    #! squad performance breakdown

    slack_message = {
        "blocks": []
    }
    ff = datetime.now().strftime("%I:%M %p")
    slack_message["blocks"].append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "Daily Slack Message:",
            "emoji": True
        }
    })
    slack_message["blocks"].append({

        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*Channel: #daily-performance* \n*Posted at: {datetime.now().strftime('%I:%M %p')}*"
        }
    })
    slack_message["blocks"].append(
    {
        "type": "divider"
    })
    slack_message["blocks"].append(
    {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"📊 Daily Squad Performance Report - [{datetime.now().strftime('%B %d, %Y')}]",
            "emoji": True
        }
    })
    slack_message["blocks"].append(
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*Overview:* \nGood morning, team! Here's our comprehensive daily performance update, focusing on post frequency, engagement, and growth metrics across all squads and individual phones."
        }
    })


    phone_data = (
        supabase.table('squad_wise_data')
        .select('*')
        .execute()
    )

except Exception as e:
    logging.error("error while preparing slack response ( 7nd try-except )", e)
    print("error while preparing slack response ( 7nd try-except )", e)

try:
    data = phone_data.data
    for sq_creator_name in data:
        slack_message["blocks"].append(
        {
            "type": "divider"
        }
        )
        slack_message["blocks"].append(
        {
            "type": "divider"
        }
        )
        squad_creator_name = sq_creator_name['creator_name']
        squad_ammount_of_phones = sq_creator_name['ammount_of_phones']
        squad_toal_accounts = sq_creator_name['total_accounts']
        squad_total_posts = sq_creator_name['total_posts']
        squad_total_stories_posted = sq_creator_name['total_stories_posted']

        squad_new_followers_today = sq_creator_name['new_followers_today']
        squad_new_followers_yesterday = sq_creator_name['new_followers_yesterday']
        squad_followers_change = squad_new_followers_today - squad_new_followers_yesterday
        if squad_new_followers_yesterday != 0:
            squad_followers_change_pct = (squad_followers_change / squad_new_followers_yesterday) * 100
        else:
            squad_followers_change_pct = 0

        squad_total_likes_today = sq_creator_name['total_likes_today']
        squad_total_likes_yesterday = sq_creator_name['total_likes_yesterday']
        squad_likes_change = squad_total_likes_today - squad_total_likes_yesterday
        if squad_total_likes_yesterday != 0:
            squad_likes_change_pct = (squad_likes_change / squad_total_likes_yesterday) * 100
        else:
            squad_likes_change_pct = 0

        squad_total_comments_today = sq_creator_name['total_comments_today']
        squad_total_comments_yesterday = sq_creator_name['total_comments_yesterday']
        squad_comments_change = squad_total_comments_today - squad_total_comments_yesterday
        if squad_total_comments_yesterday != 0:
            squad_comments_change_pct = (squad_comments_change / squad_total_comments_yesterday) * 100
        else:
            squad_comments_change_pct = 0

        squad_avg_eng_rate_today = sq_creator_name['avg_eng_rate_today']
        squad_avg_eng_rate_yesterday = sq_creator_name['avg_eng_rate_yesterday']
        squad_eng_rate_change = squad_avg_eng_rate_today - squad_avg_eng_rate_yesterday

        squad_total_post_views_today = sq_creator_name['total_post_views_today']
        squad_total_post_views_yesterday = sq_creator_name['total_post_views_yesterday']
        squad_views_change = squad_total_post_views_today - squad_total_post_views_yesterday
        if squad_total_post_views_yesterday != 0:
            squad_views_change_pct = (squad_views_change / squad_total_post_views_yesterday) * 100
        else:
            squad_views_change_pct = 0

        squad_phones = sq_creator_name['phones']

        trgt_response = (
            supabase.table('targets_ig_daily')
            .select('*')
            .ilike('creator_name', creator_name)
            .execute()
        ) 
        target = None
        if trgt_response.data:
            trgt_data = trgt_response.data
            squad_target_reels_per_day = trgt_data[0].get('target_reels_per_day', None)
            squad_target_stories_per_day = trgt_data[0].get('target_stories_per_day', None) 

        slack_message["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*🔶 Creator Squad - {squad_creator_name}*\n"
                    f"• *Amount of Phones:* {squad_ammount_of_phones}\n"
                    f"• *Total Accounts:* {squad_toal_accounts}\n"
                    f"• *Total Posts:* {squad_total_posts}\n"
                    f"• *Target Posts*: {squad_target_reels_per_day} - "
                    f"{'🔻' if squad_total_posts < squad_target_reels_per_day else '🟢'}"
                    f"{abs(squad_total_posts - squad_target_reels_per_day)} posts, "
                    f"{(squad_total_posts / squad_target_reels_per_day) * 100:.0f}% of target hit - "
                    f"{'🔴FAILED🔴' if squad_total_posts < squad_target_reels_per_day else '🟢SUCCESS🟢'})\n"
                    f"• *Total Stories Posted:* {squad_total_stories_posted}\n"
                    f"• *Target Stories*: {squad_target_stories_per_day} - "
                    f"{'🔻' if squad_total_stories_posted < squad_target_stories_per_day else '🟢'}"
                    f"{abs(squad_total_stories_posted - squad_target_stories_per_day)} stories, "
                    f"{(squad_total_stories_posted / squad_target_stories_per_day) * 100:.0f}% of target hit - "
                    f"{'🔴FAILED🔴' if squad_total_stories_posted < squad_target_stories_per_day else '🟢SUCCESS🟢'})\n"
                    f"• *New Followers:* {squad_new_followers_today} "
                    f"({'🟢' if squad_followers_change > 0 else '🔻'}{abs(squad_followers_change_pct):.1f}% from previous day)\n"
                    f"• *Total Likes:* {squad_total_likes_today} "
                    f"({'🟢' if squad_likes_change > 0 else '🔻'}{abs(squad_likes_change_pct):.1f}% from previous day)\n"
                    f"• *Total Comments:* {squad_total_comments_today} "
                    f"({'🟢' if squad_comments_change > 0 else '🔻'}{abs(squad_comments_change_pct):.1f}% from previous day)\n"
                    f"• *Average Engagement Rate:* {squad_avg_eng_rate_today:.1f}% "
                    f"({'🟢' if squad_eng_rate_change > 0 else '🔻'}{abs(squad_eng_rate_change):.1f}% from previous day)\n"
                    f"• *Post Views Change:* {squad_total_post_views_today} "
                    f"({'🟢' if squad_views_change > 0 else '🔻'}{abs(squad_views_change_pct):.1f}% from previous day)"
                )
            }
        })

        slack_message["blocks"].append({"type": "divider"})
        slack_message["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Phone Performance Breakdown:*"
            }
        })
        for p_phone in squad_phones:
            phone_response = (
                supabase.table('phone_wise_data')
                .select('*')
                .eq('phone_number', p_phone)
                .execute()
            )

            phone_data = phone_response.data
            for phone in phone_data:
                phone_number = phone['phone_number']
                phone_total_posts_today = phone['total_posts_today']
                phone_total_posts_yesterday = phone['total_posts_yesterday']
                phone_posts_change = phone_total_posts_today - phone_total_posts_yesterday 
                if phone_total_posts_yesterday != 0:
                    phone_posts_change_pct = (phone_posts_change / phone_total_posts_yesterday) * 100
                else:
                    phone_posts_change_pct = 0

                phone_total_stories_today = phone['total_stories_today']
                phone_total_stories_yesterday = phone['total_stories_yesterday']
                phone_stories_change = phone_total_stories_today - phone_total_stories_yesterday
                if phone_total_stories_yesterday != 0:
                    phone_stories_change_pct = (phone_stories_change / phone_total_stories_yesterday) * 100
                else:
                    phone_stories_change_pct = 0

                phone_new_followers_today = phone['new_followers_today']
                phone_new_followers_yesterday = phone['new_followers_yesterday']
                phone_followers_change = phone_new_followers_today - phone_new_followers_yesterday
                if phone_new_followers_yesterday != 0:
                    phone_followers_change_pct = (phone_followers_change / phone_new_followers_yesterday) * 100
                else:
                    phone_followers_change_pct = 0

                phone_avg_eng_rate_today = phone['avg_eng_rate_today']
                phone_avg_eng_rate_yesterday = phone['avg_eng_rate_yesterday']
                phone_eng_rate_change = phone_avg_eng_rate_today - phone_avg_eng_rate_yesterday

                phone_post_views_today = phone['post_views_today']
                phone_post_views_yesterday = phone['post_views_yesterday']
                phone_views_change = phone_post_views_today - phone_post_views_yesterday
                if phone_post_views_yesterday != 0:
                    phone_views_change_pct = (phone_views_change / phone_post_views_yesterday) * 100
                else:
                    phone_views_change_pct = 0

                phone_total_comments_today = phone['total_comments_today']
                phone_total_comments_yesterday = phone['total_comments_yesterday']
                phone_comments_change = phone_total_comments_today - phone_total_comments_yesterday
                if phone_total_comments_yesterday != 0:
                    phone_comments_change_pct = (phone_comments_change / phone_total_comments_yesterday) * 100
                else:
                    phone_comments_change_pct = 0

                phone_total_likes_today = phone['total_likes_today']
                phone_total_likes_yesterday = phone['total_likes_yesterday']
                phone_likes_change = phone_total_likes_today - phone_total_likes_yesterday
                if phone_total_likes_yesterday != 0:
                    phone_likes_change_pct = (phone_likes_change / phone_total_likes_yesterday) * 100
                else:
                    phone_likes_change_pct = 0

                slack_message["blocks"].append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*📱 Phone {phone_number}:*\n"
                            f"• *Posts:* {phone_total_posts_today} "
                            f"({'🔻' if phone_posts_change < 0 else '🟢'}{abs(phone_posts_change_pct):.1f}% from previous day)\n"
                            f"• *Stories:* {phone_total_stories_today} "
                            f"({'🔻' if phone_stories_change < 0 else '🟢'}{abs(phone_stories_change_pct):.1f}% from previous day)\n"
                            f"• *New Followers:* {phone_new_followers_today} "
                            f"({'🟢' if phone_followers_change > 0 else '🔻'}{abs(phone_followers_change_pct):.1f}% from previous day)\n"
                            f"• *Average Engagement Rate:* {phone_avg_eng_rate_today:.1f}% "
                            f"({'🟢' if phone_eng_rate_change > 0 else '🔻'}{abs(phone_eng_rate_change):.1f}% from previous day)\n"
                            f"• *Post Views Change:* {phone_post_views_today} "
                            f"({'🔻' if phone_views_change < 0 else '🟢'}{abs(phone_views_change_pct):.1f}% from previous day)\n"
                            f"• *Total Comments:* {phone_total_comments_today} "
                            f"({'🟢' if phone_comments_change > 0 else '🔻'}{abs(phone_comments_change_pct):.1f}% from previous day)\n"
                            f"• *Total Likes:* {phone_total_likes_today} "
                            f"({'🔻' if phone_likes_change < 0 else '🟢'}{abs(phone_likes_change_pct):.1f}% from previous day)"
                        )
                    }
                })
                        
    slack_message_json = json.dumps(slack_message, indent=4)
    with open ('slack_massage.json', 'w') as f:
        f.write(slack_message_json)

    from slack_sdk.webhook import WebhookClient
    url = "https://hooks.slack.com/services/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"
    webhook = WebhookClient(url)
    response = webhook.send(
        text="test",
        blocks=slack_message["blocks"]
    )

except Exception as e:
    logging.error(f"error in asssembling and sending slack massage (8th & last try-excpet)", e)
    print(f"error in asssembling and sending slack massage (8th & last try-excpet)", e)   


#! writing error summary to file from log file
try:
    log_file = 'app.log'
    output_file = 'error_summary.txt'

    # Regular expression patterns
    error_pattern = re.compile(r'- ERROR - (\S+) : (.+)')
    username_error_map = {}

    # Read the log file and extract errors and usernames
    with open(log_file, 'r') as file:
        for line in file:
            match = error_pattern.search(line)
            if match:
                username = match.group(1)
                error_message = match.group(2)
                if error_message not in username_error_map:
                    username_error_map[error_message] = []
                username_error_map[error_message].append(username)

    # Write the extracted information to the output file
    with open(output_file, 'w') as file:
        for error, usernames in username_error_map.items():
            file.write(f"Error: {error}\n")
            for username in usernames:
                file.write(f"- {username}\n")
            file.write("\n")

    print(f"Error summary has been written to {output_file}")

except Exception as e:
    logging.error(f"error in writing error summary to file ", e)
    print(f"error in writing error summary to file ", e)


#! sending email with error summary file
import email, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import datetime

try:
    # Email details
    subject = "Error Summary"
    sender_email = "hassanbinaman2@gmail.com"
    receiver_email = "hassanbinaman2@gmail.com"
    password = "xqtn kwsc wpjc wthx"

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message["Bcc"] = receiver_email  # Recommended for mass emails

    # Get the current time and date
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Read the contents of the error_summary.txt file
    with open("error_summary.txt", "r") as file:
        file_content = file.read()

    # Create the email body with the time, date, and file content
    body = f"This is an error summary sent from Python.\n\nTime: {current_time}\n\n{file_content}"
    message.attach(MIMEText(body, "plain"))

    # Convert message to string
    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)

except Exception as e:
    logging.error("Error in sending email with attachment: %s", e)
    print(f"Error in sending email: {e}")


#! Clear the contents of the log file
try:
    log_file = 'app.log'

    # Open the file in write mode ('w') to clear its contents
    with open(log_file, 'w') as file:
        # Simply pass to truncate the file
        pass

    print(f"The contents of {log_file} have been cleared.")

except Exception as e:
    print(f"error in clearing log file", e)


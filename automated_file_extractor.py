# ***********************************************************************
#                            FILE OVERVIEW
# ***********************************************************************
# This file implements a framework for automating file extractions and
# processing from various sources (SFTP, Outlook).
# Key Features:
# 1. Region-based scheduling for processing files.
# 2. Email alert mechanisms if files are not received on time.
# 3. Audit logging to track processing activities.
# 4. File extraction logic involving renaming, validation, and conversions.
# 5. Integration with SFTP and Microsoft Outlook for file retrieval.

import yaml
import os
import shutil
import re
import logging
import pyodbc
import json
import logging
import sys
import hashlib
from datetime import datetime,timedelta
from datetime import time
import win32com.client ##outlook read
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib
import os 
import pandas as pd
from dateutil import parser
from glob import glob
from win32com.client import Dispatch
from zoneinfo import ZoneInfo
import hashlib
import psutil
from sftp_download import sftp_download_by_key, load_config

# ***********************************************************************
#                            CLASS DEFINITION
# ***********************************************************************

class OutlookAttachmentDownloader:
    """
    Handles file extraction and processing workflows for different regions.

    Attributes:
        config_path (str): Path to the YAML configuration file.
        time_config (dict): Configuration loaded from the YAML file.
        region (str): Current region (APJ, AMS, EMEA) based on time.
    """
    def __init__(self, config_path):
        self.config_path = config_path
        self.time_config = self.read_SLA_time_yaml_file()

        if not self.time_config:
            raise ValueError("No valid timing configuration found for region")
            
        self.APJ_Start_time = self.time_config.get("APJ_Start_time", 0)
        self.APJ_End_time = self.time_config.get("APJ_End_time", 0)
        self.AMS_Start_time = self.time_config.get("AMS_Start_time", 0)
        self.AMS_End_time = self.time_config.get("AMS_End_time", 0)
        self.EMEA_Start_time = self.time_config.get("EMEA_Start_time", 0)
        self.EMEA_End_time = self.time_config.get("EMEA_End_time", 0)
        self.smtp_server = self.time_config.get("smtp_server", "")
        self.port = self.time_config.get("port", 0)
        self.sender_email = self.time_config.get("sender_email", "")
        self.base_directory = self.time_config.get("base_directory", "")
        self.receiver_email = self.time_config.get("receiver_email", "")
        self.cc_email = self.time_config.get("cc_email", "")
        self.subject = self.time_config.get("subject", "")
        self.success = self.time_config.get("success", "")
        self.failure = self.time_config.get("failure", "")
        self.Env = self.time_config.get("Env", "")
        self.manual_folder_path =  self.time_config.get("manual_folder_path", "")
        self.manual_folder_temppath =  self.time_config.get("manual_folder_temppath", "")        
    # ***********************************************************************
    #                            REGION-BASED LOGIC
    # ***********************************************************************

    def get_region_by_time(self, APJ_Start_time, APJ_End_time, AMS_Start_time, AMS_End_time, EMEA_Start_time, EMEA_End_time):
        """
        Determines the current region based on the current time.
        Supports region time windows that span past midnight (e.g., 19 to 1).
        """
        try:
            now = datetime.now().time()
            current_hour = now.hour
    
            def is_within_range(start, end):
                return start <= current_hour < end if start < end else current_hour >= start or current_hour < end
    
            if is_within_range(EMEA_Start_time, EMEA_End_time):
                return "EMEA"
            elif is_within_range(APJ_Start_time, APJ_End_time):
                return "APJ"
            elif is_within_range(AMS_Start_time, AMS_End_time):
                return "AMS"
            else:
                return False
        except Exception as e:
            logging.error(f"Error determining region: {e}")
            return False
   
    
    # ***********************************************************************
    #                        CONFIGURATION HANDLING
    # ***********************************************************************
    
    def read_SLA_time_yaml_file(self):
        """
        Reads the SLA timing configuration from a YAML file.

        Returns:
            dict: YAML configuration data.
        """
        try:
            with open(self.config_path, 'r') as file:
                data = yaml.safe_load(file)
                return data
        except Exception as e:
            print(f"Error reading YAML file: {e}")
            return None

    def load_config(self, region):
        """
        Loads configuration for a specific region from the database.

        Args:
            region (str): The region name (e.g., APJ, AMS, EMEA).

        Returns:
            list: List of configuration entries for the region.
        """
        try:
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(f"Configuration file '{self.config_path}' not found.")

            with open(self.config_path, "r") as file:
                config = yaml.safe_load(file)

            return config["regions"].get(region, [])
        except Exception as e:
            logging.error(f"Error loading configuration: {e}")
            return []
            
    # ***********************************************************************
    #                      CONFIGURATION VALIDATION
    # ***********************************************************************
    
    def load_config_from_db(self,region):
        """
        Loads the configuration for a specific region from the database.

        Args:
            region (str): The region name.

        Returns:
            list: Configuration entries for the region.
        """
        try:
            # Step 1: Connect to your SQL Server database
            conn = pyodbc.connect(f"DRIVER={'ODBC Driver 17 for SQL Server'};SERVER={'10.82.60.148'};UID={'dev_audit_usr'};PWD={'1J45Aw<brVqY_*}w'}")
            cursor = conn.cursor()
    
            # Step 2: Run the query to fetch config rows for the region
            query = """
                SELECT * FROM dbo.sc360_extraction_config WHERE is_active_flag = 'Y' and region = ? 
                AND (
                (CAST(min_filter_hour AS TIME) <= DATEADD(HOUR, 1, CAST(max_filter_hour AS TIME)) AND
                CAST(GETDATE() AS TIME) BETWEEN CAST(min_filter_hour AS TIME)
                AND DATEADD(HOUR, 1, CAST(max_filter_hour AS TIME)))
                OR
                (CAST(min_filter_hour AS TIME) > DATEADD(HOUR, 1, CAST(max_filter_hour AS TIME)) AND
                (CAST(GETDATE() AS TIME) >= CAST(min_filter_hour AS TIME) 
                OR CAST(GETDATE() AS TIME) <= DATEADD(HOUR, 1, CAST(max_filter_hour AS TIME))))
                )
                order by file_id asc ,time_window_label asc  ;
            """
                
            cursor.execute(query, (region,))
    
            # Step 3: Map rows to dicts
            columns = [column[0] for column in cursor.description]
            file_entries = []
    
            for row in cursor.fetchall():
                entry = dict(zip(columns, row))    
                file_entries.append(entry)
    
            cursor.close()
            conn.close()
    
            return file_entries

        except Exception as e:
            logging.error(f"Error loading config from DB: {e}")
            return []

    # ***********************************************************************
    #                      CONFIGURATION VALIDATION
    # ***********************************************************************
    
    def load_config_from_db_using_src_file(self, region, filename):
        """
        Loads the configuration for a specific region from the database.
    
        Args:
            region (str): The region name.
            filename (str): The filename passed as input.
    
        Returns:
            list: Configuration entries for the region.
        """
        try:
            import pyodbc
            import logging

            file_row_id = self.extract_number(filename)
            logging.info(f"file_row_id: {file_row_id}")
            
    
            # Step 1: Connect to your SQL Server database
            conn = pyodbc.connect(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=10.82.60.148;"
                "UID=dev_audit_usr;"
                "PWD=1J45Aw<brVqY_*}w"
            )
            cursor = conn.cursor()
    
            # Step 2: Run the query to fetch config rows for the region
            query = """
                SELECT * 
                FROM dbo.sc360_extraction_config
                WHERE is_active_flag = 'Y' and file_row_id = ? and region = ?
            """
            cursor.execute(query, (file_row_id,region,))
    
            # Step 3: Map rows to dicts
            columns = [column[0] for column in cursor.description]
            file_entries = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
            cursor.close()
            conn.close()
    
            return file_entries
    
        except Exception as e:
            logging.error(f"Error loading config from DB: {e}")
            return []


            
    
    # ***********************************************************************
    #                            AUDIT LOGGING
    # ***********************************************************************
    
    def setup_logging(self):
        """
        Sets up logging for the current run, creating a log file.
        """
        try:
            log_dir = os.path.join(self.base_directory, "logs")
            os.makedirs(log_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = os.path.join(log_dir, f"{self.region}_log_{timestamp}.log")

            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(levelname)s - %(message)s",
                handlers=[
                    logging.FileHandler(log_filename, encoding="utf-8"),
                    logging.StreamHandler()
                ]
            )
            logging.info(f"Log file created: {log_filename}")
        except Exception as e:
            print(f"Error setting up logging: {e}")
            
                        
            
    def setup_logging_for_manual(self,region):
        """
        Sets up logging for the current run, creating a log file.
        """
        try:
            log_dir = os.path.join(self.base_directory, "logs")
            os.makedirs(log_dir, exist_ok=True)
    
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = os.path.join(log_dir, f"{region}_manual_log_{timestamp}.log")
    
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(levelname)s - %(message)s",
                handlers=[
                    logging.FileHandler(log_filename, encoding="utf-8"),
                    logging.StreamHandler()
                ]
            )
            logging.info(f"Log file created: {log_filename}")
        except Exception as e:
            print(f"Error setting up logging: {e}")  

  
                    

    def is_allowed_today(self,exclude_days):
        """
        Determines if today is allowed for processing based on excluded days.

        Args:
            exclude_days (list): List of days to exclude.

        Returns:
            bool: True if today is allowed, False otherwise.
        """
        try:
            today = datetime.today().strftime("%A")
            return today not in exclude_days   
        except Exception as e:
            print(f"Error in function is_allowed_today: {e}")
            

    def get_extraction_config(self, file_config):
        """
        Extracts configuration details for file processing.

        Args:
            file_config (dict): Configuration for the file.
        """
        
        try:           
            self.file_id = file_config.get("file_id", "NA")
            self.file_row_id = file_config.get("file_row_id", "NA")
            self.intermediate_folder = file_config.get("download_path", "NA")
            self.folder_name = file_config.get("folder_name", "")
            self.subfolder_name = file_config.get("subfolder_name", "NA")
            self.subject_filter = file_config.get("subject_filter", "NA")
            self.sheet_name = file_config.get("sheet_name", "NA")
            self.final_dest_path = file_config.get("final_dest_path", "NA")
            self.extractor_type = file_config.get("extractor_type", "NA")
            self.frequency = file_config.get("frequency_type", "NA")
            self.exclude_days = file_config.get("exclude_days","NA")
            self.rename_required = file_config.get("rename_required", "false").lower() == "true"
            self.File_conversion = file_config.get("file_extension_conversion", "false").lower() == "true"
            self.source_ext = file_config.get("source_ext", "NA")
            self.target_ext = file_config.get("target_ext", "NA")
            self.raw_filename = file_config.get("raw_filename", "NA")
            self.Destn_file_name = file_config.get("destn_file_name", "NA")
            self.Data_validation = file_config.get("data_validation", "false").lower() == "true"
            self.field_name = file_config.get("field_name", "NA")
            self.field_value = file_config.get("field_value", "NA")
            self.archive_path = file_config.get("archive_path", "NA")
            self.date_to_be_checked = file_config.get("date_to_be_checked", "NA")
            self.time_window_label = file_config.get("time_window_label", "")
            self.schedule_dependency_enabled = file_config.get("schedule_dependency_enabled", "false").lower() == "true"
            self.unmerge_flag = file_config.get("unmerge_flag", "false").lower() == "true"
            self.is_multicopy =  file_config.get("is_multicopy", "NA")
            self.secondary_final_dest_path =  file_config.get("secondary_final_dest_path", "NA")

            
            self.min_hour = int(file_config.get("min_hour", "00:00").split(":")[0])
            self.min_min = int(file_config.get("min_hour", "00:00").split(":")[1])
            self.max_hour = int(file_config.get("max_hour", "23:59").split(":")[0])
            self.max_min = int(file_config.get("max_hour", "23:59").split(":")[1])
            self.min_hour_time = time(self.min_hour, self.min_min)  # e.g., 9:00
            self.max_hour_time = time(self.max_hour, self.max_min)    # e.g., 14:30
            
        except Exception as e:
            logging.warning(f"Error in get_extraction_config: {e}")
            return True
    
    def copy_latest_to_target(self, intermediate_path, target_folder,archive_path):
        """
        Copies the latest file to the target and archive folders.

        Args:
            intermediate_path (str): Path to the file.
            target_folder (str): Target folder path.
            archive_path (str): Archive folder path.
        """
        try:
            shutil.copy2(intermediate_path, target_folder)
            logging.info(f"Copied to target folder: {target_folder}")
            shutil.copy2(target_folder, archive_path)
            logging.info(f"Copied to Archive folder: {archive_path}")

        except Exception as e:
            logging.error(f"Error in copy_latest_to_target: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date)
            
            
            
    def copy_latest_to_secondary_target(self, intermediate_path, secondary_target_folder):
        """
        Copies the latest file to the target and archive folders.

        Args:
            intermediate_path (str): Path to the file.
            secondary_target_folder (str): Target folder path.
        """
        try:
            shutil.copy2(intermediate_path, secondary_target_folder)
            logging.info(f"Copied to secondary target folder: {secondary_target_folder}")

        except Exception as e:
            logging.error(f"Error in copy_latest_to_target: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date)

        
    
    def insert_audit_log(self, execution_status, error_msg="", run_date=None, failure_type=None):
        """
        Inserts an entry into the audit log database.

        Args:
            execution_status (str): Status of execution (Success/Failure).
            error_msg (str): Error message, if any.
            run_date (datetime): Date and time of execution.
        """
        try:
            run_date = run_date or datetime.now()
    
            # DB connection - adjust as needed
            conn = pyodbc.connect(f"DRIVER={'ODBC Driver 17 for SQL Server'};SERVER={'10.82.60.148'};UID={'dev_audit_usr'};PWD={'1J45Aw<brVqY_*}w'}")
            cursor = conn.cursor()
    
            # SQL insert query
            insert_query = """
                INSERT INTO dbo.sc360_extraction_process_log
                (file_id, region, src_file_name, extractor_type, archive_path, execution_status, error_msg, run_date, failure_type,time_window_label)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?)
            """
    
            cursor.execute(insert_query, (
                self.file_id,
                self.region,
                self.Destn_file_name,
                self.extractor_type,
                self.archive_path,
                execution_status,
                error_msg,
                run_date,
                failure_type,
                self.time_window_label
            ))
            conn.commit()
    
            logging.info("Audit log entry inserted successfully.")
    
            cursor.close()
            conn.close()
    
        except Exception as e:
            logging.error(f"Failed to insert audit log: {e}")


    def delete_audit_log(self):
        """
        Deletes failed audit log entries for the current file and region.
        """
        try:
    
            # DB connection - adjust as needed
            conn = pyodbc.connect(f"DRIVER={'ODBC Driver 17 for SQL Server'};SERVER={'10.82.60.148'};UID={'dev_audit_usr'};PWD={'1J45Aw<brVqY_*}w'}")
            cursor = conn.cursor()
    
            # SQL insert query
            delete_query = f"""
                delete from  dbo.sc360_extraction_process_log where file_id = ? and region = ? and CAST(run_date as Date) = CAST(? As Date) and time_window_label= ? and execution_status = 'Failed' 
            """
    
            cursor.execute(delete_query, (
                self.file_id,
                self.region,
                self.processing_date,
                self.time_window_label
            ))
            conn.commit()
    
            logging.info("Audit log entry deleted successfully.")
    
            cursor.close()
            conn.close()
    
        except Exception as e:
            logging.error(f"Failed to delete audit log: {e}")
    
    def was_sla_missed_email_sent(self):
        """Check if SLA missed email was already sent today."""
        try:
            check_date = datetime.today().date()
            conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.82.60.148;UID=dev_audit_usr;PWD=1J45Aw<brVqY_*}w")
            cursor = conn.cursor()
            query = """
                SELECT COUNT(*) FROM dbo.sc360_extraction_process_log
                WHERE file_id = ? AND region = ? AND CAST(run_date AS DATE) = cast(?  as DATE ) AND  time_window_label = ?
                and failure_type IN  ('SLA_MISSED_EMAIL_SENT','FOLDER_NOT_ACCESSIBLE' ,'PROCESSING_ERROR','EMPTY_FILE')
            """
            cursor.execute(query, (self.file_id, self.region, self.processing_date,self.time_window_label))
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count > 0
        except Exception as e:
            logging.warning(f"Check for SLA missed email failed: {e}")
            return False
    
    
    def get_processing_and_file_dates(self):
        """
        Determines the processing date and file received date based on region and date variable.
        For APJ region with date_variable "d-1", handles the special case for late-night processing.
        """
        try:
            
            utc_now = datetime.utcnow()
            #file_received_date = utc_now.date()
            file_received_date = datetime.combine(utc_now.date(), datetime.min.time())
            processing_date = datetime.combine(utc_now.date(), utc_now.time())
            # Default values
            # Special condition for with "d-1" during late-night window
            if self.date_variable == "d-1" and self.region == "APJ" :
                # Check if current time is between 10:00 PM
                if self.schedule_dependency_flag == "false" and utc_now.time() >= time(19, 0) and utc_now.time() <= time(23, 59):
                    file_received_date = datetime.combine(utc_now.date(), datetime.min.time())
                    processing_date = datetime.combine(utc_now.date() + timedelta(days=1), utc_now.time())
                elif self.schedule_dependency_flag == "true":
                    logging.info("step2")
                    file_received_date = datetime.combine(utc_now.date() - timedelta(days=1), datetime.min.time())
                    processing_date = datetime.combine(utc_now.date(), utc_now.time())
            if self.date_variable == "d-1" and self.region == "WW" :
                file_received_date = datetime.combine(utc_now.date() - timedelta(days=1), datetime.min.time())
                processing_date = datetime.combine(utc_now.date(), utc_now.time())
            return file_received_date, processing_date
        except Exception as e:
            logging.error(f"Failed in get_processing_and_file_dates: {e}")
    
    # ***********************************************************************
    #                        FILE EXTRACTION LOGIC
    # ***********************************************************************
    
    def fetch_latest_sftp_file(self, file_config):
        """
        Retrieves the latest file from the SFTP server based on the file configuration.

        Args:
            file_config (dict): Configuration for the SFTP file.

        Returns:
            list: List of file paths retrieved from the SFTP server.
        """
        try:        
            logging.info(f"subfolder_name: {self.subfolder_name}")
            
            # Get the list of files in the source folder
            files = [
                os.path.join(self.subfolder_name, f)
                for f in os.listdir(self.subfolder_name)
                if os.path.isfile(os.path.join(self.subfolder_name, f)) and f in [file_config] and os.path.getsize(os.path.join(self.subfolder_name, f)) > 0
            ]

            if not files:
                logging.info("No files found in the source folder.")
                return []

            logging.info(f"Fetched {len(files)} files from source folder: {self.subfolder_name}")
            return files

        except Exception as e:
            logging.error(f"Error fetching files from source folder: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="NO_FILE_FOUND")
            self.send_email(self.sender_email, self.receiver_email, self.cc_email, self.smtp_server, self.port, f"Source folder '{self.subfolder_name}' does not exist or is not accessible.",self.Destn_file_name, status="folder_not_accessible")
            self.file_fetch_failed = True
            return []


    # ***********************************************************************
    #                        FILE EXTRACTION LOGIC
    # ***********************************************************************
    
    def fetch_sla_missed_files_from_manual_folder(self, manual_folder_path):
        """
        Retrieves the latest file from the SFTP server based on the file configuration.

        Args:
            file_config (dict): Configuration for the SFTP file.

        Returns:
            list: List of file paths retrieved from the SFTP server.
        """
        try:
            if not manual_folder_path or not os.path.isdir(manual_folder_path):
                logging.error(f"Source folder '{manual_folder_path}' does not exist or is not accessible.")
                return []
            
            logging.info(f"manual_folder_path: {manual_folder_path}")
            
            # Get the list of files in the source folder
            files = [
                os.path.join(manual_folder_path, f)
                for f in os.listdir(manual_folder_path)
                if os.path.isfile(os.path.join(manual_folder_path, f)) and os.path.getsize(os.path.join(manual_folder_path, f)) > 0
            ]

            if not files:
                logging.info("No files found in the manual_folder_path.")
                return []

            logging.info(f"Fetched {len(files)} files from source folder: {manual_folder_path}")
            return files

        except Exception as e:
            logging.error(f"Error fetching files from source folder: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="NO_FILE_FOUND")
            self.send_email(self.sender_email, self.receiver_email, self.cc_email, self.smtp_server, self.port, f"Source folder '{self.subfolder_name}' does not exist or is not accessible.",self.Destn_file_name, status="folder_not_accessible")
            self.file_fetch_failed = True
            return []



    def process_sftp_file(self, file_config):
        """
        Processes a file from an SFTP server based on the file configuration.

        Args:
            file_config (dict): Configuration for the file to process.
        """
        try:
            
            logging.info(f"**************************Process started for: {file_config['src_file_name']}***********************************************")
            logging.info(f"[SFTP] Processing file: {file_config['src_file_name']}")
            
            # load once at start of run
            dropbox_config = load_config("D:\\Technical\\FileExtraction\\config\\sftp_dropbox_config.json")
            
            if file_config["src_file_name"] in dropbox_config:
                logging.info(f"{file_config['src_file_name']} found in dropbox config – fetching from Dropbox SFTP")
                try:
                    sftp_download_by_key(file_config["src_file_name"], "D:\\Technical\\FileExtraction\\config\\sftp_dropbox_config.json")
                    logging.info("Dropbox SFTP download completed successfully.")
                except Exception as e:
                    logging.error(f"Dropbox SFTP download failed: {e}")
                    return
            
            # Continue with normal SFTP logic
            self.file_fetch_failed = False
            
            current_time = datetime.now()
            today = datetime.today().date()
            
            # Extract configuration
            self.get_extraction_config(file_config)
            
            end = datetime.combine(self.file_received_date.date(), datetime.strptime(f"{self.max_hour}:{self.max_min}", "%H:%M").time()).strftime("%m/%d/%Y %I:%M %p")
            threshold_sla_end = datetime.strptime(end, "%m/%d/%Y %I:%M %p")
            
            # Prepare intermediate folder for today's files
            intermediate_folder = os.path.join(self.intermediate_folder)
            os.makedirs(intermediate_folder, exist_ok=True)

            # Fetch the list of files from the source SFTP folder
            files = self.fetch_latest_sftp_file(self.raw_filename)
            
            # If no files are found, exit
            if not files and current_time < threshold_sla_end: 
                logging.info("No files to process.")
                logging.info(f"Current time {current_time} is before SLA {threshold_sla_end}.. Skipping processing.")
                return
            if not files and current_time >=threshold_sla_end and not self.file_fetch_failed: 
                logging.info(f"Current time {current_time} is after SLA {threshold_sla_end}.. Time window is exhausted for the day.")
                logging.info("File didn't received in Source folder itself. Sending warning mail.")
                if not self.was_sla_missed_email_sent():
                    self.send_email(self.sender_email, self.receiver_email, self.cc_email, self.smtp_server, self.port, self.subject,self.Destn_file_name)
                e="File didn't received for today. Sending warning mail."
                self.delete_audit_log()
                self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="SLA_MISSED_EMAIL_SENT")
            
            # Assuming `files` contains full paths of matching files
            sorted_files = sorted(files, key=os.path.getmtime, reverse=False)
            # Use the shared `process_attachments` function to process the files
            for file in sorted_files:
                received_time = datetime.fromtimestamp(os.path.getmtime(file))
                # Separate date and time
                received_date = received_time.date()
                # Combine and zero out seconds/microseconds
                received_datetime = datetime.combine(received_date, received_time.time()).replace(second=0, microsecond=0)
                
                logging.info(f"File received datetime (UTC): {received_time}")
                if received_date == self.file_received_date.date() and self.min_hour_time <= received_time.time() < self.max_hour_time:
                    self.process_attachments(
                        file_paths=files,
                        received_datetime=received_datetime,
                        intermediate_folder=intermediate_folder,
                        is_local_file=True
                    )
                elif received_date == self.file_received_date.date() and current_time >=threshold_sla_end:
                    logging.info(f"File '{file}' received at {received_time}, File '{file}' was received after the defined SLA cutoff time.")
                    logging.info("File came out of SLA.Sending warning mail.")
                    if not self.was_sla_missed_email_sent():
                        self.send_email(self.sender_email, self.receiver_email, self.cc_email, self.smtp_server, self.port,"File was received out of the defined SLA cutoff time.",self.Destn_file_name,status="received_late")
                        e="File came after SLA"
                        self.delete_audit_log()
                        self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="SLA_MISSED_EMAIL_SENT")
                elif current_time < threshold_sla_end :
                    logging.info(f"Current time {current_time} is before SLA {threshold_sla_end}.. Skipping processing.")
                elif current_time >=threshold_sla_end: 
                    logging.info(f"Current time {current_time} is after SLA {threshold_sla_end}.. Time window is exhausted for the day.")
                    logging.info("File didn't received for current day. Source folder has Yesterday file.Sending warning mail.")
                    if not self.was_sla_missed_email_sent():
                        self.send_email(self.sender_email, self.receiver_email, self.cc_email, self.smtp_server, self.port, self.subject,self.Destn_file_name)
                    e="File didn't received for today. Sending warning mail."
                    self.delete_audit_log()
                    self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="SLA_MISSED_EMAIL_SENT")
            logging.info(f"**************************Process Completed for: {file_config['src_file_name']}***********************************************")

        except Exception as e:
            logging.error(f"Error in process_sftp_file: {e}")
    
    # ***********************************************************************
    #                        OUTLOOK HANDLING
    # ***********************************************************************
    
    #def connect_to_outlook_folder(self):
    #    """
    #    Connects to the specified Outlook folder.
    #
    #    Returns:
    #        object: Outlook folder object if successful, None otherwise.
    #    """
    #    try:
    #        #outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    #        import win32com.client
    #        outlook = win32com.client.gencache.EnsureDispatch("Outlook.Application").GetNamespace("MAPI")
    #        inbox = outlook.Folders.Item("aruba.globalscanalytics@hpe.com").Folders.Item(self.folder_name).Folders.Item(self.subfolder_name)
    #        return inbox
    #    except Exception as e:
    #        logging.error(f"Error accessing Outlook folder: {e}")
    #        self.delete_audit_log()
    #        self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")
    #        return None
            
    def is_outlook_running(self):
        return any("OUTLOOK.EXE" in p.name() for p in psutil.process_iter())

    def connect_to_outlook_folder(self):
        """
        Connects to the specified Outlook folder.
    
        Returns:
            object: Outlook folder object if successful, None otherwise.
        """
        if not self.is_outlook_running():
            logging.error("Outlook is not running. Exiting.")
            return None
    
        try:
            outlook = win32com.client.gencache.EnsureDispatch("Outlook.Application").GetNamespace("MAPI")
            inbox = outlook.Folders.Item("aruba.globalscanalytics@hpe.com").Folders.Item(self.folder_name).Folders.Item(self.subfolder_name)
            return inbox
        except Exception as e:
            logging.error(f"Error accessing Outlook folder: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure, f"{e}", self.processing_date, failure_type="PROCESSING_ERROR")
            return None        
            
            
    # ***********************************************************************
    #                         MESSAGE PROCESSING
    # ***********************************************************************
    def process_messages(self, messages):
        """
        Processes email messages to extract and process attachments.

        Args:
            messages (object): Outlook messages object containing emails.
        """
        try:
            today = datetime.today().date()
            current_time = datetime.now()
            intermediate_folder = os.path.join(self.intermediate_folder)
            latest_attachments = {}
            copied_count = 0

            message = messages.GetFirst()
            while message:
                try:
                    received_time = message.ReceivedTime
                    received_date = received_time.date()
                    received_datetime = datetime.combine(received_date, received_time.time()).replace(second=0, microsecond=0)

                    if received_date == self.file_received_date.date() and self.min_hour_time <= received_time.time() < self.max_hour_time:
                        if self.subject_filter.lower() in message.Subject.lower():
                            copied_count += self.process_attachments(message.Attachments, received_datetime, intermediate_folder, latest_attachments)
                            logging.info(f"Download Complete! {copied_count} new files copied")
                except Exception as e:
                    logging.error(f"Error reading email: {e}")
                    self.delete_audit_log()
                    self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")
                    
                message = messages.GetNext()
            
        except Exception as e:
            logging.error(f"Error Processing messages: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")
            return None
    
    # ***********************************************************************
    #                      FILE CONVERSION AND RENAMING
    # ***********************************************************************
            
    def convert_file_extension(self, intermediate_path, intermediate_folder,file_name):
        """
        Converts a file from one extension to another.

        Args:
            intermediate_path (str): Path to the file to be converted.
            intermediate_folder (str): Folder containing the file.
            file_name (str): Name of the file.

        Returns:
            str: Path to the converted file.
        """
        try:
            
            file_nme = file_name.split('.')[0] + self.target_ext
            logging.info(f"file_name: {file_name}" )  
            if file_name.lower().endswith(".csv"):
                df = pd.read_csv(intermediate_path, encoding='latin', engine='python')
                intermediate_path = os.path.join(os.path.dirname(intermediate_path), file_nme)
                df.to_excel(intermediate_path, sheet_name=self.sheet_name, index=False)
                logging.info(f"Converted file: {file_nme}")

            elif file_name.lower().endswith(".xls"):
                xlApp = Dispatch('Excel.Application')
                xlApp.DisplayAlerts = False
                wb = xlApp.Workbooks.Open(intermediate_path)
                sheet = wb.Sheets(1)
                sheet.Name = self.sheet_name
                intermediate_path = os.path.join(os.path.dirname(intermediate_path), file_nme)
                wb.SaveAs(intermediate_path, 51)
                wb.Close(True)
                logging.info(f"Converted file: {file_nme}")

            return intermediate_path

        except Exception as e:
            logging.error(f"Error during file extension conversion: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")
            if file_name:
                logging.info(f"file_name: {file_name}" )  
                original_path = os.path.join(intermediate_folder, file_name)
                if os.path.exists(original_path):
                    os.remove(original_path)
                    logging.info(f"Removed original file copy: {original_path}" )
            if file_nme:
                logging.info(f"file_nme: {file_nme}" )
                original_path = os.path.join(intermediate_folder, file_nme)
                if os.path.exists(original_path):
                     os.remove(original_path)
                     logging.info(f"Removed original file copy: {original_path}" )
                     
                                
            return None
            
    def apply_file_renaming(self, intermediate_path, intermediate_folder, file_name):
        """
        Renames a file based on the configuration.

        Args:
            intermediate_path (str): Path to the file.
            intermediate_folder (str): Folder containing the file.
            file_name (str): Name of the file.

        Returns:
            str: Path to the renamed file.
        """
        try:
            
            current_time = datetime.now()
            today = datetime.today().date()
            end = datetime.combine(self.file_received_date.date(), datetime.strptime(f"{self.max_hour}:{self.max_min}", "%H:%M").time()).strftime("%m/%d/%Y %I:%M %p")
            threshold_sla_end = datetime.strptime(end, "%m/%d/%Y %I:%M %p")
            final_path = os.path.join(self.final_dest_path, self.Destn_file_name + self.target_ext)
            logging.info(f"Renamed file: {final_path}")
            
            if self.Data_validation:
                df = pd.read_excel(intermediate_path, sheet_name=self.sheet_name)
                column_values = df[self.field_name].drop_duplicates().tolist()
                logging.info(f"column_values: {column_values}")
                logging.info(f"field_value: {eval(self.field_value)}")
                if not any(col in column_values for col in eval(self.field_value)):
                    os.remove(intermediate_path)
                    logging.info(f"File validation failed, removed: {intermediate_path}")

                    # Optional cleanup of original unconverted file (if exists)
                    if file_name:
                        original_path = os.path.join(intermediate_folder, file_name)
                        if os.path.exists(original_path):
                            os.remove(original_path)
                            logging.info(f"Removed original file copy: {original_path}")
                    if current_time < threshold_sla_end: 
                        logging.info(f"Current time {current_time} is before SLA {threshold_sla_end}.. Skipping processing.")
                        return
                    if current_time >=threshold_sla_end: 
                        logging.info(f"Current time {current_time} is after SLA {threshold_sla_end}.. Time window is exhausted for the day.")
                        if not self.was_sla_missed_email_sent():
                            self.send_email(self.sender_email, self.receiver_email, self.cc_email, self.smtp_server, self.port, self.subject,self.Destn_file_name)
                        e="File didn't received for today. Sending warning mail."
                        self.delete_audit_log()
                        self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="SLA_MISSED_EMAIL_SENT")
                    
                    
                    return None

            return final_path

        except Exception as e:
            logging.error(f"Error during file renaming: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"Error during file renaming: {e}",self.processing_date, failure_type="FILE_VALIDATION_FAILED")
            if file_name:
                original_path = os.path.join(intermediate_folder, file_name)
                if os.path.exists(original_path):
                    logging.info(f"Removed original file copy: {original_path}" )  
                    os.remove(original_path)
                if os.path.exists(intermediate_path):
                    os.remove(intermediate_path)
                    logging.info(f"Removing converted renamed file : {intermediate_path}" )
                 
            return None
    
    def clean_unnamed_columns(self,intermediate_path,intermediate_folder, file_name):
        """
        Removes unnamed columns from a CSV or Excel file.
    
        Args:
            intermediate_path (str): Path to the file to clean.
            sheet_name (str, optional): Sheet name to use when saving Excel files.
    
        Returns:
            str: The same intermediate_path after cleaning.
        """
        try:
            file_nme = file_name.split('.')[0] + self.target_ext
            if intermediate_path.lower().endswith(('.xlsx', '.xls', '.csv')):
                if intermediate_path.lower().endswith('.csv'):
                    df = pd.read_csv(intermediate_path,encoding='latin', engine='python')
                    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                    df.dropna(how='all', inplace=True)
                    df.to_csv(intermediate_path, index=False)
                else:
                    df = pd.read_excel(intermediate_path, engine='openpyxl')
                    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                    df.dropna(how='all', inplace=True)
                    df.to_excel(intermediate_path, sheet_name=self.sheet_name or 'Sheet1', index=False)
    
                logging.info(f"Removed unnamed columns from: {intermediate_path}")
            return intermediate_path
            
        except Exception as e:
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")
            logging.error(f"Could not clean unnamed columns in {intermediate_path}: {e}")
            if file_name:
                logging.info(f"file_name: {file_name}" )  
                original_path = os.path.join(intermediate_folder, file_name)
                if os.path.exists(original_path):
                    os.remove(original_path)
                    logging.info(f"Removed original file copy: {original_path}" )
            if file_nme:
                logging.info(f"file_nme: {file_nme}" )
                original_path = os.path.join(intermediate_folder, file_nme)
                if os.path.exists(original_path):
                     os.remove(original_path)
                     logging.info(f"Removed original file copy: {original_path}" )
                     
                                
            return None

    
    
    
    
    # ***********************************************************************
    #                      ATTACHMENT PROCESSING
    # ***********************************************************************
    
    


    def extract_number(self,filename):
        try:
            # Remove the file extension
            file_base = os.path.splitext(filename)[0]
            # Split by '_' and extract the last part
            extracted_number = file_base.split('_')[-1]
            
            # Ensure extracted part is a valid number
            if extracted_number.isdigit():
                return int(extracted_number)  # Convert to integer if needed
            else:
                raise ValueError("No valid number found in the filename.")
        except Exception as e:
            # Handle errors (e.g., malformed filename)
            print(f"Error: {e}")
            return 0  # Return None or any default value in case of error


    import re

    def split_paths_from_string(self,path_string):
        """
        Extracts individual Windows-style paths from a string that looks like a list.
        
        Args:
            path_string (str): A string like "['C:\\path1\\', 'C:\\path2\\']"
        
        Returns:
            list: A list of cleaned path strings.
        """
        # Use regex to find all paths inside quotes
        paths = re.findall(r"'(.*?)'", path_string)
        return paths 
    
    
    def file_md5(self, filepath):
        """
        Calculate MD5 hash of a file.

        Args:
            filepath (str): Path to the file.

        Returns:
            str or None: MD5 hash string if successful, else None.
        """
        try:
            hash_md5 = hashlib.md5()
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            # You can log the error here if needed
            print(f"Error calculating MD5 for file {filepath}: {e}")
            return None
    def process_attachments(self, file_paths, received_datetime, intermediate_folder, is_local_file=False):
        """
        Processes attachments for renaming, validation, and copying.

        Args:
            file_paths (list): List of file paths or Outlook attachments.
            received_datetime (datetime): Date and time the file was received.
            intermediate_folder (str): Folder to store intermediate files.
            is_local_file (bool): Indicates if files are local or from Outlook.

        Returns:
            int: Number of files successfully processed.
        """
        try:
            today = datetime.today().date()
            copied_count = 0
            for file_obj in file_paths:
                # Determine file name and source file path
                if is_local_file:
                    file_name = os.path.basename(file_obj)
                    source_file_path = file_obj
                else:
                    file_name = file_obj.FileName
                    source_file_path = None  # Will be saved to intermediate folder later

                # Check if the file format is allowed
                allowed_formats = {'.pdf', '.xlsx', '.csv', '.docx', '.xls','.xlsb'}
                if not any(file_name.lower().endswith(ext) for ext in allowed_formats):
                    continue
                
                filename=self.extract_base_filename(file_name)
                fil_path = os.path.join(intermediate_folder, filename)
                # --- MD5 and timestamp duplicate check ---
                if os.path.isfile(fil_path):
                    temp_path = os.path.join(intermediate_folder, f"__temp_{filename}")
                    # Save incoming file/attachment to a temp path to calculate its hash
                    if is_local_file:
                        shutil.copy2(file_obj, temp_path)
                    else:
                        file_obj.SaveAsFile(temp_path)
                    existing_hash = self.file_md5(fil_path)
                    new_hash = self.file_md5(temp_path)
                    
                    modified_time = os.path.getmtime(fil_path)
                    modified_date = datetime.fromtimestamp(modified_time).date()
                    received_date = received_datetime.date()
                    logging.info(f"modified_date:{modified_date}")
                    logging.info(f"received_date:{received_date}")
                    
                    # Skip if both timestamp and content hash match
                    if existing_hash == new_hash and received_date == modified_date:
                        logging.info(f"Skipping duplicate in intermediate folder (MD5): {file_name}")
                        os.remove(temp_path)
                        continue
                    os.remove(temp_path)
                # --- End duplicate check ---

                # Added scheduled dependency check to move the file afte 12am to handle d-1 files
                if self.schedule_dependency_enabled and self.date_variable == "d-1":
                    logging.info("schedule Dependency check")
                    now = datetime.now()
                    start_time = time(19, 0)   # 7:00 PM
                    end_time = time(23, 59)    # 11:59 PM
                    current_time = now.time()
                    received_time = received_datetime.time()
                    if start_time <= received_time <= end_time and start_time <= current_time <= end_time:
                        logging.info(f"Files is in waiting state .It will copy after 12am")
                        continue
                        
                # Save file to intermediate folder
                intermediate_path = os.path.join(intermediate_folder, filename)
                os.makedirs(intermediate_folder, exist_ok=True)
                
                archive_path = os.path.join(self.archive_path, self.processing_date.strftime("%Y-%m-%d"))
                os.makedirs(archive_path, exist_ok=True)
                
                final_dest_path = os.path.join(self.final_dest_path)
                os.makedirs(final_dest_path, exist_ok=True)          
                
                if is_local_file:
                    shutil.copy2(file_obj, intermediate_path)
                else:
                    file_obj.SaveAsFile(intermediate_path)
                logging.info(f"File saved to intermediate folder: {intermediate_path}")

                # File Extension Conversion
                if self.File_conversion:
                    intermediate_path = self.convert_file_extension(intermediate_path, intermediate_folder,filename)
                    print(intermediate_path)
                    
                if self.unmerge_flag:
                    intermediate_path = self.clean_unnamed_columns(intermediate_path,intermediate_folder,filename)
                    print(intermediate_path)
                
                if intermediate_path and not self.is_valid_file_row_count(intermediate_path, filename):
                    continue  # Skip rest of processing for single-row files
                    
                    
                if intermediate_path:
                    # Final path preparation
                    final_path = os.path.join(self.final_dest_path, os.path.basename(intermediate_path))
                    if os.path.exists(final_path) and os.path.getsize(final_path) == os.path.getsize(intermediate_path):
                        logging.info(f"Skipping already existing file: {final_path}")
                        continue
    
                    # File Renaming and Data Validation
                    if self.rename_required:
                        final_path = self.apply_file_renaming(intermediate_path, intermediate_folder, file_name)
                        
                    if final_path:
                        # Copy file to target folder
                        self.copy_latest_to_target(
                            intermediate_path=intermediate_path,
                            target_folder=final_path,
                            archive_path=archive_path
                        )
                        
                        if self.is_multicopy == "Y":
                            logging.info(f"Secondary target path: {self.secondary_final_dest_path}")
                            extracted_paths = self.split_paths_from_string(self.secondary_final_dest_path)
                            for path in extracted_paths:
                                logging.info(f"Secondary path: {path}")
                                self.copy_latest_to_secondary_target(final_path,path)
                            
 
    
                        copied_count += 1
                        self.delete_audit_log()
                        self.insert_audit_log(self.success,"",self.processing_date)

            return copied_count

        except Exception as e:
            logging.error(f"Error in process_attachments: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")
            return None
            
    # ***********************************************************************
    #                      MESSAGE FILTERING
    # ***********************************************************************
        
    def get_today_messages(self, inbox):
        """
        Filters messages in the inbox based on today's date and time window.

        Args:
            inbox (object): The Outlook inbox folder.

        Returns:
            object: Filtered messages.
        """
        try:
            today = datetime.today().date()
            current_time = datetime.now()
            

            intermediate_folder = os.path.join(self.intermediate_folder)
            os.makedirs(intermediate_folder, exist_ok=True)
            os.makedirs(self.final_dest_path, exist_ok=True)
            
            start = datetime.combine(self.file_received_date.date(), datetime.strptime(f"{self.min_hour}:{self.min_min}", "%H:%M").time()).strftime("%m/%d/%Y %I:%M %p")
            end = datetime.combine(self.file_received_date.date(), datetime.strptime(f"{self.max_hour}:{self.max_min}", "%H:%M").time()).strftime("%m/%d/%Y %I:%M %p")
            end1 = datetime.combine(self.file_received_date.date(), datetime.strptime(f"{self.max_hour}:{self.max_min}", "%H:%M").time()).strftime("%m/%d/%Y %I:%M %p")
            threshold_sla_end = datetime.strptime(end1, "%m/%d/%Y %I:%M %p")
            filter_str = f"[ReceivedTime] >= '{start}' AND [ReceivedTime] <= '{end}' AND [Subject] = '{self.subject_filter}'" 
            messages = inbox.Items.Restrict(filter_str)
            messages.Sort("[ReceivedTime]", False)
            
            if messages.Count == 0:
                if current_time  < threshold_sla_end :
                    logging.info(f"Current time {current_time} is before SLA {threshold_sla_end}. Skipping processing.")
                elif current_time >= threshold_sla_end:
                    logging.info(f"Current time {current_time} is after SLA {threshold_sla_end}. Time window is exhausted for the day.")
                    logging.info("File didn't received. Sending warning mail.")
                    if not self.was_sla_missed_email_sent():
                        self.send_email(self.sender_email, self.receiver_email, self.cc_email, self.smtp_server, self.port, self.subject,self.Destn_file_name)
                    e="File didn't received for today. Sending warning mail."
                    self.delete_audit_log()
                    self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="SLA_MISSED_EMAIL_SENT")
                return None

            return messages
        except Exception as e:
            logging.error(f"Error get_today_messages: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="SLA_MISSED_EMAIL_SENT")
            return None
        
    def process_outlook_file(self, file_config):
        """
        Processes a file from an Outlook folder.

        Args:
            file_config (dict): Configuration for the file to process.
        """
        try:
            self.get_extraction_config(file_config)

            inbox = self.connect_to_outlook_folder()
            if not inbox:
                return
            
            logging.info(f"**************************Process started for: {file_config['src_file_name']}***********************************************")
            messages = self.get_today_messages(inbox)
            
            if not messages:
                return
            
            logging.info(f"[outlook] Processing file: {file_config['src_file_name']}")
            self.process_messages(messages)
            logging.info(f"**************************Process Completed for: {file_config['src_file_name']}***********************************************")
            
        except Exception as e:
            logging.error(f"Error processing emails: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")


    #def extract_base_filename(self,file_name):
    #    """
    #    Extracts the base filename by removing:
    #    - trailing timestamp (_yyyymmdd_hhmmss)
    #    - or trailing numeric ID (_123, _476, etc.)
    #    Keeps the file extension intact.
    #
    #    Args:
    #        file_name (str): Original filename (e.g., 'report_20240504_223000.csv').
    #
    #    Returns:
    #        str: Cleaned filename with extension preserved (e.g., 'report.csv').
    #    """
    #    try:
    #        name, ext = os.path.splitext(file_name)
    #    
    #        # Remove timestamp pattern
    #        name = re.sub(r"_\d{8}_\d{6}$", "", name)
    #        # Remove simple numeric suffix like _476
    #        name = re.sub(r"_\d+$", "", name)
    # 
    #        return f"{name}{ext}"
    #    except Exception as e:
    #        logging.error(f"Error Extracting base file name: {e}")
    
    
    def extract_base_filename(self, file_name):
        """
        Extracts the base filename by removing:
        - trailing timestamp (_yyyymmdd_hhmmss or _yyyy-mm-dd)
        - trailing numeric ID (_123, _476, etc.)
        Keeps the file extension intact if present.
        """
        try:
            name, ext = os.path.splitext(file_name)
        
            # Remove timestamp pattern _yyyymmdd_hhmmss
            name = re.sub(r"_\d{8}_\d{6}$", "", name)
            # Remove date pattern _yyyy-mm-dd
            name = re.sub(r"_\d{4}-\d{2}-\d{2}$", "", name)
            # Remove simple numeric suffix like _476
            name = re.sub(r"_\d+$", "", name)
        
            return f"{name}{ext}"
        except Exception as e:
            logging.error(f"Error Extracting base file name: {e}")
    
    
    
        
    # ***********************************************************************
    #                           EMAIL UTILITIES
    # ***********************************************************************
    
    def send_email(self, sender_email, receiver_email, cc_email, smtp_server, port, subject, Destn_file_name, status="not_received"):
        """
        Sends an email notification.

        Args:
            sender_email (str): Sender's email address.
            receiver_email (str): Recipient's email address.
            cc_email (str): CC email addresses.
            smtp_server (str): SMTP server address.
            port (int): SMTP server port.
            subject (str): Email subject.
            Destn_file_name (str): File name being processed.
        """
        try:
            today = datetime.today().date()
            currentdate = self.processing_date.strftime("%Y-%m-%d")
            msg = MIMEMultipart()
            #msg["Subject"] = self.region +":"+ f"{Destn_file_name}" + "-" + " "+f"{subject}" + " " + "for" +" "+ f"{currentdate}"
            msg["Subject"] = "["+self.Env+"]"+"-"+ self.region +":"+ f"{Destn_file_name}" + "-" + " "+f"{subject}" + " " + "for" +" "+ f"{currentdate}"
            msg["From"] = sender_email
            msg["To"] = receiver_email
            msg["CC"] = cc_email
            
            status = status if status in ["not_received", "received_late","folder_not_accessible","empty_file"] else "not_received"
            if status == "received_late":
            
                dynamic_content = """
                <html>
                <body>
                    <p>Hello Everyone,</p>
                    <p>This is to inform you that the file {} expected for today {} for respective region {} was received after the SLA cutoff time for the time slot {} .</p>
                            <p>Kindly investigate and take necessary action at the earliest.</p>
                                        <p>Thanks,
                    <br>Aurba SC360 Team</p>                    
                </body>
                </html>
                """.format(f'<b>{Destn_file_name}</b>',f'<b>{currentdate}</b>',f'<b>{self.region}</b>',f'<b>{self.time_window_label}</b>')
            elif status == "folder_not_accessible":
                dynamic_content = """
                <html>
                <body>
                    <p>Hello Everyone,</p>
                    <p>This is to inform you that the file {} expected for today {} for respective region {} is not accessable in the source folder for the time slot {}.</p>
                            <p>Kindly investigate and take necessary action at the earliest.</p>
                                        <p>Thanks,
                    <br>Aurba SC360 Team</p>                    
                </body>
                </html>
                """.format(f'<b>{Destn_file_name}</b>',f'<b>{currentdate}</b>',f'<b>{self.region}</b>',f'<b>{self.time_window_label}</b>')
            elif status == "empty_file":
                dynamic_content = """
                <html>
                <body>
                    <p>Hello Everyone,</p>
                    <p>This is to inform you that the file {} that we recieved today {} for respective region {} is empty in the source folder for the time slot {}.</p>
                            <p>Kindly investigate and take necessary action at the earliest.</p>
                                        <p>Thanks,
                    <br>Aurba SC360 Team</p>                    
                </body>
                </html>
                """.format(f'<b>{Destn_file_name}</b>',f'<b>{currentdate}</b>',f'<b>{self.region}</b>',f'<b>{self.time_window_label}</b>')
            else:
                dynamic_content = """
                <html>
                <body>
                    <p>Hello Everyone,</p>
                    <p>This is to inform you that the file {} expected for today {} for respective region {} has not been received yet for the time slot {} .</p>
                            <p>Kindly investigate and take necessary action at the earliest.</p>
                                        <p>Thanks,
                    <br>Aurba SC360 Team</p>                    
                </body>
                </html>
                """.format(f'<b>{Destn_file_name}</b>',f'<b>{currentdate}</b>',f'<b>{self.region}</b>',f'<b>{self.time_window_label}</b>')
    
            recipients = receiver_email.split(",")
            cc = cc_email.split(",")
            addrs = recipients + cc
            bodytext = MIMEText(dynamic_content, "html")
            msg.attach(bodytext)

            with smtplib.SMTP(smtp_server, port) as server:
                server.sendmail(sender_email, addrs, msg.as_string())

            logging.info("Email sent successfully.")

        except Exception as e:
            logging.error(f"Failed to send email: {str(e)}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date)
            
               
    def is_valid_file_row_count(self, intermediate_path, file_name):
        """
        Checks if the file has more than 1 row. If not:
        - Logs an audit
        - Sends email alert using send_email()
    
        Args:
            intermediate_path (str): Path to the file after conversion (e.g., .xlsx)
            file_name (str): Original file name before conversion (e.g., .csv or .xlsb)
    
        Returns:
            bool: True if valid, False if skipped
        """
        try:
            logging.info("file size check :started")
            df = None
            if intermediate_path.lower().endswith(".xlsx"):
                df = pd.read_excel(intermediate_path,engine='openpyxl')
            elif intermediate_path.lower().endswith(".csv"):
                df = pd.read_csv(intermediate_path,encoding='latin', engine='python')
            elif intermediate_path.lower().endswith(".xlsb"):
                df = pd.read_excel(intermediate_path)
    
            if df is not None and df.shape[0] <= 1:
                logging.warning(f"File {file_name} has only {df.shape[0]} row(s) — skipping.")
    
                # Insert audit log
                self.insert_audit_log(
                    self.failure,
                    f"Empty row found in file {file_name} — skipped",
                    self.processing_date,
                    failure_type="EMPTY_FILE"
                )
    
                # Send alert email
                self.send_email(
                    self.sender_email,
                    self.receiver_email,
                    self.cc_email,
                    self.smtp_server,
                    self.port,
                    "Empty row found in file.Skipped processing.",
                    self.Destn_file_name,
                    status="empty_file"
                )
    
                return False
    
            return True
    
        except Exception as e:
            logging.error(f"Unable to validate row count for {file_name}: {e}")
            return False  # Let it continue if validation fails


           
    def rename_file(self, manual_folder_temppath, file_path):
    
        try:
            # Extract the directory, filename, and extension
            directory, filename = os.path.split(file_path)
            file_base, file_extension = os.path.splitext(filename)
            
            # Generate the new filename
            new_filename = '_'.join(file_base.split('_')[:-1]) + file_extension
            
            # Construct the full new file path
            new_file_path = os.path.join(manual_folder_temppath, new_filename)
            
            # Remove the file at the new path if it already exists
            if os.path.exists(new_file_path):
                os.remove(new_file_path)
            
            # Rename the file
            os.rename(file_path, new_file_path)
            print(f"File renamed to: {new_file_path}")
            
            return new_file_path
        except Exception as e:
            logging.error(f"Unable to rename the  {filename}: {e}")
            return False  # Let it continue if validation fails
        
            


    def manual_process_file(self, file_config, manual_folder_path,manual_folder_temppath):
        """
        Processes a manually placed file from a specified folder, 
        performing all validations (renaming, conversion, data validation, etc.)
        without any time window or SLA checks.
    
        Args:
            file_config (dict): Configuration for the file to process (from DB or YAML).
            manual_folder_path (str): Path where the support team dropped the file.
        """
        try:
            logging.info("********************************************************************")
            logging.info(f"*** Manual Process started for: {file_config['src_file_name']} ***")
            self.get_extraction_config(file_config)

            
            # Only look for the expected file(s) in the manual folder
            expected_files = [
            f for f in os.listdir(manual_folder_path)
            if f.lower().startswith(self.raw_filename.lower().split('.')[0])
            and f.lower().endswith(self.source_ext.lower())
            and os.path.isfile(os.path.join(manual_folder_path, f))
            and os.path.getsize(os.path.join(manual_folder_path, f)) > 0
            ]

            if not expected_files:
                logging.warning(f"No expected file named '{self.raw_filename}' found in {manual_folder_path}")
                return
            
            for file_name in expected_files:
                file_path = os.path.join(manual_folder_path, file_name)
                logging.info(f"file_path before rename: {file_path}")
                manual_path = self.rename_file(manual_folder_temppath,file_path)
                logging.info(f"manual_path after rename: {manual_path}")
                
                
                if not manual_path:
                    continue
                    
                file_name = os.path.basename(manual_path)
                
                
                # Check if the file format is allowed
                allowed_formats = {'.pdf', '.xlsx', '.csv', '.docx', '.xls','.xlsb'}
                if not any(file_name.lower().endswith(ext) for ext in allowed_formats):
                    continue
                    
                filename=self.extract_base_filename(file_name)
                logging.info(f"extract_base_filename: {filename}")
                
                # Prepare target folders as per normal
                intermediate_folder = os.path.join(self.intermediate_folder)
                os.makedirs(intermediate_folder, exist_ok=True)
                
                # --- MD5 and timestamp duplicate check ---
                fil_path = os.path.join(intermediate_folder, filename)
                logging.info(f"fil_path: {fil_path}")
                logging.info(f"manual_path: {manual_path}")
                if os.path.isfile(fil_path):
                    existing_hash = self.file_md5(fil_path)
                    new_hash = self.file_md5(manual_path)
                    existing_file_time = os.path.getmtime(fil_path)
                    existing_file_date = datetime.fromtimestamp(existing_file_time).date()
                    new_file_time = os.path.getmtime(manual_path)
                    new_file_date = datetime.fromtimestamp(new_file_time).date()
                    logging.info(f"existing_file_time:{existing_file_date}")
                    logging.info(f"new_file_date:{new_file_date}")

                    
                    # Skip if both timestamp and content hash match
                    if existing_hash == new_hash and existing_file_date == new_file_date:
                        logging.info(f"Skipping duplicate in intermediate folder (MD5): {file_name}")
                        os.remove(manual_path)
                        continue
                # --- End duplicate check ---    

                #archive_path = os.path.join(self.archive_path, datetime.now().strftime("%Y-%m-%d"))
                archive_path = os.path.join(self.archive_path, self.processing_date.strftime("%Y-%m-%d"))
                os.makedirs(archive_path, exist_ok=True)
                
                final_dest_path = os.path.join(self.final_dest_path)
                os.makedirs(final_dest_path, exist_ok=True)
    
                # Copy to intermediate folder for consistent processing
                intermediate_path = os.path.join(intermediate_folder, file_name)
                shutil.copy2(manual_path, intermediate_path)
                logging.info(f"Copied manual file {file_name} to intermediate folder {intermediate_folder}")
    
                # File Extension Conversion (if needed)
                if self.File_conversion:
                    intermediate_path = self.convert_file_extension(intermediate_path, intermediate_folder, file_name)
    
                # Clean columns if required
                if self.unmerge_flag:
                    intermediate_path = self.clean_unnamed_columns(intermediate_path, intermediate_folder, file_name)
    
                # Validate row count
                if intermediate_path and not self.is_valid_file_row_count(intermediate_path, file_name):
                    continue
    
                # File Renaming and Data Validation
                final_path = os.path.join(self.final_dest_path, os.path.basename(intermediate_path))
                
                if self.rename_required:
                    final_path = self.apply_file_renaming(intermediate_path, intermediate_folder, file_name)

                if final_path:
                    # Copy to target and archive as per normal
                    self.copy_latest_to_target(
                        intermediate_path=intermediate_path,
                        target_folder=final_path,
                        archive_path=archive_path
                    )
                    
                    
                    if self.is_multicopy == "Y":
                        logging.info(f"Secondary target path: {self.secondary_final_dest_path}")
                        extracted_paths = self.split_paths_from_string(self.secondary_final_dest_path)
                        for path in extracted_paths:
                            logging.info(f"Secondary path: {path}")
                            self.copy_latest_to_secondary_target(final_path,path)
                            
                    self.delete_audit_log()
                    self.insert_audit_log(self.success, "Manual Push completed", self.processing_date)
                    logging.info(f"Manual Push completed for {file_name}")
                    logging.info("********************************************************************")    
           
        except Exception as e:
            logging.error(f"Error in manual_process_file: {e}")

    
    # ***********************************************************************
    #                          MAIN EXECUTION
    # ***********************************************************************
    
    def initialize_and_run(self):
        try:
            print(f"[Configured region] Running for region: {self.region}")
            
            file_entries = self.load_config_from_db(self.region)
            self.setup_logging()
            
            logging.info(f"[Configured region] Running for region: {self.region}")

            if not file_entries:
                logging.info(f"No configuration found for region: {self.region}. Skipping execution.")
                return

            for entry in file_entries:
            
                self.date_variable = entry.get("date_to_be_checked", "d")
                self.schedule_dependency_flag = entry.get("schedule_dependency_enabled","false")
                logging.info(f"schedule_dependency_flag: {self.schedule_dependency_flag}")
                self.file_received_date, self.processing_date = self.get_processing_and_file_dates()
                logging.info(f"file_received_date: {self.file_received_date}")
                logging.info(f"processing_date: {self.processing_date}")
                
                if not self.is_allowed_today(entry["exclude_days"]):
                    logging.info(f"Skipping {entry['src_file_name']} – as today is excluded.")
                    continue

                if entry["extractor_type"].lower() == "outlook":
                    self.process_outlook_file(entry)
                elif entry["extractor_type"].lower() == "sftp":
                    self.process_sftp_file(entry)
                else:
                    logging.warning(f"Unknown extractor type: {entry['name']}")

        except Exception as e:
            logging.error(f"Error in initialize_and_run: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")
            
            
    def initialize_manual_run(self):
    
        try:
            
            region_list = ['AMS','APJ','EMEA','WW']
            
            for region in region_list:
                self.setup_logging_for_manual(region)
                
                logging.info("********************************************************************")
                print(f"Manual run started for SLA missed files for {region}")
                # Set the manual path from where files will be processed         
                manual_folder_path = os.path.join(self.manual_folder_path,region)
                os.makedirs(manual_folder_path, exist_ok=True)
                manual_folder_temppath = os.path.join(self.manual_folder_temppath,region)
                os.makedirs(manual_folder_temppath, exist_ok=True)                  
                    
                # Fetch the list of files from the source SFTP folder
                file_list = self.fetch_sla_missed_files_from_manual_folder(manual_folder_path)
                files = [os.path.basename(f) for f in file_list]
    
                logging.info(f"files: {files}")
                
                # If no files are found, exit
                if not files: 
                    logging.info("No files to process.")
                    continue
                
                print("Get the metadata entries against each file")             
                all_file_entries = []
        
                for file in files:
                    file_entries = self.load_config_from_db_using_src_file(region, file)
                    if file_entries:
                        for entry in file_entries:
                            self.date_variable = entry.get("date_to_be_checked", "d")
                            self.schedule_dependency_flag = entry.get("schedule_dependency_enabled","false")
                            logging.info(f"schedule_dependency_flag: {self.schedule_dependency_flag}")
                            
                            self.region = entry.get("region", "WW")
                            logging.info(f"region: {self.region}")
                            
                            self.file_received_date, self.processing_date = self.get_processing_and_file_dates()
                            logging.info(f"file_received_date: {self.file_received_date}")
                            logging.info(f"processing_date: {self.processing_date}")
                            
                            #trigger the manualprocess 
                            self.manual_process_file(entry, manual_folder_path,manual_folder_temppath)
                            
                            all_file_entries.extend(file_entries)
                            logging.info("********************************************************************")
                        
                    else:
                        logging.info(f"No config found for file: {file}")
        
                # Now you can use `all_file_entries` for further processing
                ##logging.info(f"Total config entries loaded: {len(all_file_entries)}")
    
        except Exception as e:
            logging.error(f"Error in initialize_manual_run: {e}")
            self.delete_audit_log()
            self.insert_audit_log(self.failure,f"{e}",self.processing_date, failure_type="PROCESSING_ERROR")
         

if __name__ == "__main__":
    # ***********************************************************************
    #                      START POINT OF EXECUTION
    # ***********************************************************************
    
    time_config_path = "D:\\Technical\\FileExtraction\\config\\file_extraction_config.yml"

 
    region = None
    manual_mode = None
    
    if len(sys.argv) > 2:
        print(f"Passing incorrect number of arguments. Expected one, passed two")
        sys.exit(1)
 
    # Handle region argument if passed
    if len(sys.argv) == 2:
        Input_param = sys.argv[1].upper()
        if Input_param not in ["WW", "Y"]:
            print(f"Invalid Input_param passed: {Input_param}")
            sys.exit(1)
        else:
            if Input_param == "WW":
               region = Input_param
            elif Input_param == "Y":
                manual_mode = Input_param
   
    # Instantiate the downloader
    downloader = OutlookAttachmentDownloader(time_config_path)
 
    # If region not passed, determine it based on current time
    if not manual_mode:
        if not region:
            region = downloader.get_region_by_time(
                downloader.APJ_Start_time, downloader.APJ_End_time,
                downloader.AMS_Start_time, downloader.AMS_End_time,
                downloader.EMEA_Start_time, downloader.EMEA_End_time
            )
            if not region:
                print("Current time is outside all defined region windows. Exiting.")
                sys.exit(0)
 
    if not manual_mode:
        downloader.region = region
 
    if manual_mode:
        downloader.manual_mode = manual_mode
 
    # Execute based on mode
    if getattr(downloader, 'manual_mode', None) == "Y":
        print("Running in manual mode.")
        downloader.initialize_manual_run()
    else:
        print(f"Running in automatic mode for region: {downloader.region}")
        downloader.initialize_and_run()    

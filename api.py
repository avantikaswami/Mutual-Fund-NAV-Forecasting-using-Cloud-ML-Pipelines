
#           _____                    _____                    _____          
#          /\    \                  /\    \                  /\    \         
#         /::\    \                /::\    \                /::\    \        
#        /::::\    \              /::::\    \               \:::\    \       
#       /::::::\    \            /::::::\    \               \:::\    \      
#      /:::/\:::\    \          /:::/\:::\    \               \:::\    \     
#     /:::/__\:::\    \        /:::/__\:::\    \               \:::\    \    
#    /::::\   \:::\    \      /::::\   \:::\    \              /::::\    \   
#   /::::::\   \:::\    \    /::::::\   \:::\    \    ____    /::::::\    \  
#  /:::/\:::\   \:::\    \  /:::/\:::\   \:::\____\  /\   \  /:::/\:::\    \ 
# /:::/  \:::\   \:::\____\/:::/  \:::\   \:::|    |/::\   \/:::/  \:::\____\
# \::/    \:::\  /:::/    /\::/    \:::\  /:::|____|\:::\  /:::/    \::/    /
#  \/____/ \:::\/:::/    /  \/_____/\:::\/:::/    /  \:::\/:::/    / \/____/ 
#           \::::::/    /            \::::::/    /    \::::::/    /          
#            \::::/    /              \::::/    /      \::::/____/           
#            /:::/    /                \::/____/        \:::\    \           
#           /:::/    /                  ~~               \:::\    \          
#          /:::/    /                                     \:::\    \         
#         /:::/    /                                       \:::\____\        
#         \::/    /                                         \::/    /        
#          \/____/                                           \/____/         
                                                                           

import os
import time
import requests
from abc import ABC

from typing import NamedTuple

from datetime import datetime
from decimal import Decimal

from models.base import MutualFundNAV, MutualFundScheme, KuveraPotfolioInformation
from models.pandas_schema import METADATA_SCHEMA, NAV_SCHEMA
from database.router import get_engine, get_session

from.kuvera_uti import create_from_json

from dotenv import load_dotenv
load_dotenv()

class MPTask(NamedTuple):
    """
        Encapsulates all arguments needed by the worker / individual API calls.

        - base_url: the root endpoint string (e.g. "https://api.mfapi.in/mf")
        - scheme_code: a string identifier for the fund/scheme
        - latest: a bool indicating whether to hit “/latest”

    """
    base_url: str
    scheme_code: str
    latest: bool

class KuveraTask(NamedTuple):
    """
        Encapsulates all arguments needed by the worker / individual API calls.

        - base_url: the root endpoint string (e.g. "https://mf.captnemo.in/kuvera")
        - scheme_code: a string identifier for the fund/scheme
        - isn: The isin to hit Kuvera

    """
    base_url: str
    scheme_code : str
    isn: str
    type_code : str

# Global To Keep Consider in worker Processes
_engine = None
_Session = None

class RequestMixin(ABC):

    '''
        A Mixin Base Abstract Class which Can be sub-classed to add Api Calling Functionality for the Below Sites.
            1. https://api.mfapi.in
            2. https://mf.captnemo.in/kuvera

        Note : Has a worker Method to Run into Process / Thread Pool to Avail Multi Processing or Distrubuted Processing.
    '''

    BASE_URL = "https://api.mfapi.in/mf"  # Base URL to get mutual Fund information about Schema
    KUVERA_BASE_URL = "https://mf.captnemo.in/kuvera" # Base URL to get mutual Fund Information

    def hit_api_mf(self, **kwargs):

        '''
           __
          /__)  _  _     _   _ _/   _
         / (   (- (/ (/ (- _)  /  _)
                  /
        
            Simple Single Request to API (https://api.mfapi.in/mf)
        '''

        if kwargs.get("scheme_code"):
            if kwargs.get("latest"):
                url = f"{self.BASE_URL}/{kwargs['scheme_code']}/latest"
            else:
                url = f"{self.BASE_URL}/{kwargs['scheme_code']}"
        else:
            url = self.BASE_URL

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to fetch from {url}: {e}")

        try:
            return response.json()
        
        except ValueError:
            raise RuntimeError(f"Invalid JSON received from {url}")


    def hit_api_kuvera(self, **kwargs):

        '''
           __
          /__)  _  _     _   _ _/   _
         / (   (- (/ (/ (- _)  /  _)
                  /
        
            Simple Single Request to API (https://mf.captnemo.in/kuvera)
        '''

        if kwargs.get("isin"):
            url = f"{self.BASE_URL}/{kwargs['isin']}"
        else:
            url = self.BASE_URL

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to fetch from {url}: {e}")

        try:
            return response.json()
        
        except ValueError:
            raise RuntimeError(f"Invalid JSON received from {url}")

    @staticmethod
    def init_db():
        """
            Lazily initialize the Engine and Sessionmaker in each worker process.
        """
        global _engine, _Session
        if _Session is None:
            # _engine = get_engine()
            _Session = get_session(db_type=os.environ.get('DATABASE_TYPE'))

    @staticmethod
    def _mp_worker(task : MPTask):

        """
           __
          /__)  _  _     _   _ _/   _
         / (   (- (/ (/ (- _)  /  _)
                  /   
                 
            task = MPTask(base_url: str, scheme_code: str, latest_flag: bool)
            Lives at module scope so that ProcessPoolExecutor can pickle it.

        """
        base_url, scheme_code, latest = task

        if scheme_code:
            if latest:
                url = f"{base_url}/{scheme_code}/latest"
            else:
                url = f"{base_url}/{scheme_code}"
        else:
            url = base_url

        try:
            response = requests.get(url, timeout=10)
            time.sleep(0.2)
            response.raise_for_status()
        except Exception as e:
            return scheme_code, e

        try:
            return scheme_code, response.json()
        except ValueError:
            return scheme_code, RuntimeError(f"Invalid JSON received from {url}")

    @staticmethod
    def _mp_worker_kuvera(task : KuveraTask):

        """
           __
          /__)  _  _     _   _ _/   _
         / (   (- (/ (/ (- _)  /  _)
                  /   
                 
            task = MPTask(base_url: str, scheme_code: str, latest_flag: bool)
            Lives at module scope so that ProcessPoolExecutor can pickle it.

        """
        base_url, scheme_code, isin, type_code = task

        url = f"{base_url}/{isin}"

        response = requests.get(url, timeout=10)
        time.sleep(0.2)

        if response.status_code == 200:
            data =  response.json()[-1]
        else:
            return scheme_code, response.status_code

        try:
            data['scheme_code'] = scheme_code
            data['isin'] = isin
            # data['isn_code'] = isin
            data['type_code'] = type_code
            return scheme_code, data
        except ValueError:
            return scheme_code, RuntimeError(f"Invalid JSON received from {url}")
                
    @staticmethod   
    def _mp_worker_db(task: MPTask):
        """
           __
          /__)  _  _     _   _ _/   _
         / (   (- (/ (/ (- _)  /  _)
                  /   
                 
            task = MPTask(base_url: str, scheme_code: str, latest_flag: bool)
            Lives at module scope so that ProcessPoolExecutor can pickle it.
            Tosses requests to Database in ORM

        """
        # RequestMixin.init_db()
        import pandas as pd
        from azure.storage.blob import BlobServiceClient
        import uuid

        base_url, scheme_code, latest = task

        if scheme_code:
            url = f"{base_url}/{scheme_code}/latest" if latest else f"{base_url}/{scheme_code}"
        else:
            url = base_url

        try:
            resp = requests.get(url, timeout=10)
            time.sleep(0.2)
            resp.raise_for_status()
        except Exception as e:
            return scheme_code, e

        try:
            payload = resp.json()
        except ValueError:
            return scheme_code, RuntimeError(f"Invalid JSON received from {url}")
        
        # account_url = os.getenv('ACCOUNT_URL')
        # sas_token = os.getenv('SAS_TOKEN')
        # container_name = os.getenv('CONTAINER_NAME')

        # blob_service_client = BlobServiceClient(account_url, credential=sas_token)

        try:
            # with _Session() as session:
            #     # Push Data of Metadata to Database
            #     fund_scheme_data = payload.get('meta')
            #     fund_scheme_entry = MutualFundScheme(
            #         scheme_code = fund_scheme_data.get("scheme_code"),
            #         fund_house = fund_scheme_data.get("fund_house"),
            #         scheme_type = fund_scheme_data.get("scheme_type"),
            #         scheme_category = fund_scheme_data.get("scheme_category"),
            #         scheme_name = fund_scheme_data.get("scheme_name"),
            #         isin_growth = fund_scheme_data.get("isin_growth"),
            #         isin_div_reinvestment = fund_scheme_data.get("isin_div_reinvestment")
            #     )
            #     session.add(fund_scheme_entry)
            #     session.commit()

            unique_id = uuid.uuid4().__str__()
            fund_scheme_data = payload.get('meta')
            metadata_mappings = {
                'scheme_code' : fund_scheme_data.get("scheme_code"),
                'fund_house' : fund_scheme_data.get("fund_house"),
                'scheme_type' : fund_scheme_data.get("scheme_type"),
                'scheme_category' : fund_scheme_data.get("scheme_category"),
                'scheme_name' : fund_scheme_data.get("scheme_name"),
                'isin_growth' : fund_scheme_data.get("isin_growth"),
                'isin_div_reinvestment' : fund_scheme_data.get("isin_div_reinvestment")
            }
            metadata_dataframe = pd.DataFrame([metadata_mappings],columns=METADATA_SCHEMA)

            path = f"historicaldata/metadata/mf_historical_{fund_scheme_data.get('scheme_code')}_{unique_id}_metadata.parquet"
            # blob_service_client.get_container_client(container_name).upload_blob(path, metadata_dataframe.to_parquet())
            metadata_dataframe.to_parquet(path, index=False)

            scheme_code = fund_scheme_data.get("scheme_code")
            fund_nav_historical =  payload.get('data')
            first_date = fund_nav_historical[0].get('date')
            mappings = []
            date_obj =  datetime.strptime(first_date, '%d-%m-%Y')
            
            if int(date_obj.year) not in (2025, 2024):
                return scheme_code, {
                    'status' : payload.get("status"),
                    'date' : 'no need'
                }

            # Push Data of NAV to Database
            for item in fund_nav_historical:
                try:
                    date_str = item.get('date')
                    nav_str  = item.get('nav')
                    nav_date = datetime.strptime(date_str, "%d-%m-%Y").date()
                    nav_val  = Decimal(nav_str)
                    mappings.append({
                        'insert_date' : datetime.now(),
                        'scheme_code': scheme_code,
                        'date': nav_date,
                        'nav': nav_val
                    })
                except Exception:
                    continue

            try:
                dataframe = pd.DataFrame(mappings,columns=NAV_SCHEMA)
                path = f'historicaldata/neededdata/mf_historical_{fund_scheme_data.get("scheme_code")}_{unique_id}_historical_data.parquet'
                # blob_service_client.get_container_client(container_name).upload_blob(path, dataframe.to_parquet())
                dataframe.to_parquet(path, index=False)
                # with _Session() as session:
                #     if mappings:
                #         session.bulk_insert_mappings(MutualFundNAV, mappings)
                #         session.commit()

            except Exception as db_err:
                return scheme_code, db_err
                
                # session.commit()

        except Exception as db_err:
            return scheme_code, db_err

        return scheme_code, {
            'status' : payload.get("status"),
            'date' : first_date
        }
    
    @staticmethod
    def _mp_worker_db_dump(payloads):
        """
           __
          /__)  _  _     _   _ _/   _
         / (   (- (/ (/ (- _)  /  _)
                  /   
                 
            Takes in Payloads of Daily Loads into Database
            Lives at module scope so that ProcessPoolExecutor can pickle it.
            Tosses requests to Database in ORM
        """
        # RequestMixin.init_db()
        import pandas as pd
        from azure.storage.blob import BlobServiceClient
        import uuid

        try:
            mappings = []
            code_mappings = {}
            for payload in payloads:
                fund_scheme_data = payload.get('meta')
                scheme_code = fund_scheme_data.get("scheme_code")
                fund_nav_historical =  payload.get('data')
                first_date = fund_nav_historical[0].get('date')
                code_mappings[scheme_code] = first_date
                for item in fund_nav_historical:
                    try:
                        date_str = item.get('date')
                        nav_str  = item.get('nav')
                        nav_date = datetime.strptime(date_str, "%d-%m-%Y").date()
                        nav_val  = Decimal(nav_str)
                        mappings.append({
                            'insert_date' : datetime.now(),
                            'scheme_code': scheme_code,
                            'date': nav_date,
                            'nav': nav_val
                        })
                    except Exception:
                        continue

            try:
                dataframe = pd.DataFrame(mappings,columns=['insert_date','scheme_code','date','nav'])

                account_url = os.getenv('ACCOUNT_URL')
                sas_token = os.getenv('SAS_TOKEN')
                container_name = os.getenv('CONTAINER_NAME')
                today = datetime.today()
                path = f"daily_extracts/{today.year}/{today.month:02}/{today.day:02}/mf_daily_navs_{uuid.uuid4().__str__()}_{today.year}_{today.month:02}_{today.day:02}.parquet"

                blob_service_client = BlobServiceClient(account_url, credential=sas_token)
                blob_service_client.get_container_client(container_name).upload_blob(path, dataframe.to_parquet())

                # with _Session() as session:
                #     if mappings:
                #         session.bulk_insert_mappings(MutualFundNAV, mappings)
                #         session.commit()

            except Exception as db_err:
                return db_err
            
        except Exception as db_err:
            return db_err

        return code_mappings

    @staticmethod
    def _mp_worker_db_dump_kuvera(payloads):
        """
           __
          /__)  _  _     _   _ _/   _
         / (   (- (/ (/ (- _)  /  _)
                  /   
                 
            Takes in Payloads of Kuvera Loads into Database
            Lives at module scope so that ProcessPoolExecutor can pickle it.
            Tosses requests to Database in ORM
        """
        # RequestMixin.init_db()

        import pandas as pd
        from azure.storage.blob import BlobServiceClient
        import uuid

        account_url = os.getenv('ACCOUNT_URL')
        sas_token = os.getenv('SAS_TOKEN')
        container_name = os.getenv('CONTAINER_NAME')
        unique_id = uuid.uuid4().__str__()

        blob_service_client = BlobServiceClient(account_url, credential=sas_token)

        try:
            mappings = []
            code_mappings = len(payloads)
            for payload in payloads:
                try:
                    mappings.append(create_from_json(payload))
                except Exception:
                    continue

            try:
                
                dataframe = pd.DataFrame(mappings)

                today = datetime.today()

                path = f"kuveraextracts/{today.year}/{today.month:02}/{today.day:02}/mf_kuvera_information_{unique_id}_{today.year}_{today.month:02}_{today.day:02}.parquet"
                blob_service_client.get_container_client(container_name).upload_blob(path, dataframe.to_parquet())

                # with _Session() as session:
                #     if mappings:
                #         session.add_all(mappings)
                #         session.commit()

            except Exception as db_err:
                return db_err
            
        except Exception as db_err:
            return db_err

        return code_mappings

#  ________  ________  ___  ___           ___    ___      _______      ___    ___ _________  ________  ________  ________ _________  ________      
# |\   ___ \|\   __  \|\  \|\  \         |\  \  /  /|    |\  ___ \    |\  \  /  /|\___   ___\\   __  \|\   __  \|\   ____\\___   ___\\   ____\     
# \ \  \_|\ \ \  \|\  \ \  \ \  \        \ \  \/  / /    \ \   __/|   \ \  \/  / ||___ \  \_\ \  \|\  \ \  \|\  \ \  \___\|___ \  \_\ \  \___|_    
#  \ \  \ \\ \ \   __  \ \  \ \  \        \ \    / /      \ \  \_|/__  \ \    / /     \ \  \ \ \   _  _\ \   __  \ \  \       \ \  \ \ \_____  \   
#   \ \  \_\\ \ \  \ \  \ \  \ \  \____    \/  /  /        \ \  \_|\ \  /     \/       \ \  \ \ \  \\  \\ \  \ \  \ \  \____   \ \  \ \|____|\  \  
#    \ \_______\ \__\ \__\ \__\ \_______\__/  / /           \ \_______\/  /\   \        \ \__\ \ \__\\ _\\ \__\ \__\ \_______\  \ \__\  ____\_\  \ 
#     \|_______|\|__|\|__|\|__|\|_______|\___/ /             \|_______/__/ /\ __\        \|__|  \|__|\|__|\|__|\|__|\|_______|   \|__| |\_________\
#                                       \|___|/                       |__|/ \|__|                                                      \|_________|
#  
#   A module to orchestration daily Extracts.

import json 
import os
from datetime import datetime

from .extract_historical_data import MFHistoricalActuals
from .metadata import MFMetaData
from .base import BaseExtract 
from .check import check_results, _remove_errors_from_load

from utilities import RequestMixin, MPTask, DateTimeMixin
from typing import List, Dict, Any

from logger import get_logger 

log = get_logger("Daily")

class MFDaily(BaseExtract, MFMetaData, DateTimeMixin):
    '''
        Interface Class which is used to Handle Daily Mutual Fund Extracts.
    '''
    def __init__(self, run_time_config_file: str, search_root: str = None):

        if os.path.isfile(run_time_config_file):
            self.config_path = run_time_config_file
        else:
            filename = os.path.basename(run_time_config_file)
            base = search_root or os.getcwd()
            found_path = None

            for root, _ , files in os.walk(base):
                if filename in files:
                    found_path = os.path.join(root, filename)
                    break

            if found_path:
                self.config_path = found_path
                log.warning(f"Warning: '{run_time_config_file}' not found, using '{found_path}' instead.")
            else:
 
                raise FileNotFoundError(
                    f"Config file '{run_time_config_file}' not found. "
                    f"Searched under '{base}' but no '{filename}' was found."
                )

        self.config = self.config_path
        
    def open_file_get_contents(
            self,
            run_time_config_file : str
        ):
        '''
            Open and get contents of the specified run-time configuration
        '''
        with open(run_time_config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
        return data 
    
    def update_file_contents(
        self,
        updates: Dict[str, Any],
        *,
        indent: int = 2
    ) -> None:
        """
            Open and update contents of the specified run-time configuration.
            If writing the JSON fails, dump the `updates` dict to a temp/.txt file.
        """
        data = self.open_file_get_contents(self.config_path)
        data.update(updates)
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent)
                f.write('\n')

        except Exception as exc:
            base_dir = os.path.dirname(self.config_path) or '.'
            temp_dir = os.path.join(base_dir, 'temp')
            os.makedirs(temp_dir, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%dT%H%M%SZ')
            dump_path = os.path.join(temp_dir, f'updates_{timestamp}.txt')

            with open(dump_path, 'w', encoding='utf-8') as tf:
                tf.write(json.dumps(updates, indent=indent))
                tf.write('\n')
            raise

    @staticmethod
    def list_difference(
        list1: List, 
        list2: List
    ):
        '''
            A helper to Compare to List
        '''
        return [item for item in list1 if item not in list2]
    
    def check_for_new_scheme(self):
        '''
            Call Api and check runtime config for new Scheme Codes Availabe.
            hits endpoint : "https://api.mfapi.in/mf" to get all metadata
        '''

        all_scheme_codes: List = self.get_all_scheme_codes()
        current_codes : List = [
            int(item) 
            for item in list(
                self.open_file_get_contents(self.config_path).keys()
            )
        ]
        if diff := self.list_difference(all_scheme_codes,current_codes):
            return diff
        else:
            return []

    def get_data_after(self,
        json_obj: Dict[str, Any],
        cutoff_date: str
    ) -> List[Dict[str, str]]:
        
        '''
            Serialize and get data After a certian Cutoff from API if not latest and do a full comparision for the same.
        '''
        
        cutoff = self.serialize(cutoff_date) # single dispatch into datetime.date for comparision
        filtered =  [
            entry
            for entry in json_obj.get('data', [])
            if self.serialize(entry['date']) > cutoff # single dispatch into datetime.date for comparision
        ]
        new_json : Dict = dict(json_obj)
        new_json["data"] = filtered # New Filtered Stuff for you! 

        return new_json

    def extract_daily(
        self, 
        search_for_new_schemes : bool = True, 
        threshold : int = 30
    ):
        '''
            The Daily Driver.
            if 'search_for_new_schemes' is True then look for new Mutual Funds and Load.
            else Start Daily Extracts.
        '''

        if search_for_new_schemes:
            if new_scheme := self.check_for_new_scheme():

                results = MFHistoricalActuals().Extract_All_Data(scheme_codes=new_scheme)

                check_results(results) # Check this Actuals and Raise Error : NO

                updates = {}
                for key, value in results.items():
                    updates[key] = value.get('date')
                self.update_file_contents(updates)

        else:
            scheme_data = self.open_file_get_contents(self.config_path)
            
            required_schemes = {

                scheme_code : self.day_gap(latest_update_date)
                    for scheme_code, latest_update_date in scheme_data.items() 
                    if self.day_gap(latest_update_date) <= threshold
            }  # Get Required Schemes based on threshold.

            error_flag = []

            tasks: List[MPTask] = [
                MPTask(
                    base_url=self.BASE_URL, 
                    scheme_code=code, 
                    latest=latest_flags
                )
                for code, latest_flags in 
                    {
                        intermediate : day == 1
                        for intermediate, day in required_schemes.items()
                    }.items()
            ] # Prepare Tasks to Execute in Workers

            results = self.Extract_Tasks(
                tasks=tasks,
                type_of_worker = RequestMixin._mp_worker
            )

            log.separator()
            log.alert('Starting Result Checking')

            if errors := check_results(results):
                error_flag.append(errors)
                results = _remove_errors_from_load(results)   # Remove Errored Results from Results 

            ready_to_submit = []


            for key,value in results.items():
                _data = self.get_data_after(value,scheme_data[key])
                if _data_key := _data.get('data'):
                    ready_to_submit.append(self.get_data_after(value,scheme_data[key]))
                else:
                    log.warning(f'Empty Data for : {key}')
            
            log.separator()

            log.separator()
            if ready_to_submit:
                results = self.Dump_Tasks(ready_to_submit)

                for uuid, items in results.items():
                    log.info(f'Task {uuid} Processed {len(items)} records')
                    
                if errors := check_results(results):
                    error_flag.append(errors)
                    results = _remove_errors_from_load(results)
                
                merged = {}
                for inner in results.values():
                    merged.update(inner)

                self.update_file_contents(
                    {
                        str(key):value for key,value in merged.items()
                    }
                )
                print(error_flag)
                if error_flag:
                    raise Exception('Data Extraction Failed')
                else:
                    log.success('Data Extraction Passed')
            else:
                log.alert('No Updated Data Found')
            
            log.separator()



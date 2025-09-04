
#    *                        
#  (  `        *   )   (      
#  )\))(  (  ` )  /(   )\     
# ((_)()\ )\  ( )(_)|(((_)(   
# (_()((_|(_)(_(_()) )\ _ )\  
# |  \/  | __|_   _| (_)_\(_) 
# | |\/| | _|  | |    / _ \   
# |_|  |_|___| |_|   /_/ \_\  
#
# Handle metadata......

import os
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict
from utilities import RequestMixin, MPTask
from logger import get_logger

log = get_logger('MFMetaData')

class MFMetaData(RequestMixin):

    def get_all_metadata(self):
        '''
            Request all the metadata and returns a dictonary containing all MF information.
        '''
        return self.hit_api_mf()

    def filter_available_isn(self):
        '''
            Request all the metadata and returns a dictonary containing all MF information which has an Valid ISN Code.
        '''        
        return [
            doc
            for doc in self.get_all_metadata()
            if doc.get("isinGrowth") or doc.get("isinDivReinvestment")
        ]
    
    def get_all_scheme_codes(self):
        '''
            Request all scheme codes for Mutual Funds.
        '''
        return [
            code.get("schemeCode")
            for code in self.get_all_metadata()
            if code.get("schemeCode")
        ]

    def fetch_multiple_multiprocess(
            self,
            scheme_codes,
            latest: bool = False,
            max_workers: int  = None,
        ) -> Dict[str, object]:
        """
            Fetch metadata for each scheme_code in parallel processes,

            Returns a dict mapping scheme_code → (JSON data or Exception).
        """
   

        results: Dict[str, object] = {}
        tasks : List = [MPTask(self.BASE_URL, code, latest) for code in scheme_codes]
        total : int  = len(tasks)
        completed : int = 0

        log.separator()
        log.start(f'Start Processing of {len(tasks)} in Database.\n') 

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_code = {
                executor.submit(RequestMixin._mp_worker, task): task.scheme_code
                for task in tasks
            }

            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    _, payload = future.result()
                    results[code] = payload
                except Exception as exc:
                    results[code] = exc

                completed += 1

                if os.environ.get('SHOW_PROGRESS'):
                    log.progress(completed, total, "Processing", show_bar=True)
                else:
                    log.debug(f"\rCompleted {completed}/{total}")

            if os.environ.get('SHOW_PROGRESS'):
                log.progress_finish(f'Completed Processing of {len(tasks)} in Database')
        
        log.separator()

        return results

    def prepare_run_time_config(self, file_name : str, max_workers : int = 6):
        from datetime import datetime

        codes : List = self.get_all_scheme_codes()
        run_time_config : Dict[str, object, Exception] = {}

        mp_results = self.fetch_multiple_multiprocess(codes, latest=True, max_workers = max_workers)

        for code, payload in mp_results.items():
            if isinstance(payload, Exception):
                print(f"Error for {code}: {payload}")
            else:
                date_year = datetime.strptime(payload.get('data')[0].get('date'), '%d-%m-%Y').year
  
                if int(date_year) in (2025, 2024):
                    run_time_config[payload.get('meta').get('scheme_code')] = payload.get('data')[0].get('date')

        with open(file_name,'w') as f:
            f.write(json.dumps(run_time_config, indent= 2))
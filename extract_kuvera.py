

from .metadata import MFMetaData
from .base import BaseExtract
from .check import check_results, check_results_kuvera, _remove_errors_from_load_kuvera

from utilities.api import KuveraTask, RequestMixin
from logger import get_logger
log = get_logger('KuveraPortfolioInformation')

class KuveraPortfolioInformation(MFMetaData, RequestMixin, BaseExtract):

    def start_extract_kuvera(
        self,
        types : str = 'isinDivReinvestment' 
    ): 
        '''
            Hit Kuvera api to search type as :
            1. isinDivReinvestment : Hits and searches isn availabe in mf api where there is a valid isn
            2. isinGrowth : Hits and searches isn availabe in mf api where there is a valid isn
        '''
        error_flag = []
        log.separator()
        task_to_submit = [
            KuveraTask(
                base_url=self.KUVERA_BASE_URL,
                scheme_code = item.get('schemeCode'),
                isn = item.get(types),
                type_code= types
            )
            for item in self.filter_available_isn()
            if item.get(types) 
        ]
        task_to_submit = [
            item for item in task_to_submit
            if item.isn 
        ]

        log.start(f'Submit Processing of {len(task_to_submit)} in Executor.\n')   
        log.separator()

        results =  self.Extract_Tasks(
            tasks=task_to_submit,
            type_of_worker= RequestMixin._mp_worker_kuvera
        )

        if errors := check_results_kuvera(results):
            error_flag.append(errors)
            results = _remove_errors_from_load_kuvera(results)   # Remove Errored Results from Results 
        
        ready_to_submit = [
            item
            for code, item in results.items()
        ]
        if ready_to_submit:
            log.separator()
            log.start(f'Submit Objects {len(ready_to_submit)} to Database')
            results = self.Dump_Tasks(
                ready_to_submit,
                type_of_worker=RequestMixin._mp_worker_db_dump_kuvera
            )

            if errors := check_results(results):
                error_flag.append(errors)

            for uuid, items in results.items():
                log.info(f'Task {uuid} Processed {items} records')

            if error_flag:
                log.separator()
                log.critical('Data Extraction Failed')
                log.separator()

        else:
            log.alert('No Valid Data to Submit in Database')





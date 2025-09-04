
# .---.  .---..-./`)    .-'''-.,---------.    ,-----.    .-------.      ____     __  
# |   |  |_ _|\ .-.')  / _     \          \ .'  .-,  '.  |  _ _   \     \   \   /  / 
# |   |  ( ' )/ `-' \ (`' )/`--'`--.  ,---'/ ,-.|  \ _ \ | ( ' )  |      \  _. /  '  
# |   '-(_{;}_)`-'`"`(_ o _).      |   \  ;  \  '_ /  | :|(_ o _) /       _( )_ .'   
# |      (_,_) .---.  (_,_). '.    :_ _:  |  _`,/ \ _/  || (_,_).' __ ___(_ o _)'    
# | _ _--.   | |   | .---.  \  :   (_I_)  : (  '\_/ \   ;|  |\ \  |  |   |(_,_)'     
# |( ' ) |   | |   | \    `-'  |  (_(=)_)  \ `"/  \  ) / |  | \ `'   /   `-'  /      
# (_{;}_)|   | |   |  \       /    (_I_)    '. \_/``".'  |  |  \    / \      /       
# '(_,_) '---' '---'   `-...-'     '---'      '-----'    ''-'   `'-'   `-..-'        
                                                                                   
from typing import List

from .metadata import MFMetaData
from .base import BaseExtract

from logger import get_logger


log = get_logger('MFHistoricalActual')

class MFHistoricalActuals(MFMetaData, BaseExtract):
    '''
        Historical Data Extraction for all Scheme Codes / Specified Codes.
    '''
    
    def Extract_All_Data(self, scheme_codes : List = [], latest_flags : bool = False):
        '''
            Start Extrating All the Data for Scheme Codes provided.
        '''
        log.separator()
        log.alert('Running historical Extracts\n')
        log.info('attempting to extract all the Historical Data and Attepting to load into database')
        log.separator()
        return super().Extract_All_Data(scheme_codes, latest_flags)
    



#  ________  ___  ___  _______   ________  ___  __    ________      
# |\   ____\|\  \|\  \|\  ___ \ |\   ____\|\  \|\  \ |\   ____\     
# \ \  \___|\ \  \\\  \ \   __/|\ \  \___|\ \  \/  /|\ \  \___|_    
#  \ \  \    \ \   __  \ \  \_|/_\ \  \    \ \   ___  \ \_____  \   
#   \ \  \____\ \  \ \  \ \  \_|\ \ \  \____\ \  \\ \  \|____|\  \  
#    \ \_______\ \__\ \__\ \_______\ \_______\ \__\\ \__\____\_\  \ 
#     \|_______|\|__|\|__|\|_______|\|_______|\|__| \|__|\_________\
#                                                       \|_________|
#
# The module to check Data.

from typing import Dict
from logger import get_logger

log = get_logger('Checks')

def check_results(results : Dict, raise_error : bool = False):
    '''
        Take in the List and print the errors, raise if given.
    '''
    if errors := {
        code : execp for code, execp in results.items()
        if isinstance(execp,Exception)
    }:
        for key, value in errors.items():
            log.error(f"Task Failed {key} : Due to \n{str(value)}")
        
        if raise_error:
            log.alert('Data Extraction Failed')
            raise Exception('Data Extraction Failed')

        return errors
    else:
        log.info('Error Checks passed for Daily Extracts.')

def _remove_errors_from_load(results : Dict):
    '''
        The Execp Filter.
    '''
    return {
        code : execp for code, execp in results.items()
        if not isinstance(execp,Exception)
    }

def check_results_kuvera(results : Dict, raise_error : bool = False):
    '''
        Take in the List and print the errors, raise if given.
    '''
    if errors := {
        code : execp for code, execp in results.items()
        if isinstance(execp,Exception)
    }:
        for key, value in errors.items():
            log.error(f"Task Failed {key} : Due to \n{str(value)}")
        
        if raise_error:
            log.alert('Data Extraction Failed')
            raise Exception('Data Extraction Failed')

        return errors
    else:
        log.info('Error Checks passed for Daily Extracts.')

def _remove_errors_from_load_kuvera(results : Dict):
    '''
        The Execp Filter.
    '''
    return {
        code : execp for code, execp in results.items()
        if not isinstance(execp,int) or not isinstance(execp,Exception)
    }
import argparse
import os
from dotenv import load_dotenv

from extractions import MFDaily, MFHistoricalActuals, check_results, KuveraPortfolioInformation
from extractions.metadata import MFMetaData

from models.base import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.router import get_engine

from logger import get_logger

load_dotenv()
log = get_logger(__file__)

def run_daily(config_path: str, search_new: bool):
    """
        Run daily extraction.
    """
    data = MFDaily(config_path)
    result = data.extract_daily(search_for_new_schemes=search_new)
    if result is None:
        log.alert('No Data Found For Above Selection')
    else:
        log.success('Data Extraction Sucessfull')


def run_historical():
    """
        Run historical actuals extraction and check results.
    """
    data = MFHistoricalActuals()
    result = data.Extract_All_Data()
    check_results(result)


def run_metadata(config_path: str):
    """
        Prepare runtime config for metadata.
    """
    client = MFMetaData()
    client.prepare_run_time_config(config_path)


def run_create_db():
    """
        Create database tables based on SQLAlchemy models.
    """
    engine = get_engine(db_type=os.environ.get('DATABASE_TYPE'), echo=True)
    Session = sessionmaker(bind=engine) # test if session can be made
    log.separator()
    log.start('Creating Database')
    Base.metadata.create_all(engine)
    log.separator()


def run_kuvera(operation: str):
    """
        Run Kuvera portfolio information extraction.
    """
    x = KuveraPortfolioInformation()
    x.start_extract_kuvera(operation)


def main():
    parser = argparse.ArgumentParser(
        description='Command-line tool for mutual fund data operations and database management.'
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    daily_parser = subparsers.add_parser('daily', help='Extract daily data')
    daily_parser.add_argument(
        '--config', '-c',
        default='run_time_config.json',
        help='Path to the runtime config JSON file'
    )
    daily_parser.add_argument(
        '--search',
        action='store_true',
        dest='search_for_new_schemes',
        help='Search for new schemes in daily extraction'
    )

    hist_parser = subparsers.add_parser('historical', help='Extract historical actuals')

    meta_parser = subparsers.add_parser('metadata', help='Prepare runtime config for metadata')
    meta_parser.add_argument(
        '--config', '-c',
        default='data.json',
        help='Path to the metadata config JSON file'
    )

    db_parser = subparsers.add_parser('create-db', help='Create database tables')

    kuvera_parser = subparsers.add_parser('kuvera', help='Extract Kuvera portfolio information')
    kuvera_parser.add_argument(
        'operation',
        help='Mode of operation for Kuvera extraction (e.g., isinDivReinvestment)'
    )

    args = parser.parse_args()

    if args.command == 'daily':
        run_daily(args.config, args.search_for_new_schemes)
    elif args.command == 'historical':
        run_historical()
    elif args.command == 'metadata':
        run_metadata(args.config)
    elif args.command == 'create-db':
        run_create_db()
    elif args.command == 'kuvera':
        run_kuvera(args.operation)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()

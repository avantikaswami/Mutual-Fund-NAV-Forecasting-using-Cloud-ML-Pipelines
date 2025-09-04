from models.base import KuveraPotfolioInformation
from datetime import datetime
from typing import Dict

JSON_FIELD_MAP : Dict = {
 'scheme_code': 'scheme_code',
 'isn_code': 'isin',
 'type_code': 'type_code',
 'code': 'code',
 'name': 'name',
 'short_name': 'short_name',
 'lump_available': 'lump_available',
 'sip_available': 'sip_available',
 'lump_min': 'lump_min',
 'lump_min_additional': 'lump_min_additional',
 'lump_max': 'lump_max',
 'lump_multiplier': 'lump_multiplier',
 'sip_min': 'sip_min',
 'sip_max': 'sip_max',
 'sip_multiplier': 'sip_multiplier',
 'sip_maximum_gap': 'sip_maximum_gap',
 'redemption_allowed': 'redemption_allowed',
 'redemption_amount_multiple': 'redemption_amount_multiple',
 'redemption_amount_minimum': 'redemption_amount_minimum',
 'redemption_quantity_multiple': 'redemption_quantity_multiple',
 'redemption_quantity_minimum': 'redemption_quantity_minimum',
 'category': 'category',
 'lock_in_period': 'lock_in_period',
 'fund_house': 'fund_house',
 'fund_name': 'fund_name',
 'short_code': 'short_code',
 'detail_info': 'detail_info',
 'isin': 'isin',
 'direct': 'direct',
 'switch_allowed': 'switch_allowed',
 'stp_flag': 'stp_flag',
 'swp_flag': 'swp_flag',
 'instant': 'instant',
 'reinvestment': 'reinvestment',
 'tags': 'tags',
 'slug': 'slug',
 'channel_partner_code': 'channel_partner_code',
 'tax_period': 'tax_period',
 'insta_redeem_min_amount': 'insta_redeem_min_amount',
 'insta_redeem_max_amount': 'insta_redeem_max_amount',
 'small_screen_name': 'small_screen_name',
 'nav': 'nav',
 'last_nav': 'last_nav',
 'volatility': 'volatility',
 'comparison': 'comparison',
 'returns.week_1': 'returns_week_1',
 'returns.year_1': 'returns_year_1',
 'returns.year_3': 'returns_year_3',
 'returns.year_5': 'returns_year_5',
 'returns.inception': 'returns_inception',
 'returns.date': 'returns_date',
 'start_date': 'start_date',
 'face_value': 'face_value',
 'fund_type': 'fund_type',
 'fund_category': 'fund_category',
 'plan': 'plan',
 'expense_ratio': 'expense_ratio',
 'expense_ratio_date': 'expense_ratio_date',
 'fund_manager': 'fund_manager',
 'crisil_rating': 'crisil_rating',
 'investment_objective': 'investment_objective',
 'portfolio_turnover': 'portfolio_turnover',
 'maturity_type': 'maturity_type',
 'aum': 'aum',
}

def create_from_json(data: Dict) -> KuveraPotfolioInformation:
    kwargs = {}
    for json_key, attr in JSON_FIELD_MAP.items():
        if '.' not in json_key:
            if json_key in data:
                kwargs[attr] = data[json_key]

    # kwargs['isn_code'] = data['isn']

    if 'returns' in data:
        ret = data['returns']
        for sub, attr in [
            ('week_1','returns_week_1'),
            ('year_1','returns_year_1'),
            ('year_3','returns_year_3'),
            ('year_5','returns_year_5'),
            ('inception','returns_inception'),
            ('date','returns_date'),
        ]:
            if sub in ret:
                kwargs[attr] = ret[sub]

    if 'start_date' in kwargs and isinstance(kwargs['start_date'], str):
        kwargs['start_date'] = datetime.fromisoformat(kwargs['start_date']).date()
    if 'expense_ratio_date' in kwargs and isinstance(kwargs['expense_ratio_date'], str):
        kwargs['expense_ratio_date'] = datetime.fromisoformat(kwargs['expense_ratio_date']).date()
    if 'returns_date' in kwargs and isinstance(kwargs['returns_date'], str):
        kwargs['returns_date'] = datetime.fromisoformat(kwargs['returns_date']).date()
    kwargs['insert_date'] = datetime.now()

    # return KuveraPotfolioInformation(**kwargs)
    return kwargs
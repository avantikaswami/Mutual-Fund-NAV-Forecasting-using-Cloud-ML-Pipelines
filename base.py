#     O))))     O)))))))    O))       O))
#   O))    O))  O))    O))  O) O))   O)))
# O))        O))O))    O))  O)) O)) O O))
# O))        O))O) O))      O))  O))  O))
# O))        O))O))  O))    O))   O)  O))
#   O))     O)) O))    O))  O))       O))
#     O))))     O))      O))O))       O))


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    Date,
    Numeric,
    ForeignKey,
    String,
    Float,
    JSON,
    DateTime,
    Text
)

Base = declarative_base()


class MutualFundScheme(Base):
    __tablename__ = "mutual_fund_schemes"

    scheme_code = Column(Integer, primary_key=True, comment="Unique scheme code")
    fund_house = Column(String(400), nullable=False, comment="Name of the fund house")
    scheme_type = Column(String(400), nullable=False, comment="Type of the scheme")
    scheme_category = Column(
        String(400), nullable=False, comment="Category of the scheme"
    )
    scheme_name = Column(String(400), nullable=False, comment="Full scheme name")
    isin_growth = Column(
        String(400), nullable=True, comment="ISIN code for growth option"
    )
    isin_div_reinvestment = Column(
        String(400), nullable=True, comment="ISIN code for dividend reinvestment option"
    )

    def __repr__(self):
        return (
            f"<MutualFundScheme(scheme_code={self.scheme_code}, fund_house='{self.fund_house}', "
            f"scheme_name='{self.scheme_name}')>"
        )


class MutualFundNAV(Base):
    __tablename__ = "mutual_fund_nav"

    id = Column(Integer, primary_key=True, autoincrement=True)
    insert_date = Column(DateTime, nullable=True)
    scheme_code = Column(
        Integer, ForeignKey("mutual_fund_schemes.scheme_code"), nullable=False
    )
    date = Column(Date, nullable=False, comment="NAV date")
    nav = Column(
        Numeric(38, 5), nullable=False, comment="Net Asset Value with highest precision"
    )


class KuveraPotfolioInformation(Base):
    __tablename__ = "kuvera_potfolio_information"

    # meta data fields to connect to mutual Funds Metadata and Nav information
    id = Column(Integer, primary_key=True, autoincrement=True)
    insert_date = Column(DateTime, nullable=True)
    scheme_code = Column(String(400), nullable=False)
    isn_code = Column(String(400))
    type_code = Column(String(400), nullable=False)
    code = Column(String(400), nullable=False)
    name = Column(String(400), nullable=True)
    short_name = Column(String(400), nullable=True)
    lump_available = Column(String(400), nullable=True)
    sip_available = Column(String(400), nullable=True)
    lump_min = Column(Numeric(38, 5), nullable=True)
    lump_min_additional = Column(Numeric(38, 5), nullable=True)
    lump_max = Column(Numeric(38, 5), nullable=True)
    lump_multiplier = Column(Numeric(38, 5), nullable=True)
    sip_min = Column(Numeric(38, 5), nullable=True)
    sip_max = Column(Numeric(38, 5), nullable=True)
    sip_multiplier = Column(Numeric(38, 5), nullable=True)
    redemption_allowed = Column(String(400), nullable=True)
    redemption_amount_multiple = Column(Float, nullable=True)
    redemption_amount_minimum = Column(Float, nullable=True)
    redemption_quantity_multiple = Column(Float, nullable=True)
    redemption_quantity_minimum = Column(Float, nullable=True)
    category = Column(String(400), nullable=True)
    lock_in_period = Column(Integer, nullable=True)
    sip_maximum_gap = Column(Integer, nullable=True)
    fund_house = Column(String(400), nullable=True)
    fund_name = Column(String(400), nullable=True)
    short_code = Column(String(400), nullable=True)
    detail_info = Column(String(400), nullable=True)
    isin = Column(String(400), nullable=True)
    direct = Column(String(400), nullable=True)
    switch_allowed = Column(String(400), nullable=True)
    stp_flag = Column(String(400), nullable=True)
    swp_flag = Column(String(400), nullable=True)
    instant = Column(String(400), nullable=True)
    reinvestment = Column(String(400), nullable=True)
    tags = Column(JSON, nullable=True)
    slug = Column(String(400), nullable=True)
    channel_partner_code = Column(String(400), nullable=True)
    tax_period = Column(Integer, nullable=True)
    insta_redeem_min_amount = Column(Numeric(38, 5), nullable=True)
    insta_redeem_max_amount = Column(Numeric(38, 5), nullable=True)
    small_screen_name = Column(String(400), nullable=True)
    nav = Column(JSON, nullable=True)
    last_nav = Column(JSON, nullable=True)
    volatility = Column(Float, nullable=True)
    returns = Column(JSON, nullable=True)
    returns_week_1 = Column(String(400), nullable=True)
    returns_year_1 = Column(String(400), nullable=True)
    returns_year_3 = Column(String(400), nullable=True)
    returns_year_5 = Column(String(400), nullable=True)
    returns_inception = Column(String(400), nullable=True)
    returns_date = Column(String(400), nullable=True)
    start_date = Column(Date, nullable=True)
    face_value = Column(Float, nullable=True)
    fund_type = Column(String(400), nullable=True)
    fund_category = Column(String(400), nullable=True)
    plan = Column(String(400), nullable=True)
    expense_ratio = Column(Float, nullable=True)
    expense_ratio_date = Column(Date, nullable=True)
    fund_manager = Column(String(400), nullable=True)
    crisil_rating = Column(String(400), nullable=True)
    investment_objective = Column(Text, nullable=True)
    portfolio_turnover = Column(Float, nullable=True)
    maturity_type = Column(String(400), nullable=True)
    aum = Column(Float, nullable=True)
    comparison = Column(JSON, nullable=True)

    def __repr__(self):
        return (
            f"<KuveraPotfolioInformation(code={self.code!r}, "
            f"name={self.name!r}, nav={self.nav})>"
        )

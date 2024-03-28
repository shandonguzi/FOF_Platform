'''this module updates level3 data'''

from level3.active_share import active_share
from level3.bond_raw_factors import bond_raw_factors
from level3.capm_α import capm_α
from level3.capm_α_ε import capm_α_ε
from level3.connected_companies_portfolio_capm import connected_companies_portfolio_capm
from level3.connected_companies_portfolio_svc import connected_companies_portfolio_svc
from level3.industry_concentration import industry_concentration
from level3.ME import ME
from level3.money_fund_factor import money_fund_factor
from level3.return_gap import return_gap
from level3.svc_α import svc_α
from level3.svc_α_ε import svc_α_ε
from level3.svc_α_truncated import svc_α_truncated
from level3.TD_α import TD_α
from level3.TDP_α import TDP_α
from level3.z_score import z_score
from level3.Value.A2ME import A2ME
from level3.Value.BEME import BEME
from level3.Value.C import C
from level3.Value.CF import CF
from level3.Value.CF2P import CF2P
from level3.Value.E2P import E2P
from level3.Value.Lev import Lev
from level3.Value.Q import Q
from level3.Value.S2P import S2P
from level3.TradingFrictions.AT import AT
from level3.TradingFrictions.Beta import CAPM_beta
from level3.TradingFrictions.IdioVol import IdioVol
from level3.TradingFrictions.LME import LME
from level3.TradingFrictions.LTurnover import LTurnover
from level3.TradingFrictions.MktBeta import MktBeta
from level3.TradingFrictions.Rel2High import Rel2High
from level3.TradingFrictions.ResidVar import ResidVar
from level3.stock_return.LT_Rev import LT_Rev
from level3.stock_return.r2_1 import r2_1
from level3.stock_return.r12_2 import r12_2
from level3.stock_return.r12_7 import r12_7
from level3.stock_return.r36_13 import r36_13
from level3.stock_return.ST_Rev import ST_Rev
from level3.Profitability.ATO import ATO
from level3.Profitability.CTO import CTO
from level3.Profitability.D2A import D2A
from level3.Profitability.FC2Y import FC2Y
from level3.Profitability.OP import OP
from level3.Profitability.PM import PM
from level3.Profitability.PROF import PROF
from level3.Profitability.RNA import RNA
from level3.Profitability.ROA import ROA
from level3.Profitability.ROE import ROE
from level3.Profitability.SGA2S import SGA2S
from level3.Investment.DPI2A import DPI2A
from level3.Investment.Investment import Investment
from level3.Investment.NOA import NOA
from level3.Investment.NSI import NSI
from level3.Intangibles.AC import AC
from level3.Intangibles.OL import OL
from level3.Intangibles.PCM import PCM
from level3.FundMomentum.F_r12_2 import F_r12_2
from level3.FundMomentum.F_ST_Rev import F_ST_Rev
from level3.FundCharacters.Fund_age import Fund_age
from level3.FundCharacters.Fund_tna import Fund_TNA
from level3.Stock_to_Fund import Stock_to_Fund
from utils.time_function import timeit
from level3.ipca_score import ipca_score
from level3.MacroFactors.CreditFactor import CreditFactor
from level3.MacroFactors.ExchangeRateFactor import ExchangeRateFactor
from level3.MacroFactors.HighFreqEcoGrowth import HighFreqEcoGrowth
from level3.MacroFactors.HighFreqInflationConsumer import HighFreqInflationConsumer
from level3.MacroFactors.HighFreqInflationProductor import HighFreqInflationProductor
from level3.MacroFactors.InterestRateFactor import InterestRateFactor
from level3.MacroFactors.TermSpreadFactor import TermSpreadFactor
from level3.MacroFactors.lowfreq_EcoGrowth import lowfreq_EcoGrowth
from level3.MacroFactors.lowfreq_InfConsumer import lowfreq_InfConsumer
from level3.MacroFactors.lowfreq_InfProductor import lowfreq_InfProductor


def basic():
    bond_raw_factors() # monthly
    money_fund_factor() # monthly
    capm_α() # monthly
    capm_α_ε() # monthly
    svc_α() # monthly
    svc_α_ε() # monthly
    TD_α() # monthly
    TDP_α() # monthly

    connected_companies_portfolio_capm() # quarterly
    connected_companies_portfolio_svc() # quarterly
    industry_concentration() # quarterly
    ME() # quarterly
    active_share() # quarterly
    return_gap() # quarterly

def advanced():
    svc_α_truncated() # monthly
    z_score() # monthly

def fundamental_stock_factors():
    A2ME()
    BEME()
    C()
    CF()
    CF2P()
    E2P()
    Lev()
    Q()
    S2P()
    AT()
    CAPM_beta()
    IdioVol()
    LME()
    LTurnover()
    MktBeta()
    Rel2High()
    ResidVar()
    LT_Rev()
    r2_1()
    r12_2()
    r12_7()
    r36_13()
    ST_Rev()
    ATO()
    CTO()
    D2A()
    FC2Y()
    OP()
    PM()
    PROF()
    RNA()
    ROA()
    ROE()
    SGA2S()
    DPI2A()
    Investment()
    NOA()
    NSI()
    AC()
    OL()
    PCM()

def fundamental_fund_factors():
    F_r12_2()
    F_ST_Rev()
    Fund_age()
    Fund_TNA()

def MacroFactors():
    lowfreq_EcoGrowth()
    lowfreq_InfConsumer()
    lowfreq_InfProductor()
    CreditFactor()
    ExchangeRateFactor()
    HighFreqEcoGrowth()
    HighFreqInflationConsumer()
    HighFreqInflationProductor()
    InterestRateFactor()
    TermSpreadFactor()


@timeit('Level 3')
def level3():
    basic()
    advanced()
    MacroFactors()
    # fundamental_stock_factors() # monthly
    # Stock_to_Fund() # monthly
    # fundamental_fund_factors() # monthly
    # ipca_score() # monthly

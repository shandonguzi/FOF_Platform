
import os

from level0.csmar.factor.BF_IHEAD import BF_IHEAD
from level0.csmar.financial_report.FS_Combas import FS_Combas
from level0.csmar.financial_report.FS_Comins import FS_Comins
from level0.csmar.financial_report.FS_Comscfd import FS_Comscfd
from level0.csmar.financial_report.FS_Comscfi import FS_Comscfi
from level0.csmar.fund.Fund_Allocation import Fund_Allocation
from level0.csmar.fund.Fund_FeesChange import Fund_FeesChange
from level0.csmar.fund.FUND_FundCodeInfo import FUND_FundCodeInfo
from level0.csmar.fund.Fund_FundDividend import Fund_FundDividend
from level0.csmar.fund.FUND_MainInfo import FUND_MainInfo
from level0.csmar.fund.Fund_NAV import Fund_NAV
from level0.csmar.fund.Fund_Portfolio_Stock import Fund_Portfolio_Stock
from level0.csmar.fund.Fund_Ptf_BondSpe import Fund_Ptf_BondSpe
from level0.csmar.fund.FUND_PurchRedChg import FUND_PurchRedChg
from level0.csmar.fund.Fund_Resolution import Fund_Resolution
from level0.csmar.fund.Fund_ShareChange import Fund_ShareChange
from level0.csmar.index.IDX_Idxtrd import IDX_Idxtrd
from level0.csmar.index.IDX_Smprat import IDX_Smprat
from level0.csmar.stock.TRD_Co import TRD_Co
from level0.csmar.stock.TRD_Dalyr import TRD_Dalyr
from level0.csmar.stock.TRD_Mnth import TRD_Mnth
from level0.jiayin.CH3 import CH3
from level0.csmar.stock.FF3 import FF3
from level0.csmar.bond.risk_free import BND_Exchange
from level0.joinquant.Fund_MainInfo import JQ_Fund_MainInfo
from level0.joinquant.Stock_SWL1_Clsf import JQ_Stock_SWL1_Clsf
from level0.joinquant.Stock_CSRC_Clsf import JQ_Stock_CSRC_Clsf
from utils.time_function import timeit


def daily():

    Fund_FeesChange()
    FUND_FundCodeInfo() # overwrite
    Fund_FundDividend()
    FUND_MainInfo()
    Fund_NAV()
    FUND_PurchRedChg()
    Fund_Resolution()
    IDX_Idxtrd()
    IDX_Smprat()
    TRD_Co()
    TRD_Dalyr()
    JQ_Fund_MainInfo() # overwrite
    JQ_Stock_SWL1_Clsf() # overwrite
    JQ_Stock_CSRC_Clsf() # overwrite
    FF3() # overwrite
    BND_Exchange() # overwrite
    CH3() # overwrite
    BF_IHEAD()

def monthly():
    TRD_Mnth()

def quarterly():
    Fund_Allocation()
    Fund_Portfolio_Stock()
    Fund_Ptf_BondSpe()
    Fund_ShareChange()
    FS_Combas()
    FS_Comins()
    FS_Comscfd()
    FS_Comscfi()

@timeit('Level 0')
def level0():
    os.system('rm -rf /root/Downloads/*')
    daily()
    monthly()
    quarterly()

'''this module updates level2 data'''

from level1.Fund_Allocation import Fund_Allocation
from level1.Fund_FeesChange import Fund_FeesChange
from level1.Fund_FundCodeInfo import Fund_FundCodeInfo
from level1.Fund_FundDividend import Fund_FundDividend
from level1.Fund_MainInfo import Fund_MainInfo
from level1.Fund_NAV import Fund_NAV
from level1.Fund_Portfolio_Stock import Fund_Portfolio_Stock
from level1.Fund_Ptf_BondSpe import Fund_Ptf_BondSpe
from level1.Fund_PurchRedChg import Fund_PurchRedChg
from level1.Fund_Resolution import Fund_Resolution
from level1.Fund_ShareChange import Fund_ShareChange
from level1.IDX_Idxtrd import IDX_Idxtrd
from level1.IDX_Smprat import IDX_Smprat
from level1.MoneyFundReturn import MoneyFundReturn
from level1.MoneyFundReturn_m import MoneyFundReturn_m
from level1.TRD_Co import TRD_Co
from level1.TRD_Dalyr import TRD_Dalyr
from level1.TRD_Mnth import TRD_Mnth
from level1.FF3 import FF3
from level1.risk_free import BND_Exchange
from level1.TreasuryBond1D import TreasuryBond1D
from level1.TreasuryBond1M import TreasuryBond1M
from level1.BF_IHEAD import BF_IHEAD
from level1.FS_Combas import FS_Combas
from level1.FS_Comins import FS_Comins
from level1.FS_Comscfd import FS_Comscfd
from level1.FS_Comscfi import FS_Comscfi

from utils.time_function import timeit

def daily():
    
    Fund_FeesChange()
    Fund_FundCodeInfo() # no time span
    Fund_FundDividend()
    Fund_MainInfo()
    Fund_NAV()
    Fund_PurchRedChg()
    Fund_Resolution()
    IDX_Idxtrd()
    IDX_Smprat()
    MoneyFundReturn()
    MoneyFundReturn_m()
    TRD_Co()
    TRD_Dalyr()
    FF3()
    BND_Exchange()
    TreasuryBond1D()
    BF_IHEAD()


def monthly():

    TRD_Mnth()
    TreasuryBond1M()

def quarterly():

    Fund_Allocation()
    Fund_Portfolio_Stock()
    Fund_Ptf_BondSpe()
    Fund_ShareChange()
    FS_Combas()
    FS_Comins()
    FS_Comscfd()
    FS_Comscfi()

@timeit('Level 1')
def level1():
    daily()
    monthly()
    quarterly()

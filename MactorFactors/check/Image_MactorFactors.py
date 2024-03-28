import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as mtick

from settings.database import *
from utils.mysql.get_sql import get_sql
from utils.time_function import timeit


def read_sql(FactorName):

    YoY = get_sql(level3_factors, FactorName + '_YoY')
    WoW = get_sql(level3_factors, FactorName + '_WoW')
    YoY = YoY.set_index('Date')
    WoW = WoW.set_index('Date')
    Factor = pd.merge(YoY, WoW, left_index=True, right_index=True)
    Factor = Factor[Factor.index >= '2014-01-01']

    return Factor


def create_image(Factor, FactorName, ylims):

    fig, ax1 = plt.subplots(figsize=(10,6))
    ax1.set_ylim(ylims[0])
    ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax1.yaxis.tick_left()
    ax2 = ax1.twinx()
    ax2.set_ylim(ylims[1])
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax2.yaxis.tick_right()
    ax1.spines['bottom'].set_visible(False)
    ax1.spines['top'].set_visible(False)
    ax2.spines['bottom'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax1.axhline(0, color='black', linewidth=0.5)
    ax2.axhline(0, color='black', linewidth=0.5)
    ax1.plot(Factor.index.values, Factor[f'{FactorName}_YoY'].values, color='red', linewidth=2)
    ax2.plot(Factor.index.values, Factor[f'{FactorName}_WoW'].values, color='green', linewidth=1)
    plt.title(f'Image_{FactorName}')
    plt.savefig(f'/code/MactorFactors/check/Image_{FactorName}.png')
    # plt.show()


def handle():
    
    FactorName = ['InterestRateFactor', 
                  'ExchangeRateFactor',
                  'CreditFactor',
                  'TermSpreadFactor',
                  'HighFreqInflationProductor',
                  'HighFreqInflationConsumer',
                  'HighFreqEcoGrowth'
                  ]
    Image_ylims = [[(-10,10),(-5,5)],
                  [(-30,30),(-10,10)],
                  [(-10,10),(-5,5)],
                  [(-10,10),(-5,5)],
                  [(-20,20),(-5,5)],
                  [(-12,12),(-1,1)],
                  [(-15,15),(-5,5)]
                  ]
    for factor in FactorName:
        Factor = read_sql(factor)
        create_image(Factor, factor, Image_ylims[FactorName.index(factor)])


@timeit('MactorFactors')
def MactorFactors():
    handle()


if __name__ == '__main__':
    MactorFactors()
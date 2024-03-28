
import pandas as pd

from settings.database import level0_csmar


def upload_data(CHINESE_NAME, CODE, if_exists):
    '''
    multi-threaded data upload
    '''
    df = pd.read_csv(f'/data/{CHINESE_NAME}/{CODE}.csv', low_memory=False)
    df.to_sql(CODE, con=level0_csmar, index=False, if_exists=if_exists)

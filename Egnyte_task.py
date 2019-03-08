# Libraries
import dask.bag as db
import dask
import json
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import glob
import sys
pd.set_option('display.width', 200)

def load_fse_json_data():
    # Load fse data using Bag
    json_fse_data_bag = db.read_text('fse_data*.json').map(json.loads)
    # json_data_bag1 = db.read_text('1fse_data-000000000000.json').map(json.loads)
    # print(json_data_bag.take(2))
    fse_data_df = json_fse_data_bag.to_dataframe()
    # print(json_data_df.head())
    features = ['action', 'app_inferred', 'os_inferred', 'country', 'datacenter', 'event_id', 'automated_action', 'space_used', 'timestamp', 'workgroup_id']
    fse_data_df = fse_data_df[features]
    return fse_data_df

def load_domain_json_data():
    # Load domain data using Bag
    filenames = glob.glob('domain*.json')
    domain_data_df = pd.DataFrame()
    for f in filenames: domain_data_df = domain_data_df.append(pd.read_json(f, lines=True))
    # domain_data_df = pd.read_json('domain*.json', lines=True)
    # print(domain_data_df.head())
    return domain_data_df

def df_basic_statistics(df):
    # Calculate basic statistics on dataframe
    # Drop NA values
    df = df.dropna()
    #Number of rows and columns in dataframe
    #print('Number of rows: %d, number of columns: %d', (df.shape[0]).compute(), df.shape[1])
    print('Dataframe columna')
    print(df.columns)
    print('Columns with values type')
    print(df.dtypes)

def fse_actions_stat(df):
    features = ['action', 'app_inferred', 'os_inferred', 'country', 'datacenter', 'event_id', 'automated_action', 'space_used', 'timestamp', 'workgroup_id']
    if(pd.Series(features).isin(df.columns).all()):
        actions = df['action'].value_counts().compute()
        print(actions)
        plt.figure(figsize=(8, 6), dpi=80)
        plt.plot(actions, color="blue")
        plt.xlabel('The name of the action')
        plt.ylabel('The number of actions')
        plt.title('The most common actions')
        plt.grid(True)
        plt.show()
    else:
        print('Wrong dataframe !')

def fse_countries_stat(df):
    features = ['action', 'app_inferred', 'os_inferred', 'country', 'datacenter', 'event_id', 'automated_action', 'space_used', 'timestamp', 'workgroup_id']
    if(pd.Series(features).isin(df.columns).all()):
        countries = df.groupby(df.country).workgroup_id.count().nlargest(10).compute()
        print(countries)
        plt.figure(figsize=(8, 7))
        countries.plot.bar()
        plt.xlabel('The name of the country')
        plt.ylabel('The number of working group in countries')
        plt.title('Ten top countries with the largest number of working groups')
        plt.show()
    else:
        print('Wrong dataframe !')

def fse_actions_in_hours(df):
    features = ['action', 'app_inferred', 'os_inferred', 'country', 'datacenter', 'event_id', 'automated_action', 'space_used', 'timestamp', 'workgroup_id']
    if(pd.Series(features).isin(df.columns).all()):
        df['timestamp'] = df['timestamp'].map(lambda x: pd.to_datetime(x, errors='coerce'))
        actions_per_hours = df.groupby(df.timestamp.dt.hour).action.count().compute()
        print(actions_per_hours)
        plt.figure(figsize=(8, 6), dpi=80)
        plt.plot(actions_per_hours, color="red")
        plt.xlabel('Working hours')
        plt.ylabel('The number of actions')
        plt.title('The number of actions registered at particular hours')
        plt.grid(True)
        plt.show()
    else:
        print('Wrong dataframe !')   

def domain_data_advanced_stat(df):
    columns = ['account_type', 'data_center', 'domain_status', 'plan_name', 'pu_bought', 'pu_used', 'storage_bought_MB', 'storage_used_MB','subscription_date', 'trial_end_date', 'trial_start_date','workgroup_id']

    if(pd.Series(columns).isin(df.columns).all()):

        df['subscription_date'] = df['subscription_date'].map(lambda x: pd.to_datetime(x, errors='coerce'))

        # sub_month = df.groupby(df.subscription_date.dt.month).workgroup_id.count()
        # print(sub_month)
        # plt.plot(sub_month, color="red")
        # plt.xlabel('Month')
        # plt.ylabel('Number of subscriptions')
        # plt.title('Number of subscriptions in particular months')

        sub_year = df.groupby(df.subscription_date.dt.year).workgroup_id.count()
        print(sub_year)
        plt.plot(sub_year, color="orange")
        plt.xlabel('Year')
        plt.ylabel('Number of subscriptions')
        plt.title('Number of subscriptions in years')
        plt.grid(True)
        plt.show()

        domain_status = df['domain_status'].value_counts()
        print(domain_status)
        domain_status.plot.bar()
        plt.xlabel('Domain status')
        plt.ylabel('Number of observations')
        plt.title('Number of observations for each domain status group')
        plt.show()

    else:
        print('Wrong dataframe !')

def main():
    try:
        fse_df = load_fse_json_data()
        domain_df = load_domain_json_data()

        print ('Data from Egnyte Cloud Server')
        df_basic_statistics(fse_df)
        # fse_actions_stat(fse_df)
    
        print ('Data from Egnyte Connect domain')
        df_basic_statistics(domain_df)
        domain_data_advanced_stat(domain_df)

    except ImportError as error:
        print(error.__class__.__name__ + ": " + error.message)
    except Exception as exception:
        print(exception, False)
        print(exception.__class__.__name__ + ": " + exception.message)

if __name__ == '__main__':
    main()

    

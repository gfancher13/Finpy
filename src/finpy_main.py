
import pandas as pd
from polygon import RESTClient
import sqlite3
import fredapi as Fred
import requests


class OptionsClient:
    def __init__(self, api_key):
        self.client = RESTClient(api_key=api_key)
        self.utils = self.Utils()

    class Utils:
        def __init__(self):
            pass

        def options_contract_data_clean_(self, data, ticker):
            df = pd.DataFrame(data)
            df.drop(columns=[
                'additional_underlyings',
                'cfi',
                'correction',
                'primary_exchange',
                'shares_per_contract',
                'exercise_style',
                'underlying_ticker'
            ], axis=1, inplace=True)
            df.rename({
                'contract_type':'type',
                'ticker':'contract_ticker',
                'underlying_ticker':'stock_ticker'
                }, axis=1, inplace=True)
            df['expiration_date'] = pd.to_datetime(df['expiration_date'])
            df['ticker'] = ticker.upper()
            return df
        
        def options_aggs_data_clean_(self, data):
            df = pd.DataFrame(data)
            df['date'] = df['timestamp'].astype('datetime64[ms]')
            df['date'] = pd.to_datetime(df['date'].dt.date)
            df.drop(['timestamp', 'transactions', 'otc'], axis=1, inplace=True)
            return df
        
    ##############################

    def get_contracts_from_ticker(self, ticker):
        contracts = []
        for contract in self.client.list_options_contracts(ticker.upper(), limit=1000):
            contracts.append(contract)
        df = self.utils.options_contract_data_clean_(contracts, ticker)
        if not contracts:
            return "ERROR! dictionary empty; check request parameters"
        else:
            df['ticker'] = ticker.upper()
            return df
        
    def get_aggs_options(self, contract_ticker):
        contract_pricing = []
        for agg in self.client.list_aggs(contract_ticker, 1, 'day', '2000-01-01', '2025-01-01', limit=5000):
            contract_pricing.append(agg)
        if not contract_pricing:
            return "ERROR! dictionary empty; check request parameters"
        else:
            df = self.utils.options_aggs_data_clean_(contract_pricing)
            df['contract_ticker'] = contract_ticker
            return df


class DataBaseClient:
    def __init__(self, db_name):
        self.db = db_name
        self.conn = sqlite3.connect(db_name)

    def data_query(self, sql_string):
        df = pd.read_sql(sql_string, self.conn)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            return df
        elif 'expiration_date' in df.columns:
            df['expiration_date'] = pd.to_datetime(df['expiration_date'])
            return df
        else:
            return df
        
    def data_add(self, df, table_name):
        df.to_sql(table_name, self.conn, if_exists='append', index=False)

    def get_table_names(self):
        df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", self.conn)
        return df 
    
    def close_db(self):
        self.conn.close()

    def delete_table(self, table_name):
        self.conn.execute(f"DROP TABLE {table_name}")


class FredClient:
    def __init__(self, api_key):
        self.fred = Fred(api_key=api_key)
        self.utils = self.Utils()
    
    class Utils:
        def __init__(self):
            pass

        def fred_data_clean_(self, data):
            df = pd.DataFrame(data, columns=['value'])
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'date'}, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df = df[df['date'].dt.year >= 2000]
            return df

    ##############################

    def get_series(self, series):
        data = self.fred.get_series(series)
        df = self.utils.fred_data_clean_(data)
        return df
    

class FmpClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://financialmodelingprep.com/api/v3/'
        self.utils = self.Utils()

    class Utils:
        def __init__(self):
            pass
        
        def request_(self, url):
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                if not data:
                    return "ERROR! dictionary empty; check request parameters"
                else:
                    return data
            else:
                return r.json()

        def aggs_data_clean_(self, raw_data):
            data = raw_data['historical']
            df = pd.DataFrame(data)
            df.drop(columns=[
                'unadjustedVolume',
                'label',
                'changeOverTime',
                'adjClose'
            ], axis=1, inplace=True)
            df.rename(columns={
                'changePercent':'percent_change'
            }, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df['change'] = round(df['change'], 4)
            df['percent_change'] = round(df['percent_change'], 4)
            df['vwap'] = round(df['vwap'], 4)
            return df

        def human_num_read_(self, number):
            suffixes = ['', 'K', 'M', 'B', 'T']
            magnitude = 0
            num = float(number)
            while abs(num) >= 1000 and magnitude < len(suffixes) - 1:
                magnitude += 1
                num /= 1000.0
            human_readable = f"{num:.3}{suffixes[magnitude]}"
            return human_readable
        
        def dividend_data_clean_(self, raw_data):
            data = raw_data['historical']
            if not data:
                return "ERROR! list empty; no dividend"
            else:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df = df[df['date'].dt.year >= 2000]
                df.drop(columns=[
                    'date',
                    'label',
                    'adjDividend',
                    'recordDate',
                    'paymentDate',
                ], axis=1, inplace=True)
                df.rename(columns={
                    'declarationDate':'date'
                }, inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                return df
        
        def income_statement_data_clean_(self, data):
            df = pd.DataFrame(data)
            df.rename(columns={
                'calendarYear':'year',
                'costOfRevenue':'cost_of_revenue',
                'grossProfit':'gross_profit',
                'grossProfitRatio':'gross_profit_ratio',
                'researchAndDevelopmentExpenses':'r_and_d_exp',
                'generalAndAdministrativeExpenses':'general_and_admin_exp',
                'sellingAndMarketingExpenses':'selling_and_marketing_exp',
                'sellingGeneralAndAdministrativeExpenses':'selling_general_and_admin_exp',
                'otherExpenses':'other_exp',
                'operatingExpenses':'operating_exp',
                'costAndExpenses':'cost_and_exp',
                'interestIncome':'interest_income',
                'interestExpense':'interest_expense',
                'depreciationAndAmortization': 'depreciation_and_amortization',
                'ebitdaratio':'ebitda_ratio',
                'operatingIncome':'operating_income',
                'operatingIncomeRatio':'operating_income_ratio',
                'totalOtherIncomeExpensesNet':'total_other_income_exp_net',
                'incomeBeforeTax':'income_before_tax',
                'incomeBeforeTaxRatio':'income_before_tax_ratio',
                'incomeTaxExpense':'income_tax_expense',
                'netIncome':'net_income',
                'netIncomeRatio':'net_income_ratio',
            }, inplace=True)
            df.drop(columns=[
                'reportedCurrency',
                'cik',
                'link',
                'date',
                'finalLink',
                'acceptedDate',
                'epsdiluted',
                'weightedAverageShsOut',
                'weightedAverageShsOutDil',
                'period'
            ],inplace=True)
            df['date'] = pd.to_datetime(df['fillingDate'])
            df.drop(columns=['fillingDate'], inplace=True)
            df = df[df['date'].dt.year >= 2000]
            return df
        
        def balance_sheet_data_clean_(self, data):
            df = pd.DataFrame(data)
            df.rename(columns={
                'calendarYear':'year',
                'cashAndCashEquivalents':'cash_and_cash_equivalents',
                'shortTermInvestments':'short_term_investments',
                'cashAndShortTermInvestments':'cash_and_short_term_investments',
                'netReceivables':'net_receivables',
                'otherCurrentAssets':'other_current_assets',
                'totalCurrentAssets':'total_current_assets',
                'propertyPlantEquipmentNet':'property_plant_equipment_net',
                'intangibleAssets':'intangible_assets',
                'goodwillAndIntangibleAssets':'goodwill_and_intangible_assets',
                'longTermInvestments':'long_term_investments',
                'taxAssets':'tax_assets',
                'otherNonCurrentAssets':'other_non_current_assets',
                'totalNonCurrentAssets':'total_non_current_assets',
                'otherAssets':'otherAssets',
                'totalAssets':'totalAssets',
                'accountPayables':'account_payables',
                'shortTermDebt':'short_term_debt',
                'taxPayables':'tax_payables',
                'deferredRevenue':'deferred_revenue',
                'otherCurrentLiabilities':'other_current_liabilities',
                'totalCurrentLiabilities':'total_current_liabilities',
                'longTermDebt':'long_term_debt',
                'deferredRevenueNonCurrent':'deferred_revenue_non_current',
                'deferredTaxLiabilitiesNonCurrent':'deferred_tax_liabilities_non_urrent',
                'otherNonCurrentLiabilities':'other_non_current_liabilities',
                'totalNonCurrentLiabilities':'total_non_current_liabilities',
                'otherLiabilities':'other_liabilities',
                'capitalLeaseObligations':'capital_lease_obligations',
                'totalLiabilities':'total_liabilities',
                'preferredStock':'preferred_stock',
                'commonStock':'common_stock',
                'retainedEarnings':'retained_earnings',
                'accumulatedOtherComprehensiveIncomeLoss':'accumulated_other_comprehensive_income_loss',
                'othertotalStockholdersEquity':'other_total_stockholders_equity',
                'totalStockholdersEquity':'total_stockholders_equity',
                'totalEquity':'total_equity',
                'totalLiabilitiesAndStockholdersEquity':'total_liabilities_and_stockholders_equity',
                'minorityInterest':'minority_nterest',
                'totalLiabilitiesAndTotalEquity':'total_liabilities_and_total_equity',
                'totalInvestments':'total_investments',
                'totalDebt':'total_debt',
                'netDebt':'net_debt'
            }, inplace=True)
            df.drop(columns=[
                'link',
                'finalLink',
                'reportedCurrency',
                'cik',
                'period',
                'acceptedDate',
                'date'
            ],inplace=True)
            df['date'] = pd.to_datetime(df['fillingDate'])
            df.drop(columns=['fillingDate'], inplace=True)
            df = df[df['date'].dt.year >= 2000]
            return df
        
        def cash_flow_data_clean_(self, data):
            df = pd.DataFrame(data)
            df.rename(columns={
                'calendarYear':'year',
                'netIncome':'net_income',
                'depreciationAndAmortization':'depreciation_and_amortization',
                'deferredIncomeTax':'deferred_income_tax',
                'stockBasedCompensation':'stock_based_compensation',
                'changeInWorkingCapital':'change_in_working_capital',
                'accountsReceivables':'accounts_receivables',
                'accountsPayables':'accounts_payables',
                'otherWorkingCapital':'other_working_capital',
                'otherNonCashItems':'other_non_cash_items',
                'netCashProvidedByOperatingActivities':'net_cash_provided_by_operating_activities',
                'investmentsInPropertyPlantAndEquipment':'investments_in_property_plant_and_equipment',
                'acquisitionsNet':'acquisitions_net',
                'purchasesOfInvestments':'purchases_of_investments',
                'salesMaturitiesOfInvestments':'sales_maturities_of_investments',
                'otherInvestingActivites':'other_investing_activites',
                'netCashUsedForInvestingActivites':'net_cash_used_for_investing_activites',
                'debtRepayment':'debt_repayment',
                'commonStockIssued':'common_stock_issued',
                'commonStockRepurchased':'common_stock_repurchased',
                'dividendsPaid':'dividends_paid',
                'otherFinancingActivites':'other_financing_activites',
                'netCashUsedProvidedByFinancingActivities':'net_cash_used_provided_by_financing_activities',
                'effectOfForexChangesOnCash':'effect_of_forex_changes_on_cash',
                'netChangeInCash':'net_change_in_cash',
                'cashAtEndOfPeriod':'cash_at_end_of_period',
                'cashAtBeginningOfPeriod':'cash_at_beginning_of_period',
                'operatingCashFlow':'operating_cash_flow',
                'capitalExpenditure':'capital_expenditure',
                'freeCashFlow':'free_cash_flow'
            }, inplace=True)
            df.drop(columns=[
                'reportedCurrency',
                'cik',
                'acceptedDate',
                'link',
                'period',
            ],inplace=True)
            df['date'] = pd.to_datetime(df['fillingDate'])
            df.drop(columns=['fillingDate'], inplace=True)
            df = df[df['date'].dt.year >= 2000]
            return df
        
    ##############################

    def get_aggs(self, ticker):
        url = f'{self.base_url}historical-price-full/{ticker.upper()}?from=2000-01-01&to=2025-01-01&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.aggs_data_clean_(data)
            df['ticker'] = ticker.upper()
            return df
        
    def get_aggs_forex(self, pairs):
        url = f'{self.base_url}historical-price-full/{pairs.upper()}?from=2000-01-01&to=2025-01-01&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.aggs_data_clean_(data)
            df.drop(columns=[
                'volume'
            ], inplace=True)
            df['ticker'] = pairs.upper()
            return df
        
    def get_aggs_index(self, pairs):
        url = f'{self.base_url}historical-price-full/{pairs.upper()}?from=2000-01-01&to=2025-01-01&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.aggs_data_clean_(data)
            df.drop(columns=[
                'volume'
            ], inplace=True)
            df['ticker'] = pairs.upper()
            return df

    def get_price_rt(self, ticker):
        url = f'{self.base_url}quote-short/{ticker.upper()}?apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = pd.DataFrame(data)
            return df

    def get_market_cap_rt(self, ticker, human=False):
        url = f'{self.base_url}market-capitalization/{ticker.upper()}?from=2000-01-01&to-2025-01-01&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = pd.DataFrame(data)
            df.drop(columns=['date'], inplace=True)
            df.rename(columns={'marketCap':'market_cap'}, inplace=True)
            if human:
                df['market_cap'] = df['market_cap'].apply(self.human_read_)
                return df
            else:
                return df

    def get_market_cap_range(self, ticker, start, finish):
        url = f'{self.base_url}historical-market-capitalization/{ticker.upper()}?&from={start}&to={finish}&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = pd.DataFrame(data)
            df.rename(columns={'marketCap':'market_cap'}, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            return df

    def get_market_cap_history(self, ticker):
        dates = [
            ['2000-01-01','2005-01-01'],
            ['2005-01-01', '2010-01-01'],
            ['2010-01-01', '2015-01-01'],
            ['2015-01-01', '2020-01-01'],
            ['2020-01-01', '2025-01-01']
        ]
        df = pd.DataFrame()
        for i in dates:
            data = self.get_market_cap_range(ticker.upper(), i[0], i[1])
            if isinstance(data, str) and data == "ERROR! dictionary empty; check request parameters":
                continue
            else:
                df = pd.concat([df, data], ignore_index=True)
        if df.empty:
            return "ERROR! DataFrame empty; check request parameters"
        df = df.sort_values(by='date').reset_index(drop=True)
        return df
    
    ##############################

    def get_snp_companies(self):
        url = f'{self.base_url}sp500_constituent?apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = pd.DataFrame(data)
            df.drop(columns='cik', inplace=True)
            df.rename(columns={
                'subSector':'sub_sector',
                'headQuarter': 'hq',
                'dateFirstAdded': 'date_added'
            }, inplace=True)
            return df

    def get_dow_companies(self):
        url = f'{self.base_url}dowjones_constituent?apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = pd.DataFrame(data)
            df.drop(columns='cik', inplace=True)
            df.rename(columns={
                'subSector':'sub_sector',
                'headQuarter': 'hq',
                'dateFirstAdded': 'date_added'
            }, inplace=True)
            return df
        
    def get_nasdaq_companies(self):
        url = f'{self.base_url}nasdaq_constituent?apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = pd.DataFrame(data)
            df.drop(columns='cik', inplace=True)
            df.rename(columns={
                'subSector':'sub_sector',
                'headQuarter': 'hq',
                'dateFirstAdded': 'date_added'
            }, inplace=True)
            return df  

    ##############################

    def get_income_statement_a(self, ticker):
        url = f'{self.base_url}income-statement/{ticker.upper()}?period=annual&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.income_statement_data_clean_(data)
            return df  
        
    def get_income_statement_q(self, ticker):
        url = f'{self.base_url}income-statement/{ticker.upper()}?period=quarter&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.income_statement_data_clean_(data)
            return df
    
    def get_balance_sheet_a(self, ticker):
        url = f'{self.base_url}balance-sheet-statement/{ticker.upper()}?period=annual&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.balance_sheet_data_clean_(data)
            return df
    
    def get_balance_sheet_q(self, ticker):
        url = f'{self.base_url}balance-sheet-statement/{ticker.upper()}?period=quarter&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.balance_sheet_data_clean_(data)
            return df
        
    def get_cash_flow_a(self, ticker):
        url = f'{self.base_url}cash-flow-statement/{ticker.upper()}?period=annual&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.cash_flow_data_clean_(data)
            return df
        
    def get_cash_flow_q(self, ticker):
        url = f'{self.base_url}cash-flow-statement/{ticker.upper()}?period=quarter&apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.cash_flow_data_clean_(data)
            return df
        
    def get_dividend(self, ticker):
        url = f'{self.base_url}historical-price-full/stock_dividend/{ticker.upper()}?apikey={self.api_key}'
        data = self.utils.request_(url)
        if data == "ERROR! dictionary empty; check request parameters":
            return "ERROR! dictionary empty; check initial request"
        else:
            df = self.utils.dividend_data_clean_(data)
            return df
        
    ##############################
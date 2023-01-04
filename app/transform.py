import pandas as pd
import datetime

from tabulate import tabulate


class Transformation:
    """all data manipulation functions"""

    def __init__(self, config):
        self.project = config.project
        self.team = config.team
        self.status = config.status

    def calculate_sp(self, df):
        """T-shirts estimates -> numeric estimates"""
        df.loc[df['labels'].apply(lambda x: 'XS' in x), 'sp'] = 1
        df.loc[df['labels'].apply(lambda x: 'xs' in x), 'sp'] = 1
        df.loc[df['labels'].apply(lambda x: 'S' in x), 'sp'] = 2
        df.loc[df['labels'].apply(lambda x: 's' in x), 'sp'] = 2
        df.loc[df['labels'].apply(lambda x: 'M' in x), 'sp'] = 3
        df.loc[df['labels'].apply(lambda x: 'm' in x), 'sp'] = 3
        df.loc[df['labels'].apply(lambda x: 'L' in x), 'sp'] = 5
        df.loc[df['labels'].apply(lambda x: 'l' in x), 'sp'] = 5
        df.loc[df['labels'].apply(lambda x: 'XL' in x), 'sp'] = 8
        df.loc[df['labels'].apply(lambda x: 'xl' in x), 'sp'] = 8
        return df

    def make_issues_with_times(self, issues, changelog):
        """
        1. join issues with log
        2. assign teams by components
        3. calculate cycle time (from first touch to done)
        4. calculate lead time (from created to done or now())
        :param issues: jira issues
        :param changelog: jira changelog
        :return: ready to use datamart (dataframe) with lead times, cycle times, teams
        """
        df = issues.join(changelog.set_index('key'), on='key')

        # assign teams and weeks
        df['team'] = df['component'].map(self.team)
        df['issue_created'] = pd.to_datetime(df['issue_created'], errors='coerce')
        df['week_created'] = df['issue_created'].dt.strftime('%U')
        df['month_created'] = df['issue_created'].dt.strftime('%m')
        df['year_created'] = df['issue_created'].dt.strftime('%Y')
        df['year_week_created'] = df['issue_created'].dt.strftime('%Y-%U')

        # calculate cycle time
        # 1st time taken from backlog = start of cycle
        df_ct = df[(df['from_string'] == 'Backlog') & (df['rank'] == 1)]
        df_ct['ct_days'] = round((df_ct['done_or_now'] - df_ct['log_created']) / pd.to_timedelta(1, unit='D'), 2)
        df = df.join(df_ct[['key', 'ct_days']].set_index('key'), on='key')

        # calculate lead time
        df['lt_days'] = round((df['done_or_now'] - df['issue_created']) / pd.to_timedelta(1, unit='D'), 2)

        # exclude redundant columns
        df = df.loc[:, ~df.columns.isin(['from_string', 'to_string', 'rank',
                                         'done_or_now', 'status_ended', 'log_created'])]
        # exclude lists
        # for drop_duplicates and convenience
        df = df.loc[:, ~df.columns.isin(['sprints', 'labels'])]
        # datetime to date
        # for drop_duplicates and convenience
        date_columns = df.select_dtypes(include='datetime64[ns, UTC]').columns
        for date_column in date_columns:
            df[date_column] = df[date_column].dt.date

        df.drop_duplicates(inplace=True)

        df_st = self.__calculate_status_days(changelog)
        df = df.join(df_st, on='key')
        return df

    def calculate_hours_by_types(self, df):
        """
        pivot table with median for the last 180 days
        :param df: dataframe with issue keys, teams, issue types, creation dates and days in statuses
        :return:
        """
        df = df[pd.to_datetime(df['issue_created']) >= (datetime.datetime.now() - pd.to_timedelta('180day'))]
        df = df.pivot_table(index=['team', 'issue_type'],
                            values=['ct_days', 'lt_days', 'in_progress_days'], aggfunc='median').reset_index()
        return df

    def __calculate_status_days(self, changelog):
        """
        for each issue status (except done, canceled) calculates days in this status
        :param changelog: changelog dataframe with status and lead(status)
        :return: dataframe with changelog columns + 1 column for each status with days in that status
        """
        # to bring datamart column names to the same form
        names_dict = {'To Do': 'to_do_days',
                      'In Progress': 'in_progress_days',
                      'In Review': 'in_review_days',
                      'Reporter Review': 'reporter_review_days'}

        df = changelog[changelog['to_string'].isin(self.status)]
        df['days'] = round((df['status_ended'] - df['log_created']) /
                           pd.to_timedelta(1, unit='D'), 4)
        df = df.pivot_table(index='key', columns='to_string', values='days', aggfunc='sum')
        df.rename(columns=names_dict, inplace=True)
        return df


class SalesEfficiency:
    """
    functions to calculate load types =  sales team load by proportion of the following types of tasks:
    1. Техническая операционка = Ошибка + Fail
    2. Ad-hoc и выгрузки = Ad-hoc * 0.91 (coefficient from sep.2021)
    3. Бизнесовая операционка = Ad-hoc * 0.09 (coefficient from sep.2021)
    4. Бизнесовые проекты развития = Задача * 0.33 + Доработка отчета + Новый отчет (coefficient from sep.2021)
    5. Технические проекты развития = Задача * 0.77 (coefficient from sep.2021)

    calculation in excel:
    https://docs.google.com/spreadsheets/d/1E3lcHgwVpnDYTvlrYGZUy1cFzE5a0CkFN3m4a-zC7r0/edit#gid=0
    """

    """
    TODO:
    - Задачи для бизнесовых проектов развития должны определятся по эпикам
    - Бизнесовая операционка должна определяться по заказчику (бизнес заказчик - тот, которого нет в исполнителях)
    - Задачи для тех. проектов должны определяться по эпикам + по заказчикам не из бизнеса (проверить такой подход)
    """

    def __init__(self, config):
        self.fails = config.fails29122023
        self.coefficients = config.coefficients

    def assign_correct_type(self, df):
        """
        reassign issue type for issues from list fails
        :param df: issues
        :return: correct issue types
        """
        df.loc[df['key'].isin(self.fails), 'issue_type'] = 'Fail'
        return df

    def map_strategy_types(self, df):
        """
        assign strategy team load types
        :param df: issues with issue_type and team
        :return: df with load statistics by month
        """
        df = df[['key', 'epic', 'issue_type', 'year_created', 'month_created', 'team', 'in_progress_days']] \
            .loc[df['team'] == 'sales']
        df['load_type'] = ''

        df = self.__propagate_rows_by_list(df, 'Ad-hoc', ['Ad-hoc и выгрузки',
                                                          'Бизнесовая операционка'])
        df.loc[df['load_type'] == 'Ad-hoc и выгрузки', 'in_progress_days'] = \
            df.loc[df['load_type'] == 'Ad-hoc и выгрузки', 'in_progress_days'] \
            * self.coefficients['simpleAdHoc']
        df.loc[df['load_type'] == 'Бизнесовая операционка', 'in_progress_days'] = \
            df.loc[df['load_type'] == 'Бизнесовая операционка', 'in_progress_days'] \
            * self.coefficients['businessAdHoc']

        # TODO: not working
        # df = self.__propagate_rows_by_list(df, 'Задача', ['Бизнесовые проекты развития',
        #                                                   'Технические проекты развития'])
        # df.loc[df['load_type'] == 'Бизнесовые проекты развития', 'in_progress_days'] = \
        #     df.loc[df['load_type'] == 'Бизнесовые проекты развития', 'in_progress_days']\
        #     * self.coefficients['businessTask']
        # df.loc[df['load_type'] == 'Технические проекты развития', 'in_progress_days'] = \
        #     df.loc[df['load_type'] == 'Технические проекты развития', 'in_progress_days']\
        #     * self.coefficients['techTask']

        df.loc[df['issue_type'] == 'Задача', 'load_type'] = 'Проекты развития'  # TODO: delete
        df.loc[df['issue_type'] == 'Ошибка', 'load_type'] = 'Техническая операционка'
        df.loc[df['issue_type'] == 'Fail', 'load_type'] = 'Техническая операционка'
        df.loc[df['issue_type'] == 'Доработка отчета', 'load_type'] = 'Проекты развития'
        df.loc[df['issue_type'] == 'Новый отчет', 'load_type'] = 'Проекты развития'

        # print(tabulate(df.head(3), headers='keys'))

        df = df.pivot_table(index=['year_created', 'month_created', 'load_type'],
                            values='in_progress_days', aggfunc='sum').reset_index()

        df_montly_agg = df.pivot_table(index=['year_created', 'month_created'],
                                       values='in_progress_days', aggfunc='sum').reset_index()
        df_montly_agg.rename(columns={'in_progress_days': 'in_progress_days_month'}, inplace=True)
        df = pd.merge(df, df_montly_agg, how='left', on=['year_created', 'month_created'])
        df['percentage'] = round(100 * (df['in_progress_days'] / df['in_progress_days_month']), 2)

        # print(tabulate(df.head(3), headers='keys'))
        # print(tabulate(df_montly_agg.head(3), headers='keys'))
        # print(df.dtypes)
        df = df.convert_dtypes()
        # print(df.dtypes)

        return df

    def __propagate_rows_by_list(self, df, issue_type, lst):
        """
        one does not simply assign a list to a dataframe cell
        here is a workaround
        :param df:
        :param issue_type:
        :return:
        """
        rows = df.loc[df['issue_type'] == issue_type, 'load_type'].index
        for i in rows:
            df.at[i, 'load_type'] = lst

        df = df.explode('load_type')
        return df

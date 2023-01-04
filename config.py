from os import environ
from dotenv import load_dotenv


class Config:
    """
    credentials for postgres (pg) with jira data and for greenplum (gp) - target dwh
    project - key for jira project
    team - dict to distribute jira components between two teams
    status - list with status to calculate statistics for them
        (it's not possible to calculate time in backlog from changelog only)
    fails29122023 - sales team issues that are fails (market as [Исследование] in issue summary)
        jql: project = "BI Core-Team" and summary ~ "исследование" order by created (not all from sales)
    coefficients - for mapping issue types
    """

    load_dotenv('/Users/a.bondarenko/.env')
    pgurl = environ.get('pgurl')
    gpurl = environ.get('gpurl')

    project = 'BC'
    team = {'КС': 'core', 'Маркетинг': 'core', 'Финансы': 'core', 'Стратегия': 'core', 'Core': 'core', 'HR': 'core',
            'БЮ Коммерческая': 'core', 'Модерация': 'core', 'МСФО': 'core', 'Топ-менеджмент и СД': 'core',
            'math': 'core', 'Исследователи': 'core',
            'B2B Marketing': 'sales', 'Вторичка': 'sales', 'SMB': 'sales', 'Newbuilding': 'sales', 'Продажи': 'sales',
            'Countryside': 'sales'}
    status = ['To Do', 'In Progress', 'In Review', 'Reporter Review']
    fails29122023 = ['BC-4286', 'BC-4294', 'BC-4278', 'BC-4265', 'BC-4259', 'BC-4251', 'BC-4170', 'BC-4150', 'BC-4134',
                     'BC-4035', 'BC-4034', 'BC-4028', 'BC-4026', 'BC-4014', 'BC-4001', 'BC-3994', 'BC-3988', 'BC-3980',
                     'BC-3976', 'BC-3951', 'BC-3939', 'BC-3938', 'BC-3937', 'BC-3921', 'BC-3920', 'BC-3915', 'BC-3907',
                     'BC-3795']
    coefficients = {'businessAdHoc': 0.09, 'simpleAdHoc': 0.91, 'businessTask': 0.33, 'techTask': 0.77}

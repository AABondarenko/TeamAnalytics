import sqlalchemy as sql
import pandas as pd


class Devmetrics:
    """extract jira issues and log from internal database"""

    def __init__(self, Config):
        self.project = Config.project
        self.url = Config.pgurl
        self.db = sql.create_engine(self.url)

    def extract_issues(self):
        issuessql = '''
            with t as (
                select i.key
                    ,i.epic
                    ,i.assignee_key
                    ,i.reporter_key
                    ,u.name as assignee
                    ,i.issue_type
                    ,unnest(i.components)
                    ,i.status
                    ,i.sprints
                    ,i.labels
                    ,i.created as issue_created
                    ,coalesce(i.done, now()) as done_or_now -- if an issue isn't done, calculate CT and LT till today
                    ,i.done
                from issues i
                left join jira_users u on i.assignee_key = u.key
                where 1=1
                    and i.status != 'Canceled'
                    and i.issue_type not in ('Epic', 'История')
                    and i.project_key = %s)
            select t.key
                ,t.epic
                ,a.summary as epic_name
                ,t.issue_type
                ,t.assignee
                ,u.name as reporter
                ,c.name as component
                ,t.status
                ,t.sprints
                ,t.labels
                ,t.issue_created
                ,t.done_or_now
                ,t.done
            from t
            left join jira_users u on t.reporter_key = u.key
            left join components c on t.unnest = c.id
            left join (select key, summary from issues where issue_type = 'Epic') as a -- add epic names
                on t.epic = a.key'''
        issues = pd.read_sql(issuessql, params=[self.project], con=self.db)
        success_message = f'Extracted {len(issues)} rows from issues table'
        print(success_message)
        return issues

    def extract_changelog(self):
        changelogsql = '''
            select i.key
                ,c.from_string
                ,c.to_string
                ,c.created as log_created
                -- for Cycle Time we'll need first from_string=Backlog
                ,rank() over (partition by i.key, c.from_string order by c.created)
                -- date of next status, i.e. when current status (to_string) ended
                ,lead(c.created) over (partition by i.key order by c.created) as status_ended
            from issues i
            left join changelog c on i.id = c.issue_id
            where 1=1
                and i.status != 'Canceled'
                and i.issue_type not in ('Epic', 'История')
                and i.project_key = %s
                and c.field_id = 'status' '''
        changelog = pd.read_sql(changelogsql, params=[self.project], con=self.db)
        success_message = f'Extracted {len(changelog)} rows from changelog table'
        print(success_message)
        return changelog

    def extract_sprint(self, sprint):
        """Create SQL table of JIRA
        :param sprint:
        :return:
        """
        # TODO do something with many components by 1 issue
        # TODO try to get sprint with API
        sprintsql = '''
            with t as(
                select i.key
                    ,i.status
                    ,i.done
                    ,i.labels
                    ,unnest(i.components)
                    ,unnest(i.sprints) as sprint
                from issues i
                where 1=1
                    and project_key = 'BC'
                    --and 'Core.S10' = any(sprints)
                )
            select t.*
                ,c.name as component_name
            from t
            left join components c on t.unnest = c.id
            where sprint = %s'''
        sprint = pd.read_sql(sprintsql, params=[sprint], con=self.db)
        return sprint


class GP:
    """extract useful information from DWH"""

    def __init__(self, Config):
        self.url = Config.gpurl
        self.db = sql.create_engine(self.url)

    def get_calendar(self):
        """dictionary with working and non-working days"""
        calendar_sql = 'select dt, day_type, start_week_dt, end_week_dt from dm.dm_calendar_d'
        calendar = pd.read_sql(calendar_sql, con=self.db)
        success_message = f'Extracted {len(calendar)} rows from calendar'
        print(success_message)
        return calendar

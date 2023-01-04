from config import Config
from app.extract import Devmetrics
from app.transform import Transformation, SalesEfficiency
from app.load import Load


def main(draw=False):
    df, df_median_days, df_sales_efficiency = create_tables()

    load = Load(Config)
    load.to_greenplum(df, 'stats_detailed')
    load.to_greenplum(df_median_days, 'stats_median')
    load.to_greenplum(df_sales_efficiency, 'sales_efficiency')
    # df.to_excel('output.xlsx')

    if draw:
        draw_burndown()

    return print('Done')


def draw_burndown(sprint='Core.S10'):
    """
    burndown for specific sprint
    :param sprint: jira sprint name
    :return: graph
    """
    # TODO: finish function
    jira = Devmetrics(Config)
    df = jira.extract_sprint(sprint)
    return print(df.head(10))


def create_tables():
    """
    extractions and combined transformations (order matters)
    :return: ready to use datamarts (dataframes) with lead times, cycle times, story points etc.
    """
    jira = Devmetrics(Config)
    transform = Transformation(Config)
    sales = SalesEfficiency(Config)

    issues = jira.extract_issues()
    issues = transform.calculate_sp(issues)

    changelog = jira.extract_changelog()
    df = transform.make_issues_with_times(issues, changelog)

    df_median_days = transform.calculate_hours_by_types(df)

    df_sales_efficiency = sales.assign_correct_type(df)
    df_sales_efficiency = sales.map_strategy_types(df_sales_efficiency)

    return df, df_median_days, df_sales_efficiency


if __name__ == '__main__':
    main()

from sqlalchemy import create_engine, text


class Load:
    """upload results to Greenplum"""

    def __init__(self, Config):
        self.url = Config.gpurl
        self.db = create_engine(self.url)

    def to_greenplum(self, df, table):
        self.__drop_table(table)
        success_message = self.__upload_df(df, table)
        print(success_message)

    def __drop_table(self, table):
        """Dropping tables for idempotence"""
        sql = text(f'DROP TABLE IF EXISTS {table}')
        self.db.execute(sql)

    def __upload_df(self, df, table):
        """
        upload dataframe to greenplum
        :param df: dataframe
        :param table: target table
        :return: message with nrows uploaded
        """
        df.to_sql(
            table,
            self.db,
            if_exists='fail',
            chunksize=500,
            index=False)

        success_message = f'Uploaded {len(df)} rows to {table} table'
        return success_message

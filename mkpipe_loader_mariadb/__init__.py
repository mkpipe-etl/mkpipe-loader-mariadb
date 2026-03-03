from mkpipe.spark import JdbcLoader

JAR_PACKAGES = ['org.mariadb.jdbc:mariadb-java-client:3.5.1']


class MariadbLoader(JdbcLoader, variant='mariadb'):
    driver_name = 'mariadb'
    driver_jdbc = 'org.mariadb.jdbc.Driver'

    def build_jdbc_url(self):
        return (
            f'jdbc:{self.driver_name}://{self.host}:{self.port}/{self.database}'
            f'?user={self.username}&password={self.password}'
            f'&sessionVariables=sql_mode=ANSI_QUOTES'
        )

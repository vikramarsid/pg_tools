import os
import re
import psycopg2
import logging
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from . import utils
from .utils import CouldNotConnectPostgreSQLException
from .utils import CreatedbFailedException
from .utils import ExportFileAlreadyExistsException
from .utils import UnknownCommandException

BUFSIZE = 8 * 1024 * 1024

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PGRestore:
    """Will launch correct pgrestore binary to restore a dump file to some
    remote database, which we have to create first"""

    def __init__(
        self,
        dbname,
        user,
        host,
        port,
        owner,
        maintdb,
        major,
        restore_cmd="/usr/bin/pg_restore",
        st=False,
        schemas=None,
        schemas_nodata=None,
        relname_nodata=None,
        connect=True,
        connect_timeout=3,
    ):
        """ dump is a filename """

        self.dbname = dbname
        self.user = user
        self.host = host
        self.port = int(port)
        self.owner = owner
        self.maintdb = maintdb
        self.major = major
        self.restore_cmd = restore_cmd
        self.st = st
        self.schemas = schemas or []
        self.schemas_nodata = schemas_nodata or []
        self.relname_nodata = relname_nodata or []
        self.connect_timeout = connect_timeout
        self.restore_jobs = 1
        self.mconn = None

        # check that the pg_restore binary do exists
        if not os.path.isfile(self.restore_cmd):
            mesg = f"Error: pg_restore command: no such file '{self.restore_cmd}'"
            raise UnknownCommandException(mesg)

        self.dsn = f"dbname='{self.maintdb}' user='{self.user}' host='{self.host}' port={self.port} connect_timeout={connect_timeout}"
        self.mconn = None

        if not connect:
            return

        try:
            self.mconn = psycopg2.connect(self.dsn)
        except Exception as exp:
            mesg = f"Error: could not connect to server '{host}'"
            mesg += f"\nDetail: {exp}"
            mesg += "\nHint: Following command might help to debug:"
            mesg += f"\n  psql -U {user} -h {host} -p {port} {self.maintdb} "
            raise CouldNotConnectPostgreSQLException(mesg)

        logger.info(f"Connected to {self.dsn}")

    def __del__(self):
        """ destructor, close the PG connection """
        if self.mconn is not None:
            self.mconn.close()
            self.mconn = None

    def source_sql_file(self, filename):
        """ load the given SQL file into the maintenance connection """

        # we use the psql console in order to support extended commands
        cmd = "psql -U %s -h %s -p %s -f %s %s " % (
            self.user,
            self.host,
            self.port,
            filename,
            self.maintdb,
        )

        logger.info(cmd)

        out = os.popen(cmd)
        line = "stupid init value"
        while line != "":
            line = out.readline()
            logger.info(line[:-1])

        returncode = out.close()
        return returncode

    def createdb(self, encoding):
        """ connect to remote PostgreSQL server to create the new database"""

        logger.info(f"createdb -O {self.owner} -E {encoding} {self.dbname}")

        try:
            # create database can't run from within a transaction
            self.mconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = self.mconn.cursor()
            curs.execute(
                f'CREATE DATABASE "{self.dbname}" '
                + f"WITH OWNER \"{self.owner}\" ENCODING '{encoding}'"
            )
            curs.close()
        except Exception as exp:
            mesg = f"Error: createdb: {exp}"
            raise CreatedbFailedException(mesg)

        logger.debug(
            f"created database '{self.dbname}' owned by '{self.owner}', encoded in {encoding}"
        )

    def dropdb(self):
        """ connect to remote PostgreSQL server to drop database"""

        logger.info(f"dropdb {self.dbname}")

        try:
            # drop database can't run from within a transaction
            self.mconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = self.mconn.cursor()
            curs.execute(f'DROP DATABASE "{self.dbname}"')
            curs.close()
        except Exception:
            raise

        logger.info(f'dropped database "{self.dbname}"')

    def vacuumdb(self):
        """ connect to remote PostgreSQL server to vacuum database"""

        logger.info(f"vacuumdb analyze {self.dbname}")

        try:
            # vacuum database can't run from within a transaction
            self.mconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = self.mconn.cursor()

            # mesure pg_restore timing
            import time

            start_time = time.time()

            curs.execute("VACUUM ANALYZE")

            end_time = time.time()

            curs.close()
        except Exception:
            raise

        return end_time - start_time

    def try_connection(self, timeout=None):
        """try to connect to target database and raise Exception after
        timeout, this helps preventing pgbouncer pause issues and waiting
        before a non running pg_restore"""

        if timeout is None:
            timeout = self.connect_timeout

        dsn = f"dbname='{self.dbname}' user='{self.user}' host='{self.host}' port={self.port} connect_timeout={timeout}"

        logger.info(f"Trying to connect to: {dsn}")

        try:
            mconn = psycopg2.connect(dsn)
            mconn.close()
        except Exception:
            raise

    def pg_restore(self, filename, excluding_tables=None):
        """ restore dump file to new database """

        if not excluding_tables:
            excluding_tables = []

        os.system(f"ls -l {self.restore_cmd}")
        if self.schemas:
            logger.info(f"Restoring only schemas: {self.schemas}")

        # Single Transaction?
        st = ""
        if self.st:
            logger.info("Notice: pg_restore will work in a single transaction")
            st = "-1"

        # prepare cmd in several steps
        cmd = [
            self.restore_cmd,
            st,
            "-h",
            self.host,
            "-p",
            str(self.port),
            "-U",
            self.user,
            "-d",
            self.dbname,
        ]

        # pg_restore -j
        if self.restore_jobs > 1:
            cmd += ["-j", str(self.restore_jobs)]

        # Exclude some schemas at restore time?
        catalog = ""
        if self.schemas or self.schemas_nodata:
            catalog = str(
                self.get_catalog(filename, excluding_tables, out_to_file=True)
            )

            cmd += ["-L", catalog]

        cmd += [filename]

        # now filter out empty array elements in order to prepare a command
        # without extra spacing
        cmd = [x for x in cmd if x is not None and x != ""]

        logger.info(" ".join(cmd))

        # try to connect with a safe timeout, raise an exception when failing
        self.try_connection()

        # mesure pg_restore timing
        import time

        start_time = time.time()

        # utils.run_command will raise a SubprocessException if pg_restore
        # returns an error code (non zero)
        out = utils.run_command(cmd, returning=utils.RET_OUT)

        end_time = time.time()

        # time elapsed, in secs
        return end_time - start_time

    def get_catalog(self, filename, tables, out_to_file=False):
        """ return the backup catalog, pg_restore -l, commenting table data """

        cmd = [self.restore_cmd, "-l", filename]
        out = utils.run_command(cmd, returning=utils.RET_OUT)

        from io import StringIO

        catalog = StringIO()

        # out is a simple string, so we split on \n or read one char at a
        # time in the loop, which isn't what we want
        #
        # here's what the DATA lines we're after look like:
        #
        # 3; 2615 122814 SCHEMA - pgq postgres
        # 6893; 0 0 ACL - pgq postgres
        # 3385; 1259 123008 TABLE londiste subscriber_table payment
        # 1206; 1247 123043 TYPE londiste ret_subscriber_table postgres
        # 1118; 1247 122925 TYPE pgq ret_batch_event postgres
        # 142; 1255 122813 FUNCTION public txid_visible_in_snapshot(bigint, txid_snapshot) postgres
        # 70; 1255 1487229 FUNCTION public upper(ip4r) postgres
        # 2526; 2617 1487283 OPERATOR public # postgres
        # 2524; 2617 1487281 OPERATOR public & postgres
        # 2647; 2616 1487309 OPERATOR CLASS public btree_ip4_ops postgres
        # 3961; 2605 1487223 CAST pg_catalog CAST (cidr AS public.ip4r)
        # 6662; 0 788811 TABLE DATA payment abocb_code payment
        # 6663; 0 788819 TABLE DATA payment abocb_renew payment
        # 6664; 0 788825 TABLE DATA payment acte_code payment
        # 3380; 1259 122980 SEQUENCE londiste provider_seq_nr_seq payment
        # 6904; 0 0 SEQUENCE OWNED BY londiste provider_seq_nr_seq payment
        # 6905; 0 0 SEQUENCE SET londiste provider_seq_nr_seq payment
        # 4301; 2604 122984 DEFAULT londiste nr payment
        # 4656; 1259 56340 INDEX archives ap_rev_2004 webadmin
        # 6236; 2620 15995620 TRIGGER jdb www_to_reporting_logger webadmin
        # 6014; 2606 56535 FK CONSTRAINT archives rev_2001_id_compte_fkey webadmin

        # tables are schema.table, we want (schema, table)
        splitted_tables = []
        if tables:
            splitted_tables = [(x.split(".")[0], x.split(".")[1]) for x in tables]

        # for meta data (md_) commands, filter_out what's neither in schemas
        # nor in schemas_nodata
        schemas = self.schemas
        if schemas is None:
            schemas = []

        md_schemas = schemas
        if self.schemas_nodata:
            md_schemas += self.schemas_nodata

        # schemas here are used to filter what to restore (values not in
        # self.schemas are filtered out)
        if md_schemas:
            md_schemas.append("pg_catalog")

        # which triggers calls which function (schema qualified) cache
        triggers = self.get_trigger_funcs(filename)

        for line in out.split("\n"):
            if line.strip() == "":
                continue

            filter_out = False

            if (
                line.find("SCHEMA") > -1
                or line.find("ACL") > -1
                or line.find("TABLE") > -1
                or line.find("TYPE") > -1
                or line.find("FUNCTION") > -1
                or line.find("OPERATOR") > -1
                or line.find("CAST") > -1
                or line.find("TABLE DATA") > -1
                or line.find("SEQUENCE") > -1
                or line.find("VIEW") > -1
                or line.find("COMMENT") > -1
                or line.find("DEFAULT") > -1
                or line.find("INDEX") > -1
                or line.find("TRIGGER") > -1
                or line.find("DOMAIN") > -1
                or line.find("CONSTRAINT") > -1
            ):

                try:
                    a, b, c, d = line.split()[3:7]
                    schema = ""
                    if a in ("ACL", "SCHEMA"):
                        if b == "-":
                            schema = c
                        else:
                            schema = b

                    elif a == "COMMENT":
                        if b == "-" and c == "SCHEMA":
                            schema = d

                    elif b == "CLASS":
                        schema = c

                    elif b == "DATA":
                        schema = c
                        table = d

                    elif a == "SEQUENCE":
                        if b == "OWNED" and c == "BY":
                            schema = d
                        elif b == "SET":
                            schema = c
                        else:
                            schema = b

                    elif a == "FK" and b == "CONSTRAINT":
                        schema = c

                    else:
                        schema = b

                    # filter out ACL lines for schemas we want to exclude
                    if a == "ACL" and b == "-" and c not in schemas:
                        filter_out = True

                    # check schemas (contains data we want to restore)
                    if not filter_out and schema not in md_schemas:
                        filter_out = True

                    # check TRIGGER function dependancy
                    if not filter_out and a == "TRIGGER":
                        # triggers[schema][trigger_name] = [f1, f2, f3]
                        if b in triggers and c in triggers[b]:
                            for f in triggers[b][c]:
                                s = f.split(".")[0]

                                if s not in schemas:
                                    filter_out = True
                                    break

                    # filter out TABLE DATA section for schemas_nodata
                    if (
                        not filter_out
                        and self.schemas_nodata is not None
                        and a == "TABLE"
                        and b == "DATA"
                        and schema in self.schemas_nodata
                    ):
                        filter_out = True

                    # then additional tables given by caller
                    if not filter_out and a == "TABLE" and b == "DATA":
                        for s, t in splitted_tables:
                            if not filter_out and schema == s and table == t:
                                filter_out = True

                    # then tables filtered out by regexp in the config
                    if not filter_out and a == "TABLE" and b == "DATA":
                        qualified_relname = f"{schema}.{table}"
                        for regexp in self.relname_nodata:
                            if re.search(regexp, qualified_relname):
                                filter_out = True
                                break

                except ValueError:
                    # unpack error, line won't match anything, don't filter
                    # out
                    pass

            # filter_out means we turn it into a comment
            if filter_out:
                catalog.write(f";{line}\n")
            else:
                catalog.write(f"{line}\n")

        # chop last \n
        if "SEEK_CUR" in os.__dict__:
            whence = os.SEEK_CUR
        else:
            whence = 1

        catalog.seek(-1, whence)
        catalog.truncate()

        if not out_to_file:
            return catalog

        import tempfile

        fd, realname = tempfile.mkstemp(prefix="/tmp/staging.", suffix=".catalog")

        temp = os.fdopen(fd, "wb")
        temp.write(catalog.getvalue())
        temp.close()

        return realname

    ##
    # In the catalog, we have such TRIGGER lines:
    #
    # 6236; 2620 15995620 TRIGGER jdb www_to_reporting_logger webadmin
    #
    # The TRIGGER code could depend on a procedure hosted in a schema that
    # we filter out. In this case, we want to also filter out the TRIGGER
    # itself.
    #
    # CREATE TRIGGER www_to_reporting_logger
    # AFTER INSERT OR DELETE OR UPDATE ON daily_journal
    # FOR EACH ROW
    # EXECUTE PROCEDURE pgq.logtriga('www_to_reporting', 'kkvvvvvvvvv', 'jdb.daily_journal');
    #
    # get_trigger_funcs will return a dict of
    #  {'trigger_name': ['procedure']}

    def get_trigger_funcs(self, filename):
        """ return the backup catalog, pg_restore -l, commenting table data """

        cmd = [self.restore_cmd, "-s", filename]
        out = utils.run_command(cmd, returning=utils.RET_OUT)

        # expressions we're searching
        set_search_path = "SET search_path = "
        set_search_path_l = len(set_search_path)
        create_trigger = "CREATE TRIGGER"
        create_trigger_l = len(create_trigger)
        execute_procedure = "EXECUTE PROCEDURE"
        execute_procedure_l = len(execute_procedure)
        returns_trigger = 'RETURNS "trigger"'

        # parsing state and results
        triggers = {}
        triggers_funcs = {}  # {func: schema} cache
        current_schema = "public"
        current_trigger = None

        for line in out.split("\n"):
            if line.find(set_search_path) > -1:
                current_schema = line[set_search_path_l:-1].split(", ")[0]

                if current_schema not in triggers:
                    triggers[current_schema] = {}

                # no need to search for CREATE TRIGGER here
                continue

            if line.find(create_trigger) > -1:
                current_trigger = line[create_trigger_l:].strip().split(" ")[0]

                if current_trigger not in triggers[current_schema]:
                    # add an empty procedures list
                    triggers[current_schema][current_trigger] = []

            if line.find(returns_trigger) > -1:
                # CREATE FUNCTION partition_board_log() RETURNS "trigger"
                pname = line.split()[2].strip("()")
                triggers_funcs[pname] = current_schema

            if current_trigger:
                start = line.find(execute_procedure)

                if start > -1:
                    start = start + execute_procedure_l
                    pname = line[start: line.find("(", start)].strip()

                    if pname.find(".") == -1:
                        # procedure name is NOT schema qualified
                        ## if pname in triggers_funcs:
                        ##     pname = '%s.%s' % (triggers_funcs[pname], pname)
                        ## else:
                        pname = f"{current_schema}.{pname}"

                    if pname not in triggers[current_schema][current_trigger]:
                        triggers[current_schema][current_trigger].append(pname)

                if line.find(";") > -1:
                    current_trigger = None

        return triggers

    def dbsize(self):
        """ return pretty printed dbsize """

        sql = (
            f"SELECT pg_database_size('{self.dbname}'), "
            + f"pg_size_pretty(pg_database_size('{self.dbname}'))"
        )

        logger.info(sql)

        try:
            curs = self.mconn.cursor()
            curs.execute(sql)
            dbsize, dbsize_pretty = curs.fetchone()
            dbsize = int(dbsize)
            curs.close()
        except Exception:
            raise

        return dbsize, dbsize_pretty

    def pg_size_pretty(self, size):
        """ return pretty printed dbsize """

        sql = f"SELECT pg_size_pretty({size}::bigint)"

        logger.info(sql)

        try:
            curs = self.mconn.cursor()
            curs.execute(sql)
            dbsize_pretty = curs.fetchone()[0]
            curs.close()
        except Exception:
            raise

        return dbsize_pretty

    def show(self, setting):
        """ return pretty printed dbsize """

        sql = f"SHOW {setting}"

        logger.info(sql)

        try:
            curs = self.mconn.cursor()
            curs.execute(sql)
            value = curs.fetchone()[0]
            curs.close()
        except Exception:
            raise

        return value

    def set_database_search_path(self, search_path):
        """ ALTER DATABASE self.dbname SET search_path TO ... """

        try:
            sp = ", ".join(search_path)
            sql = f"ALTER DATABASE {self.dbname} SET search_path TO {sp};"

            logger.info(sql)

            curs = self.mconn.cursor()
            curs.execute(sql)
            self.mconn.commit()
            curs.close()
        except Exception as exp:
            logger.error(exp)
            raise

    def psql_source_file(self, filename=None):
        """ launch psql and connect to given database """

        if filename:
            dash_f = f" -f {filename} "
        else:
            dash_f = ""

        cmd = "%s %s -U %s -h %s -p %d %s" % (
            self.restore_cmd.replace("pg_restore", "psql"),
            dash_f,
            self.user,
            self.host,
            self.port,
            self.dbname,
        )

        logger.info(cmd)

        if filename is None:
            return os.system(cmd)
        else:
            return utils.run_command(cmd, returning=utils.RET_OUT)

    def pg_dump(self, filename, fmt="-Fc", force=False):
        """ pg_dump to filename, formating to -Fc by default """

        cmd = "%s %s -U %s -h %s -p %d %s" % (
            self.restore_cmd.replace("pg_restore", "pg_dump"),
            fmt,
            self.user,
            self.host,
            self.port,
            self.dbname,
        )

        logger.info(f"{cmd} > {filename}")

        # try to connect with a safe timeout, raise an exception when failing
        self.try_connection()

        if not force and os.path.exists(filename):
            raise ExportFileAlreadyExistsException

        f = open(filename, "wb", BUFSIZE)

        # mesure pg_dump timing
        import time

        start_time = time.time()

        # utils.run_command will raise a SubprocessException if pg_restore
        # returns an error code (non zero)
        out = utils.run_command(cmd, stdout=f)
        f.close()

        end_time = time.time()

        # time elapsed, in secs
        return end_time - start_time

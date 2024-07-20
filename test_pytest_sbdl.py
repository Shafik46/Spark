import pytest
from datetime import date, datetime, timedelta
from datetime import datetime, date
import chispa
from chispa import assert_df_equality,assert_basic_rows_equality, DataFramesNotEqualError
from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row
from prettytable import PrettyTable
from pyspark.sql.functions import col
from lib import DataLoader, Transformation
from lib.ConfigLoader import get_config
from lib.DataLoader import get_party_schema
from lib.Utils import get_spark_session


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")


@pytest.fixture(scope='session')
def expected_party_rows():
    # Define the offset for 4 hours and 30 minutes
    offset_430 = timedelta(hours=4, minutes=30)
    # Define the offset for 5 hours and 30 minutes
    offset_530 = timedelta(hours=5, minutes=30)

    # List of rows with adjusted datetimes
    return [
        Row(load_date=date(2022, 8, 2), account_id='6982391060', party_id='9823462810', relation_type='F-N',
            relation_start_date=datetime(2019, 7, 29, 6, 21, 32) - offset_430),
        Row(load_date=date(2022, 8, 2), account_id='6982391061', party_id='9823462811', relation_type='F-N',
            relation_start_date=datetime(2018, 8, 31, 5, 27, 22) - offset_430),
        Row(load_date=date(2022, 8, 2), account_id='6982391062', party_id='9823462812', relation_type='F-N',
            relation_start_date=datetime(2018, 8, 25, 15, 50, 29) - offset_430),
        Row(load_date=date(2022, 8, 2), account_id='6982391063', party_id='9823462813', relation_type='F-N',
            relation_start_date=datetime(2018, 5, 11, 7, 23, 28) - offset_430),
        Row(load_date=date(2022, 8, 2), account_id='6982391064', party_id='9823462814', relation_type='F-N',
            relation_start_date=datetime(2019, 6, 6, 14, 18, 12) - offset_430),
        Row(load_date=date(2022, 8, 2), account_id='6982391065', party_id='9823462815', relation_type='F-N',
            relation_start_date=datetime(2019, 5, 4, 5, 12, 37) - offset_430),
        Row(load_date=date(2022, 8, 2), account_id='6982391066', party_id='9823462816', relation_type='F-N',
            relation_start_date=datetime(2019, 5, 15, 10, 39, 29) - offset_430),
        Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462817', relation_type='F-N',
            relation_start_date=datetime(2018, 5, 16, 9, 53, 4) - offset_430),
        Row(load_date=date(2022, 8, 2), account_id='6982391068', party_id='9823462818', relation_type='F-N',
            relation_start_date=datetime(2017, 11, 27, 1, 20, 12) - offset_530),
        Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462820', relation_type='F-S',
            relation_start_date=datetime(2017, 11, 20, 14, 18, 5) - offset_530),
        Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462821', relation_type='F-S',
            relation_start_date=datetime(2018, 7, 19, 18, 56, 57) - offset_430)
    ]


@pytest.fixture(scope='session')
def parties_list():
    return [
        (date(2022, 8, 2), '6982391060', '9823462810', 'F-N', datetime(2019, 7, 29, 1, 51, 32)),
        (date(2022, 8, 2), '6982391061', '9823462811', 'F-N', datetime(2018, 8, 31, 0, 57, 22)),
        (date(2022, 8, 2), '6982391062', '9823462812', 'F-N', datetime(2018, 8, 25, 11, 20, 29)),
        (date(2022, 8, 2), '6982391063', '9823462813', 'F-N', datetime(2018, 5, 11, 2, 53, 28)),
        (date(2022, 8, 2), '6982391064', '9823462814', 'F-N', datetime(2019, 6, 6, 9, 48, 12)),
        (date(2022, 8, 2), '6982391065', '9823462815', 'F-N', datetime(2019, 5, 4, 0, 42, 37)),
        (date(2022, 8, 2), '6982391066', '9823462816', 'F-N', datetime(2019, 5, 15, 6, 9, 29)),
        (date(2022, 8, 2), '6982391067', '9823462817', 'F-N', datetime(2018, 5, 16, 5, 23, 4)),
        (date(2022, 8, 2), '6982391068', '9823462818', 'F-N', datetime(2017, 11, 26, 19, 50, 12)),
        (date(2022, 8, 2), '6982391067', '9823462820', 'F-S', datetime(2017, 11, 20, 8, 48, 5)),
        (date(2022, 8, 2), '6982391067', '9823462821', 'F-S', datetime(2018, 7, 19, 14, 26, 57))
    ]


@pytest.fixture(scope='session')
def expected_contract_df(spark):
    schema = StructType([StructField('account_id', StringType()),
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             ArrayType(StructType(
                                                                 [StructField('contractTitleLineType', StringType()),
                                                                  StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())]))])

    return spark.read.format("json").schema(schema).load("test_data/results/contract_df.json")


@pytest.fixture(scope='session')
def expected_final_df(spark):
    schema = StructType([
        StructField('keys', ArrayType(StructType([
            StructField('keyField', StringType(), False),
            StructField('keyValue', StringType(), False)
        ]), False), False),
        StructField('payload', StructType([
            StructField('contractIdentifier', StructType([
                StructField('operation', StringType(), False),
                StructField('newValue', StringType(), True),
                StructField('oldValue', NullType(), True)
            ]), False),
            StructField('sourceSystemIdentifier', StructType([
                StructField('operation', StringType(), False),
                StructField('newValue', StringType(), True),
                StructField('oldValue', NullType(), True)
            ]), False),
            StructField('contactStartDateTime', StructType([
                StructField('operation', StringType(), False),
                StructField('newValue', TimestampType(), True),
                StructField('oldValue', NullType(), True)
            ]), False),
            StructField('contractTitle', StructType([
                StructField('operation', StringType(), False),
                StructField('newValue', ArrayType(StructType([
                    StructField('contractTitleLineType', StringType(), False),
                    StructField('contractTitleLine', StringType(), True)
                ]), True), False),
                StructField('oldValue', NullType(), True)
            ]), False),
            StructField('taxIdentifier', StructType([
                StructField('operation', StringType(), False),
                StructField('newValue', StructType([
                    StructField('taxIdType', StringType(), True),
                    StructField('taxId', StringType(), True)
                ]), False),
                StructField('oldValue', NullType(), True)
            ]), False),
            StructField('contractBranchCode', StructType([
                StructField('operation', StringType(), False),
                StructField('newValue', StringType(), True),
                StructField('oldValue', NullType(), True)
            ]), False),
            StructField('contractCountry', StructType([
                StructField('operation', StringType(), False),
                StructField('newValue', StringType(), True),
                StructField('oldValue', NullType(), True)
            ]), False),
            StructField('partyRelations', ArrayType(StructType([
                StructField('partyIdentifier', StructType([
                    StructField('operation', StringType(), False),
                    StructField('newValue', StringType(), True),
                    StructField('oldValue', NullType(), True)
                ]), False),
                StructField('partyRelationshipType', StructType([
                    StructField('operation', StringType(), False),
                    StructField('newValue', StringType(), True),
                    StructField('oldValue', NullType(), True)
                ]), False),
                StructField('partyRelationStartDateTime', StructType([
                    StructField('operation', StringType(), False),
                    StructField('newValue', TimestampType(), True),
                    StructField('oldValue', NullType(), True)
                ]), False),
                StructField('partyAddress', StructType([
                    StructField('operation', StringType(), False),
                    StructField('newValue', StructType([
                        StructField('addressLine1', StringType(), True),
                        StructField('addressLine2', StringType(), True),
                        StructField('addressCity', StringType(), True),
                        StructField('addressPostalCode', StringType(), True),
                        StructField('addressCountry', StringType(), True),
                        StructField('addressStartDate', DateType(), True)
                    ]), False),
                    StructField('oldValue', NullType(), True)
                ]), True)
            ]), False), True)
        ]), False)
    ])
    return spark.read.format("json").schema(schema).load("test_data/results/final_df.json").select("keys", "payload")
def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.5.1"


def test_get_config():
    conf_local = get_config("LOCAL")
    conf_qa = get_config("QA")
    assert conf_local["kafka.topic"] == "sbdl_kafka_cloud"
    assert conf_qa["hive.database"] == "sbdl_db_qa"


def test_read_accounts(spark):
    accounts_df = DataLoader.read_accounts(spark, "LOCAL", False, None)
    assert accounts_df.count() == 8

def test_read_parties_row(spark, expected_party_rows):
    actual_party_rows = DataLoader.read_parties(spark, "LOCAL", False, None).collect()
    assert expected_party_rows == actual_party_rows

def test_read_party_schema(spark, parties_list):
    expected_df = spark.createDataFrame(parties_list, get_party_schema())
    actual_df = DataLoader.read_parties(spark, "LOCAL", False, None)
    assert_df_equality(expected_df, actual_df)

def test_get_contract(spark, expected_contract_df):
    accounts_df = DataLoader.read_accounts(spark, "LOCAL", False, None)
    actual_contract_df = Transformation.get_contract(accounts_df)
    expected_data = [row.asDict() for row in expected_contract_df.collect()]
    actual_data = [row.asDict() for row in actual_contract_df.collect()]

    # Sort the data to ensure order doesn't affect the comparison
    expected_data.sort(key=lambda row: sorted(row.items()))
    actual_data.sort(key=lambda row: sorted(row.items()))

    # Compare the data
    assert expected_data == actual_data



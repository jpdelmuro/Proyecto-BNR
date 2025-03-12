#!/usr/bin/env python3
import datetime
import logging
import random
import uuid

import time_uuid
from cassandra.query import BatchStatement

# Set logger
log = logging.getLogger()

CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""
# CREACION DE LAS TABLAS (ESTAS SON PROVISIONALES)

CREATE_STUDENTS_ACTIVITY = """
    CREATE TABLE student_activity (
        student_id UUID,
        course_id UUID,
        activity_id UUID,
        tipo_actividad TEXT,
        activity_timestamp TIMESTAMP,
        detalles TEXT,
        PRIMARY KEY ((student_id, course_id), activity_timestamp, activity_id)
    ) WITH CLUSTERING ORDER BY (activity_timestamp DESC);
"""

STUDENT_PROGRESS = """
    CREATE TABLE student_progress (
        student_id UUID,
        course_id UUID,
        progress_id UUID,
        porcentaje_completado FLOAT,
        calificaciones INT,
        PRIMARY KEY ((student_id, course_id), progress_id)
    );
"""

SYSTEM_NOTIFICATIONS = """
    CREATE TABLE system_notifications (
        user_id UUID,
        notification_id UUID,
        course_id UUID,
        tipo TEXT,
        notificacion TEXT,
        notification_timestamp TIMESTAMP,
        PRIMARY KEY ((user_id, course_id), notification_timestamp, notification_id)
    ) WITH CLUSTERING ORDER BY (notification_timestamp DESC);
"""


USER_SESSIONS = """
    CREATE TABLE user_sessions (
        user_id UUID,
        session_id UUID,
        start_time TIMESTAMP,
        last_activity TIMESTAMP,
        device_info TEXT,
        active_status BOOLEAN,
        PRIMARY KEY (user_id, session_id)
    ) WITH CLUSTERING ORDER BY (last_activity DESC);
"""


STUDENT_CERTIFICATES = """
    CREATE TABLE student_certificates (
        student_id UUID,
        course_id UUID,
        certificate_id UUID,
        student_name TEXT,
        course_title TEXT,
        completion_date TIMESTAMP,
        certificate_url TEXT,
        PRIMARY KEY ((student_id, course_id), certificate_id)
    );
"""



# EJEMPLOS DE LOS QUERYS
# Variable Q1
SELECT_USER_ACCOUNTS = """
    SELECT username, account_number, name, cash_balance
    FROM accounts_by_user
    WHERE username = ?
"""

# Variable Q2
POSITIONS_BY_ACCOUNT = """
    SELECT symbol, quantity
    FROM positions_by_account
    WHERE account = ?
"""



USERS = [
    ('mike', 'Michael Jones'),
    ('stacy', 'Stacy Malibu'),
    ('john', 'John Doe'),
    ('marie', 'Marie Condo'),
    ('tom', 'Tomas Train')
]
INSTRUMENTS = [
    'ETSY', 'PINS', 'SE', 'SHOP', 'SQ', 'MELI', 'ISRG', 'DIS', 'BRK.A', 'AMZN',
    'VOO', 'VEA', 'VGT', 'VIG', 'MBB', 'QQQ', 'SPY', 'BSV', 'BND', 'MUB',
    'VSMPX', 'VFIAX', 'FXAIX', 'VTSAX', 'SPAXX', 'VMFXX', 'FDRXX', 'FGXX'
]

def execute_batch(session, stmt, data):
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i+batch_size]:
            batch.add(stmt, item)
        session.execute(batch)
    session.execute(batch)


def bulk_insert(session):
    acc_stmt = session.prepare("INSERT INTO accounts_by_user (username, account_number, cash_balance, name) VALUES (?, ?, ?, ?)")
    pos_stmt = session.prepare("INSERT INTO positions_by_account(account, symbol, quantity) VALUES (?, ?, ?)")
    tad_stmt = session.prepare("INSERT INTO trades_by_a_d (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tatd_stmt = session.prepare("INSERT INTO trades_by_a_td (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tastd_stmt = session.prepare("INSERT INTO trades_by_a_std (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tasd_stmt = session.prepare("INSERT INTO trades_by_a_sd (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    
    accounts = []

    accounts_num=10
    positions_by_account=100
    trades_by_account=1000
   
    # Generate accounts by user
    data = []
    for i in range(accounts_num):
        user = random.choice(USERS)
        account_number = str(uuid.uuid4())
        accounts.append(account_number)
        cash_balance = random.uniform(0.1, 100000.0)
        data.append((user[0], account_number, cash_balance, user[1]))
    execute_batch(session, acc_stmt, data)
    
   
    # Genetate possitions by account
    acc_sym = {}
    data = []
    for i in range(positions_by_account):
        while True:
            acc = random.choice(accounts)
            sym = random.choice(INSTRUMENTS)
            if acc+'_'+sym not in acc_sym:
                acc_sym[acc+'_'+sym] = True
                quantity = random.randint(1, 500)
                data.append((acc, sym, quantity))
                break
    execute_batch(session, pos_stmt, data)

    # Generate trades by account
    data = []
    for i in range(trades_by_account):
        trade_id = random_date(datetime.datetime(2013, 1, 1), datetime.datetime(2022, 8, 31))
        acc = random.choice(accounts)
        sym = random.choice(INSTRUMENTS)
        trade_type = random.choice(['buy', 'sell'])
        shares = random.randint(1, 5000)
        price = random.uniform(0.1, 100000.0)
        amount = shares * price
        data.append((acc, trade_id, trade_type, sym, shares, price, amount))
    execute_batch(session, tad_stmt, data)
    execute_batch(session, tatd_stmt, data)
    execute_batch(session, tastd_stmt, data)
    execute_batch(session, tasd_stmt, data)


def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(rand_date))


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_USERS_TABLE)
    session.execute(CREATE_POSSITIONS_BY_ACCOUNT_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_A_TD_TABLE)
    session.execute(CREATE_TRADES_BY_A_STD_TABLE)
    session.execute(CREATE_TRADES_BY_A_SD_TABLE)

# Este es el Q1
def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    for row in rows:
        print(f"=== Account: {row.account_number} ===")
        print(f"- Cash Balance: {row.cash_balance}")

# Este es el Q2
def get_positions_by_account(session, account):
    log.info(f"Retrieving positions for account: {account}")
    stmt = session.prepare(POSITIONS_BY_ACCOUNT)
    rows = session.execute(stmt, [account])
    for row in rows:
        print(f"Symbol: {row.symbol}, Quantity: {row.quantity}")

# Este es el Q3.1
def get_trades_by_account(session, account):
    log.info(f"Retrieving trades for account: {account}")
    stmt = session.prepare(TRADES_FOR_ACCOUNT)
    rows = session.execute(stmt, [account])
    for row in rows:
        print(f"Trade Date: {row.trade_date}, Type: {row.type}, Symbol: {row.symbol}")
        print(f"Shares: {row.shares}, Price: {row.price}, Amount: {row.amount}\n")


# Este es el Q3.2 AAAAAAAAAAAAAAA
def get_trades_by_account_in_range(session, account, start_date):
    log.info(f"Retrieving trades for account: {account} from {start_date}")
    start_timeuuid = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(start_date))
    stmt = session.prepare(TRADES_FOR_ACCOUNT_IN_DATE)
    rows = session.execute(stmt, [account, start_timeuuid])
    for row in rows:
        print(f"Trade Date: {row.trade_date}, Type: {row.type}, Symbol: {row.symbol}")
        print(f"Shares: {row.shares}, Price: {row.price}, Amount: {row.amount}\n")

# Este es el Q3.3
def get_trades_by_account_type_and_range(session, account, trade_type, start_date, end_date):
    log.info(f"Retrieving '{trade_type}' trades for account: {account} from {start_date} to {end_date}")
    start_timeuuid = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(start_date))
    end_timeuuid = time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(end_date))
    stmt = session.prepare(TRADES_FOR_ACCOUNT_IN_DATE_AND_TRANS)
    rows = session.execute(stmt, [account, trade_type, start_timeuuid, end_timeuuid])
    for row in rows:
        print(f"Trade Date: {row.trade_date}, Type: {row.type}, Symbol: {row.symbol}")
        print(f"Shares: {row.shares}, Price: {row.price}, Amount: {row.amount}\n")

# Este es el Q3.4
def get_trades_by_filters(session, account, trade_type, symbol, start_date, end_date):
    log.info(f"Retrieving '{trade_type}' trades for account: {account}, symbol: {symbol}, from {start_date} to {end_date}")
    stmt = session.prepare(TRADES_BY_ACCOUNT_W_TYPE_SYMBOL)
    rows = session.execute(stmt, [account, trade_type, symbol, start_date, end_date])
    for row in rows:
        print(f"Trade Date: {row.trade_date}, Type: {row.type}, Symbol: {row.symbol}")
        print(f"Shares: {row.shares}, Price: {row.price}, Amount: {row.amount}\n")

# Este es el Q3.5
def get_trades_by_symbol_and_date(session, account, symbol, start_date, end_date):
    log.info(f"Retrieving trades for account: {account}, symbol: {symbol}, from {start_date} to {end_date}")
    stmt = session.prepare(queryq5)
    rows = session.execute(stmt, [account, symbol, start_date, end_date])
    for row in rows:
        print(f"Trade Date: {row.trade_date}, Type: {row.type}, Symbol: {row.symbol}")
        print(f"Shares: {row.shares}, Price: {row.price}, Amount: {row.amount}\n")

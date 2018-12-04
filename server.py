from flask import Flask, render_template, request, redirect
from flask_bootstrap import Bootstrap
import pandas as pd
import pyodbc
from logger import get_logger
import sys
import signal

app = Flask(__name__)
Bootstrap(app)
app.config.from_pyfile('config.ini', silent=True)
log_name = app.config['CM_LOGGING_NAME']
log_file = app.config['CM_LOGFILE']
log_lvl = app.config['CM_LOGGING_LEVEL']
app_logger = get_logger(log_name, log_file, log_lvl, app)


class SqlDataClient:
    def __init__(self):
        self.connect = pyodbc.connect(
            Trusted_Connection=app.config['CM_TRUSTED_CONNECTION'],
            DRIVER=app.config['CM_DRIVER'],
            SERVER=app.config['CM_SERVER_SQL'],
            DATABASE=app.config['CM_DATABASE'])

    def get_data(self):
        query = "with cards as (SELECT AddData1 FROM [PaymentsFraud].[dbo].[Payments] " \
                "where AddData1 not in " \
                "(select [CardNumber] collate Cyrillic_General_CI_AS from [PaymentsFraud].[dbo].[Deleted_cards]) " \
                "group by AddData1 having count(distinct NNDeposit) > 1) " \
                "Select DatePayment, concat(cast(d.Account as varchar(max)), ': ', d.lName, ' ', d.fName) Account, " \
                "p.AddData1 CardNumber, coalesce(d.NameStatus, 'Non status') [Status], " \
                "p.Summa Amount, d.NameValuta Currency, " \
                "d.Country Country, case when p.Summa > 0 then 'INPUT' else 'OUTPUT' end [Transaction] " \
                "FROM [PaymentsFraud].[dbo].[Payments] p join [dbo].[Deposits] d on p.NNDeposit = d.NNDeposit " \
                "where AddData1 in (select * from cards) and p.DatePayment = (select max(DatePayment) " \
                "from [dbo].[Payments] p1 where p1.AddData1 = p.AddData1 group by p1.AddData1)" \
                "order by DatePayment desc"
        data = pd.read_sql(query, self.connect)
        return data

    def get_account(self, card: str):
        query = "with pay as (select distinct NNDeposit from [PaymentsFraud].[dbo].[Payments] " \
                "where AddData1 = ?) " \
                "select concat(cast(d.Account as varchar(max)), ': ', d.lName, ' ', d.fName) Account, " \
                "d.NameStatus [Status], count(distinct b.BetID) Qty_Bets, count(distinct p.NNPayment) QTransactions, " \
                "RestOfSum Balance, Country Country, d.Town City, NameValuta Currency, d.Account IdAccount, " \
                "coalesce(sum(b.Stake), 0) Amount, coalesce(sum(b.Profit), 0) Profit, " \
                "cast((sum(b.Profit) / sum(b.Stake) * 100) as varchar(max)) + '%' Margin " \
                "FROM pay left join [dbo].[Deposits] d on pay.NNDeposit = d.NNDeposit left join [dbo].[Bets] b " \
                "on d.Account = b.Account left join [dbo].[Payments] p on pay.NNDeposit = p.NNDeposit " \
                "group by d.Account, concat(cast(d.Account as varchar(max)), ': ', d.lName, ' ', d.fName), " \
                "d.NameStatus, RestOfSum, Country, d.Town, NameValuta"
        account = pd.read_sql(query, self.connect, params=[card])
        return account

    def delete_wallet(self, del_val):
        cursor = self.connect.cursor()
        cursor.execute("insert into [PaymentsFraud].[dbo].[Deleted_cards](CardNumber) values(?)", del_val)
        self.connect.commit()


dataClient = SqlDataClient()


@app.route('/wallets', methods=['GET', 'Post'])
def get_table():
    if request.method == 'GET':
        app_logger.info('GET /wallets')
        app_logger.debug('GET /wallets')

        if request.args:
            prm = request.args.get('card')
            app_logger.debug('GET /wallets/card=%s', prm)
            app_logger.info('GET /wallets/card=%s', prm)
            return render_template("accounts.html", data=dataClient.get_account(prm))

        return render_template("wallets.html", data=dataClient.get_data())

    if request.method == 'POST':
        deleted_wallet = request.form.get('delete')

        if deleted_wallet is not None:
            dataClient.delete_wallet(deleted_wallet)
            app_logger.debug('POST Deleted %s', deleted_wallet)
            app_logger.info('POST Deleted %s', deleted_wallet)
            return render_template("wallets.html", data=dataClient.get_data())

        else:
            app_logger.warning(r"POST Didn't get card number")


@app.errorhandler(404)
def page_not_found(error):
    app_logger.warning('Page not found %s', request.path)
    return render_template("404.html")


@app.route('/')
def redirection_from_external():
    return redirect("/wallets", code=302)


def exit_gracefully(signum, frame):
    def raw_input(my_raw):
        return input(my_raw)

    signal.signal(signal.SIGINT, original_sigint)
    try:
        if raw_input(r"Really quit? (y/n): ").lower().startswith('y'):
            sys.exit(1)

    except KeyboardInterrupt:
        print("Ok ok, quitting")
        sys.exit(1)

    signal.signal(signal.SIGINT, exit_gracefully)


if __name__ == '__main__':
    app_logger.debug('Debugger is Activate')
    app_logger.info('Running on %s:%s', app.config['CM_SERVER_HOST'], app.config['CM_SERVER_PORT'])
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)
    app.run()

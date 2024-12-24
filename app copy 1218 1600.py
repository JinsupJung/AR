from flask import Flask, request, render_template, redirect, url_for, flash, jsonify
from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, SubmitField, DecimalField, DateField, FileField
from wtforms.validators import DataRequired, NumberRange
from werkzeug.utils import secure_filename
import pandas as pd
import mysql.connector
import logging 
import os
from datetime import datetime
from flask_wtf.csrf import CSRFProtect
import json  # JSON 처리를 위해 추가
from decimal import Decimal  # Decimal 처리를 위해 추가
import re

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # CSRF 보호를 위한 비밀 키 설정
       
# CSRF 보호 설정
csrf = CSRFProtect(app)

# 파일 업로드 설정
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 최대 업로드 크기 설정 (16MB)

# 로그 설정
today = datetime.now().strftime("%Y%m%d")
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_filename = os.path.join(log_dir, f'app_{today}.log')

logging.basicConfig(
    level=logging.DEBUG,  # 로깅 레벨을 DEBUG로 설정
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# MySQL 연결 설정 함수
def get_db_connection():
    try:
        db = mysql.connector.connect(
            host=os.environ.get('DB_HOST', '175.196.7.45'),
            user=os.environ.get('DB_USER', 'nolboo'),
            password=os.environ.get('DB_PASSWORD', '2024!puser'),
            database=os.environ.get('DB_NAME', 'nolboo'),
            charset='utf8mb4'
        )
        logging.info("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
        return db
    except mysql.connector.Error as err:
        logging.error(f"MySQL 연결 오류: {err}")
        return None

# 헬퍼 함수: 파일 확장자 확인
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# 폼 정의
class UploadForm(FlaskForm):
    file = FileField('Upload Excel File', validators=[DataRequired()])
    submit = SubmitField('Upload')

class AddOrderForm(FlaskForm):
    client_code = SelectField('매출처', choices=[], validators=[DataRequired()])
    representative_code = StringField('대표코드', render_kw={'readonly': True})
    order_date = DateField('발주일', format='%Y-%m-%d', validators=[DataRequired()])
    amount = DecimalField('금액', validators=[DataRequired(), NumberRange(min=0)])
    submit = SubmitField('발주저장')

class BulkUploadOrdersForm(FlaskForm):
    file = FileField('발주 내역 엑셀 업로드', validators=[DataRequired()])
    submit = SubmitField('업로드')

# 새로운 폼 클래스 추가 (DailyTransactionsForm)
class DailyTransactionsForm(FlaskForm):
    current_year = datetime.now().year
    years = [(str(year), str(year)) for year in range(current_year - 5, current_year + 1)]
    months = [(str(month).zfill(2), str(month)) for month in range(1, 13)]
    
    year = SelectField('연도', choices=years, validators=[DataRequired()])
    month = SelectField('월', choices=months, validators=[DataRequired()])
    submit = SubmitField('조회')    

# 메인 페이지
@app.route('/')
def index():
    return render_template('index.html')

# 발주 내역 추가
@app.route('/add_order', methods=['GET', 'POST'])
def add_order():
    form = AddOrderForm()
    
    db = get_db_connection()
    if not db:
        flash('Database connection failed.', 'danger')
        return render_template('add_order.html', form=form)

    try:
        with db.cursor(dictionary=True) as cursor:
            # 거래처 목록을 동적으로 가져와 폼의 선택지에 추가
            try:
                cursor.execute("SELECT client_code, client_name FROM ARClientMaster")
                clients = cursor.fetchall()
                form.client_code.choices = [(client['client_code'], client['client_name']) for client in clients]
            except mysql.connector.Error as err:
                logging.error(f"거래처 목록 조회 실패: {err}")
                flash('거래처 목록을 불러오는 중 오류가 발생했습니다.', 'danger')
                form.client_code.choices = []

            if form.validate_on_submit():
                client_code = form.client_code.data
                order_date = form.order_date.data
                amount = form.amount.data

                logging.debug(f"발주 추가 요청 - client_code: {client_code}, order_date: {order_date}, amount: {amount}")

                try:
                    # 거래처 마스터에서 필요한 정보 조회 (client_code 기준, 첫 번째 레코드)
                    cursor.execute("SELECT representative_code, manager, client_name FROM ARClientMaster WHERE client_code = %s LIMIT 1", (client_code,))
                    client = cursor.fetchone()

                    if not client:
                        logging.warning(f"선택된 client_code '{client_code}'가 ARClientMaster 테이블에 존재하지 않습니다.")
                        flash('선택한 거래처가 존재하지 않습니다.', 'danger')
                        return render_template('add_order.html', form=form)  # form 전달

                    representative_code = client['representative_code']
                    manager = client['manager']
                    client_name = client['client_name']

                    logging.debug(f"대표 코드: {representative_code}, 관리자: {manager}")

                    # AROrderDetails 테이블에 삽입
                    insert_order_query = """
                        INSERT INTO AROrderDetails (
                            representative_code, client_code, client_name, collector_key, manager, order_date, order_amount
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    cursor.execute(insert_order_query, (representative_code, client_code, client_name, '', manager, order_date, amount))

                    # ARTransactionsLedger 테이블에 식자재 매출 기록 삽입
                    insert_ledger_query = """
                        INSERT INTO ARTransactionsLedger (
                            transaction_date,
                            client,
                            client,
                            outlet_name,
                            debit,
                            credit,
                            food_material_sales
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    cursor.execute(insert_ledger_query, (
                        order_date,
                        client_code,  # client_code 사용
                        client_name,
                        '',  # outlet_name은 발주와 관련 없으므로 빈 문자열
                        amount,  # debit (식자재 매출)
                        0,       # credit
                        amount   # food_material_sales
                    ))

                    db.commit()

                    logging.info(f"발주 내역이 성공적으로 추가되었습니다: client_code={client_code}, order_date={order_date}, amount={amount}")
                    flash('발주 내역이 성공적으로 추가되었습니다.', 'success')
                    return redirect(url_for('index'))

                except mysql.connector.Error as db_err:
                    logging.error(f"발주 내역 추가 실패 (DB 오류): {db_err}")
                    db.rollback()
                    flash('발주 내역을 추가하는 중 오류가 발생했습니다.', 'danger')
                    return render_template('add_order.html', form=form)  # form 전달
                except Exception as e:
                    logging.error(f"발주 내역 추가 실패 (기타 오류): {e}")
                    db.rollback()
                    flash('발주 내역을 추가하는 중 예상치 못한 오류가 발생했습니다.', 'danger')
                    return render_template('add_order.html', form=form)  # form 전달

    finally:
        db.close()

    return render_template('add_order.html', form=form)

# 대표 코드 반환 API 엔드포인트
@app.route('/get_representative_code', methods=['POST'])
def get_representative_code():
    client_code = request.form.get('client_code')
    if not client_code:
        return jsonify({'error': 'Client code not provided.'}), 400

    db = get_db_connection()
    if not db:
        return jsonify({'error': 'Database connection failed.'}), 500

    try:
        with db.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT representative_code FROM ARClientMaster WHERE client_code = %s LIMIT 1", (client_code,))
            result = cursor.fetchone()
            if result:
                return jsonify({'representative_code': result['representative_code']})
            else:
                return jsonify({'error': 'Client not found.'}), 404
    except mysql.connector.Error as db_err:
        logging.error(f"대표 코드 조회 실패: {db_err}")
        return jsonify({'error': 'Database error occurred.'}), 500
    finally:
        db.close()


def clean_virtual_account_number(van_number):
    """
    가상계좌번호에서 하이픈 제거
    """
    if isinstance(van_number, str):
        return van_number.replace('-', '').strip()
    return van_number

@app.route('/upload_bank_payments', methods=['GET', 'POST'])
def upload_bank_payments():
    form = UploadForm()
    if form.validate_on_submit():
        file = form.file.data
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            # 업로드 디렉토리 생성
            if not os.path.exists(app.config['UPLOAD_FOLDER']):
                os.makedirs(app.config['UPLOAD_FOLDER'])
                logging.debug(f"업로드 디렉토리 생성: {app.config['UPLOAD_FOLDER']}")
            
            try:
                file.save(file_path)
                logging.info(f"업로드된 파일이 저장되었습니다: {file_path}")
            except Exception as e:
                logging.error(f"파일 저장 실패: {e}")
                flash('업로드된 파일을 저장하는 중 오류가 발생했습니다.', 'danger')
                return redirect(request.url)

            db = get_db_connection()
            if not db:
                flash('Database connection failed.', 'danger')
                return redirect(request.url)

            try:
                with db.cursor(dictionary=True) as cursor:
                    # 엑셀 파일을 헤더 없이 읽기
                    df = pd.read_excel(file_path, header=None)
                    logging.info(f"엑셀 파일을 성공적으로 읽었습니다: {filename}")
                    
                    # 헤더 행 찾기: 두 번째 열이 "입금일자"인 행 (첫 번째 열은 "No.")
                    header_row_index = None
                    for i, row in df.iterrows():
                        second_cell = str(row.iloc[1]).strip()
                        if second_cell == '입금일자':
                            header_row_index = i
                            break

                    if header_row_index is None:
                        logging.warning("엑셀 파일에서 헤더를 찾을 수 없습니다.")
                        flash('엑셀 파일에서 헤더를 찾을 수 없습니다.', 'danger')
                        return redirect(request.url)
                    
                    # 헤더가 있는 행부터 데이터 읽기
                    df = pd.read_excel(file_path, header=header_row_index)
                    logging.debug(f"헤더가 발견된 행: {header_row_index}")
                    logging.debug(f"데이터프레임 샘플:\n{df.head()}")

                    # 필요한 열만 선택 (불필요한 열은 무시)
                    required_columns = ['입금일자', '입금시간', '가상계좌번호', '입금금액']
                    if not all(col in df.columns for col in required_columns):
                        missing_cols = [col for col in required_columns if col not in df.columns]
                        logging.warning(f"엑셀 파일에 누락된 필드: {missing_cols}")
                        flash(f'엑셀 파일에 누락된 필드가 있습니다: {missing_cols}', 'danger')
                        return redirect(request.url)
                    
                    df = df[required_columns]

                    # 데이터 타입 강제 변환
                    df['입금일자'] = pd.to_datetime(df['입금일자'], errors='coerce').dt.date
                    df['입금시간'] = pd.to_datetime(df['입금시간'], format='%H:%M:%S', errors='coerce').dt.time
                    df['가상계좌번호'] = df['가상계좌번호'].apply(clean_virtual_account_number)
                    df['입금금액'] = pd.to_numeric(df['입금금액'].astype(str).str.replace(',', '', regex=True), errors='coerce')
                    
                    logging.debug(f"형변환 후 데이터프레임 샘플:\n{df.head()}")

                    # 데이터 삽입 준비
                    inserted_records = 0
                    for index, row in df.iterrows():
                        # 데이터 유효성 검사: '입금일자'와 '가상계좌번호'가 유효한지 확인
                        if pd.isna(row['입금일자']) or pd.isna(row['가상계좌번호']):
                            logging.debug(f"Row {index}은 '입금일자' 또는 '가상계좌번호'가 NaN이므로 무시됩니다.")
                            continue

                        payment_date = row['입금일자']
                        payment_time = row['입금시간']
                        virtual_account_number = row['가상계좌번호']
                        payment_amount = row['입금금액']

                        # 상태가 '입금'인지 확인 (필요 시 추가 조건)
                        # 새로운 엑셀 포맷에서 '상태' 필드는 무시되므로 필요 시 로직 추가
                        # 현재 엑셀 포맷에서는 이 필드를 무시하고 있습니다.

                        # 입금금액이 NaN인지 확인하고 기본값 설정
                        if pd.isna(payment_amount):
                            logging.warning(f"Row {index}의 '입금금액'이 NaN입니다. 기본값 0.00으로 설정합니다.")
                            payment_amount = Decimal('0.00')
                        else:
                            payment_amount = Decimal(payment_amount)

                        logging.debug(f"데이터 삽입 준비 - Row {index}: virtual_account_number={virtual_account_number}, payment_date={payment_date}, payment_amount={payment_amount}")

                        # ARBankAccountMaster 테이블에서 client_code, client_name, manager, collector_key, representative_code 조회
                        try:
                            cursor.execute("""
                                SELECT client_code, client_name, manager, collector_key, representative_code 
                                FROM ARBankAccountMaster 
                                WHERE REPLACE(hana_bank_virtual_account, '-', '') = %s 
                                LIMIT 1
                            """, (virtual_account_number,))
                            account_data = cursor.fetchone()
                            if account_data:
                                client_code = account_data['client_code']
                                client_name = account_data['client_name']
                                manager = account_data['manager']
                                collector_key = account_data['collector_key']
                                representative_code = account_data['representative_code']
                            else:
                                logging.warning(f"가상계좌번호 '{virtual_account_number}'에 해당하는 계좌를 ARBankAccountMaster에서 찾을 수 없습니다.")
                                flash(f"Row {index + 1}: 가상계좌번호 '{virtual_account_number}'에 해당하는 계좌를 찾을 수 없습니다.", 'warning')
                                continue
                        except mysql.connector.Error as db_err:
                            logging.error(f"ARBankAccountMaster에서 데이터 조회 실패 - Row {index}: {db_err}")
                            flash(f"Row {index + 1}: ARBankAccountMaster 조회 실패.", 'danger')
                            continue

                        # ARBankPaymentDetails 테이블에 삽입
                        insert_bank_payment_query = """
                            INSERT INTO ARBankPaymentDetails (
                                payment_date, payment_time, client_code, collector_key, virtual_account_number, payment_amount
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        try:
                            cursor.execute(insert_bank_payment_query, (
                                payment_date,
                                payment_time,
                                client_code,
                                collector_key,
                                virtual_account_number,
                                payment_amount
                            ))
                            logging.debug(f"ARBankPaymentDetails 삽입 성공 - Row {index}")
                        except mysql.connector.Error as db_err:
                            logging.error(f"ARBankPaymentDetails 삽입 실패 - Row {index}: {db_err}")
                            flash(f"Row {index + 1}: ARBankPaymentDetails 삽입 실패.", 'danger')
                            continue

                        # ARTransactionsLedger 테이블에 삽입
                        insert_ledger_query = """
                            INSERT INTO ARTransactionsLedger (
                                transaction_date, representative_code, client, outlet_name, debit, credit, cash_deposit
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        try:
                            cursor.execute(insert_ledger_query, (
                                payment_date,
                                representative_code if representative_code else '',  # representative_code이 없을 경우 빈 값
                                client_code,
                                client_name,     # outlet_name에 client_name 사용 (필요 시 수정)
                                Decimal('0.00'), # debit
                                payment_amount,  # credit
                                payment_amount   # cash_deposit
                            ))
                            logging.debug(f"ARTransactionsLedger 삽입 성공 - Row {index}")
                        except mysql.connector.Error as db_err:
                            logging.error(f"ARTransactionsLedger 삽입 실패 - Row {index}: {db_err}")
                            flash(f"Row {index + 1}: ARTransactionsLedger 삽입 실패.", 'danger')
                            continue

                        inserted_records += 1

                    db.commit()
                    logging.info(f"{inserted_records}개의 은행 입금 내역이 성공적으로 업로드되었습니다.")
                    flash(f'{inserted_records}개의 은행 입금 내역이 성공적으로 업로드되었습니다.', 'success')
                    return redirect(url_for('index'))

            except Exception as e:
                logging.error(f"업로드된 파일 처리 중 오류 발생: {e}")
                db.rollback()
                flash('파일을 처리하는 중 오류가 발생했습니다.', 'danger')
                return redirect(request.url)
            finally:
                db.close()

    return render_template('upload_bank_payments.html', form=form)


# 발주 내역 다량 업로드
@app.route('/upload_orders', methods=['GET', 'POST'])
def upload_orders():
    form = BulkUploadOrdersForm()
    if form.validate_on_submit():
        file = form.file.data
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            # 업로드 디렉토리 생성 (존재하지 않을 경우)
            if not os.path.exists(app.config['UPLOAD_FOLDER']):
                os.makedirs(app.config['UPLOAD_FOLDER'])
                logging.debug(f"업로드 디렉토리 생성: {app.config['UPLOAD_FOLDER']}")
            
            try:
                file.save(file_path)
                logging.info(f"업로드된 파일이 저장되었습니다: {file_path}")
            except Exception as e:
                logging.error(f"파일 저장 실패: {e}")
                flash('업로드된 파일을 저장하는 중 오류가 발생했습니다.', 'danger')
                return redirect(request.url)

            db = get_db_connection()
            if not db:
                flash('Database connection failed.', 'danger')
                return redirect(request.url)

            try:
                with db.cursor(dictionary=True) as cursor:
                    # 엑셀 파일 읽기 (헤더 없는 경우)
                    df = pd.read_excel(file_path, header=None)
                    logging.info(f"엑셀 파일을 성공적으로 읽었습니다: {filename}")
                    
                    # 헤더 행 찾기
                    header_row_index = None
                    for i, row in df.iterrows():
                        if all(col in row.values for col in ['order_date', 'client_code', 'order_amount', 'collector_key']):
                            header_row_index = i
                            break

                    if header_row_index is None:
                        logging.warning("엑셀 파일에서 헤더를 찾을 수 없습니다.")
                        flash('엑셀 파일에서 헤더를 찾을 수 없습니다.', 'danger')
                        return redirect(request.url)
                    
                    # 헤더가 있는 행부터 데이터 추출
                    df = pd.read_excel(file_path, header=header_row_index)
                    logging.debug(f"헤더가 발견된 행: {header_row_index}")
                    logging.debug(f"데이터프레임 샘플:\n{df.head()}")

                    # 필요한 컬럼 선택
                    required_columns = ['order_date', 'client_code', 'order_amount', 'collector_key']
                    if not all(col in df.columns for col in required_columns):
                        missing_cols = [col for col in required_columns if col not in df.columns]
                        logging.warning(f"엑셀 파일에 누락된 필드: {missing_cols}")
                        flash(f'엑셀 파일에 누락된 필드가 있습니다: {missing_cols}', 'danger')
                        return redirect(request.url)

                    df = df[required_columns]

                    # 데이터 타입 변환
                    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce').dt.date
                    df['client_code'] = df['client_code'].astype(str).str.strip()
                    df['order_amount'] = pd.to_numeric(df['order_amount'], errors='coerce').fillna(0)
                    df['collector_key'] = df['collector_key'].astype(str).str.strip()

                    logging.debug(f"형변환 후 데이터프레임 샘플:\n{df.head()}")

                    # 데이터 정제: 필수 컬럼 결측값 제거
                    df = df.dropna(subset=['order_date', 'client_code', 'collector_key'])

                    # ARClientMaster에서 필요한 데이터 가져오기
                    client_codes = df['client_code'].unique().tolist()
                    if not client_codes:
                        logging.warning("발주 내역에 client_code가 없습니다.")
                        flash('발주 내역에 client_code가 없습니다.', 'danger')
                        return redirect(request.url)

                    format_strings = ','.join(['%s'] * len(client_codes))
                    cursor.execute(f"SELECT client_code, representative_code, client_name, manager FROM ARClientMaster WHERE client_code IN ({format_strings})", tuple(client_codes))
                    clients = cursor.fetchall()
                    client_dict = {client['client_code']: client for client in clients}

                    # AROrderDetails 및 ARTransactionsLedger 데이터 준비
                    order_details_data = []
                    ledger_data = []
                    for index, row in df.iterrows():
                        client_code = row['client_code']
                        order_date = row['order_date']
                        order_amount = row['order_amount']
                        collector_key = row['collector_key']
                        
                        client = client_dict.get(client_code)
                        if not client:
                            logging.warning(f"client_code '{client_code}'가 ARClientMaster 테이블에 존재하지 않습니다.")
                            flash(f"Row {index + 1}: client_code '{client_code}'가 ARClientMaster 테이블에 존재하지 않습니다.", 'warning')
                            continue
                        representative_code = client['representative_code']
                        client_name = client['client_name']
                        manager = client['manager']

                        # AROrderDetails용 데이터 준비
                        order_details_data.append((
                            representative_code,
                            client_code,    # client_code 사용
                            client_name,
                            collector_key,
                            manager,
                            order_date,
                            order_amount
                        ))

                        # ARTransactionsLedger용 데이터 준비
                        ledger_data.append((
                            order_date,                # transaction_date
                            representative_code,       # representative_code
                            client_code,               # client
                            client_name,                        # outlet_name (빈 문자열)
                            order_amount,                         # debit
                            0,              # credit
                            order_amount,              # food_material_sales
                            0,                         # royalty_sales
                            0,                         # advertising_fees
                            0,                         # other_sales
                            0,                         # cash_deposit
                            0,                         # meal_voucher_deposit
                            0,                         # delivery_fee
                            0,                         # card_deposit
                            0,                         # pos_usage_fee
                            0                          # receivables
                        ))

                    # AROrderDetails 테이블에 데이터 삽입
                    if order_details_data:
                        insert_order_query = """
                            INSERT INTO AROrderDetails (
                                representative_code, client_code, client_name, collector_key, manager, order_date, order_amount
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, %s, %s
                            )
                        """
                        cursor.executemany(insert_order_query, order_details_data)
                        logging.debug(f"AROrderDetails 삽입 성공: {len(order_details_data)}건")
                    
                    # ARTransactionsLedger 테이블에 데이터 삽입
                    if ledger_data:
                        insert_ledger_query = """
                            INSERT INTO ARTransactionsLedger (
                                transaction_date,
                                representative_code,
                                client,
                                outlet_name,
                                debit,
                                credit,
                                food_material_sales,
                                royalty_sales,
                                advertising_fees,
                                other_sales,
                                cash_deposit,
                                meal_voucher_deposit,
                                delivery_fee,
                                card_deposit,
                                pos_usage_fee,
                                receivables
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """
                        cursor.executemany(insert_ledger_query, ledger_data)
                        logging.debug(f"ARTransactionsLedger 삽입 성공: {len(ledger_data)}건")

                    # 트랜잭션 커밋
                    db.commit()
                    inserted_records = len(order_details_data)
                    logging.info(f"{inserted_records}개의 발주 내역이 성공적으로 업로드되었습니다.")
                    flash(f'{inserted_records}개의 발주 내역이 성공적으로 업로드되었습니다.', 'success')
                    return redirect(url_for('index'))
            except Exception as e:
                logging.error(f"업로드된 파일 처리 중 오류 발생: {e}")
                db.rollback()
                flash('파일을 처리하는 중 오류가 발생했습니다.', 'danger')
                return redirect(request.url)
            finally:
                db.close()
    return render_template('upload_orders.html', form=form)


# 미수금액 조회
@app.route('/view_receivables', methods=['GET'])
def view_receivables():
    db = get_db_connection()
    if not db:
        flash('Database connection failed.', 'danger')
        return redirect(url_for('index'))

    try:
        with db.cursor(dictionary=True) as cursor:
            # 검색 파라미터 가져오기
            search_outlet = request.args.get('search_outlet', '').strip()

            # 기본 쿼리 수정: TRIM과 UPPER을 사용하여 데이터 정제 후 그룹화
            query = """
                SELECT 
                    TRIM(UPPER(client)) AS client,
                    TRIM(UPPER(outlet_name)) AS outlet_name,
                    SUM(debit) AS total_debit,
                    SUM(credit) AS total_credit,
                    SUM(food_material_sales) AS total_food_material_sales,
                    SUM(royalty_sales) AS total_royalty_sales,
                    SUM(pos_usage_fee) AS total_pos_usage_fee,
                    SUM(cash_deposit) AS total_cash_deposit,
                    SUM(card_deposit) AS total_card_deposit,
                    SUM(debit) - SUM(credit) AS receivables
                FROM 
                    ARTransactionsLedger
            """

            params = []

            # 검색 조건 추가
            if search_outlet:
                query += " WHERE TRIM(UPPER(outlet_name)) LIKE %s"
                params.append(f"%{search_outlet.upper().strip()}%")

            # GROUP BY client, outlet_name로 변경
            query += """
                GROUP BY 
                    TRIM(UPPER(client)), 
                    TRIM(UPPER(outlet_name))
                ORDER BY 
                    client, outlet_name
            """

            cursor.execute(query, tuple(params))
            results = cursor.fetchall()

            # 합계 계산
            sum_total_debit = sum(row['total_debit'] for row in results) if results else 0
            sum_total_credit = sum(row['total_credit'] for row in results) if results else 0
            sum_food_material_sales = sum(row['total_food_material_sales'] for row in results) if results else 0
            sum_royalty_sales = sum(row['total_royalty_sales'] for row in results) if results else 0
            sum_pos_usage_fee = sum(row['total_pos_usage_fee'] for row in results) if results else 0
            sum_cash_deposit = sum(row['total_cash_deposit'] for row in results) if results else 0
            sum_card_deposit = sum(row['total_card_deposit'] for row in results) if results else 0
            sum_receivables = sum(row['receivables'] for row in results) if results else 0

            # 쿼리 결과 로그 출력
            logging.debug(f"미수금액 조회 결과: {results}")

            logging.info("미수금액 조회 성공")
            return render_template(
                'view_receivables.html', 
                results=results, 
                search_outlet=search_outlet,
                sum_total_debit=sum_total_debit,
                sum_total_credit=sum_total_credit,
                sum_food_material_sales=sum_food_material_sales,
                sum_royalty_sales=sum_royalty_sales,
                sum_pos_usage_fee=sum_pos_usage_fee,
                sum_cash_deposit=sum_cash_deposit,
                sum_card_deposit=sum_card_deposit,
                sum_receivables=sum_receivables
            )
    except mysql.connector.Error as db_err:
        logging.error(f"미수금액 조회 실패: {db_err}")
        flash('미수금액을 조회하는 중 오류가 발생했습니다.', 'danger')
        return redirect(url_for('index'))
    except Exception as e:
        logging.error(f"미수금액 조회 실패 (기타 오류): {e}")
        flash('미수금액을 조회하는 중 예상치 못한 오류가 발생했습니다.', 'danger')
        return redirect(url_for('index'))
    finally:
        db.close()

def clean_decimal(value):
    """
    문자열에서 숫자와 소수점, 음수 기호만 남기고 제거한 후 Decimal로 변환합니다.
    """
    if isinstance(value, str):
        value = re.sub(r'[^\d.-]', '', value)  # 숫자, 소수점, 음수 기호만 남김
    return Decimal(value) if value else Decimal('0.00')



@app.route('/view_daily_transactions', methods=['GET'])
def view_daily_transactions():
    db = get_db_connection()
    if not db:
        flash('데이터베이스 연결에 실패했습니다.', 'danger')
        return redirect(url_for('index'))  # 'index' 라우트는 홈 페이지로 가정

    try:
        with db.cursor(dictionary=True) as cursor:
            # 검색 파라미터 가져오기
            search_outlet = request.args.get('search_outlet', '').strip()
            selected_year = request.args.get('year', datetime.now().year, type=int)
            selected_month = request.args.get('month', datetime.now().month, type=int)

            # 첫날과 마지막 날 계산
            try:
                first_day = datetime(selected_year, selected_month, 1).date()
                if selected_month == 12:
                    last_day = datetime(selected_year + 1, 1, 1).date()
                else:
                    last_day = datetime(selected_year, selected_month + 1, 1).date()
            except ValueError as ve:
                logging.error(f"날짜 계산 오류: {ve}")
                flash('유효하지 않은 날짜입니다.', 'danger')
                return redirect(url_for('index'))

            # SQL 쿼리 작성
            query = """
                SELECT 
                    A.client,
                    A.outlet_name,
                    C.collector_key,
                    C.manager,
            """

            # 동적 SQL 쿼리 생성: 각 일자별로 debit과 credit 합계를 계산
            day_sums = []
            for day in range(1, 32):
                day_sums.append(f"SUM(CASE WHEN DAY(A.transaction_date) = {day} THEN A.debit ELSE 0 END) AS `day_{day}_debit`")
                day_sums.append(f"SUM(CASE WHEN DAY(A.transaction_date) = {day} THEN A.credit ELSE 0 END) AS `day_{day}_credit`")

            # 총합계 계산 추가
            day_sums.append("SUM(A.debit) AS `total_debit`")
            day_sums.append("SUM(A.credit) AS `total_credit`")
            day_sums.append("(SUM(A.debit) - SUM(A.credit)) AS `total_receivables`")  # total_receivables 추가

            # SELECT 절에 모든 필드를 추가
            query += ",\n".join(day_sums)

            # FROM 절과 JOIN 절
            query += """
                FROM 
                    ARTransactionsLedger A
                JOIN 
                    ARClientMaster C ON A.client = C.client_code
            """

            # 검색 조건 추가
            conditions = []
            params = []
            if search_outlet:
                conditions.append("A.outlet_name LIKE %s")
                params.append(f"%{search_outlet}%")
            conditions.append("A.transaction_date >= %s AND A.transaction_date < %s")
            params.extend([first_day, last_day])

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            # GROUP BY 절 및 ORDER BY 절
            query += """
                GROUP BY 
                    A.client, A.outlet_name, C.collector_key, C.manager
                ORDER BY 
                    total_receivables DESC
            """

            logging.debug(f"실행할 쿼리: {query}")
            logging.debug(f"쿼리 파라미터: {params}")

            cursor.execute(query, tuple(params))
            results = cursor.fetchall()

            logging.debug(f"쿼리 결과: {results}")

            # 전달할 데이터 구조를 준비
            data = []
            sum_total_debit = Decimal('0.00')
            sum_total_credit = Decimal('0.00')
            sum_total_receivables = Decimal('0.00')  # 최종 합계에서 계산

            for row in results:
                # total_debit, total_credit, total_receivables을 Decimal으로 변환
                try:
                    total_debit = clean_decimal(row['total_debit'])
                    total_credit = clean_decimal(row['total_credit'])
                    total_receivables = clean_decimal(row['total_receivables'])
                except (ValueError, TypeError) as ve:
                    logging.error(f"데이터 변환 오류: {ve}")
                    total_debit = Decimal('0.00')
                    total_credit = Decimal('0.00')
                    total_receivables = Decimal('0.00')

                logging.debug(f"Row total_debit: {total_debit}, Row total_credit: {total_credit}, Row total_receivables: {total_receivables}")

                # 일별 데이터 수집
                day_data = {}
                for day in range(1, 32):
                    debit_key = f'day_{day}_debit'
                    credit_key = f'day_{day}_credit'
                    try:
                        debit_value = clean_decimal(row.get(debit_key, 0.0))
                        credit_value = clean_decimal(row.get(credit_key, 0.0))
                    except (ValueError, TypeError) as ve:
                        logging.error(f"일별 데이터 변환 오류: {ve}")
                        debit_value = Decimal('0.00')
                        credit_value = Decimal('0.00')

                    logging.debug(f"day_{day}_debit: {debit_value}, day_{day}_credit: {credit_value}")

                    # 데이터가 0보다 크면 포맷팅된 값, 아니면 '-'
                    day_data[f'day_{day}_debit'] = f"{debit_value:,.2f}" if debit_value > 0 else '-'
                    day_data[f'day_{day}_credit'] = f"{credit_value:,.2f}" if credit_value > 0 else '-'

                # 데이터 행 구성
                data_row = {
                    'client': row['client'],
                    'outlet_name': row['outlet_name'],
                    'collector_key': row['collector_key'],
                    'manager': row['manager'],
                    'total_debit': "{0:.2f}".format(total_debit) if total_debit > 0 else '0.00',
                    'total_credit': "{0:.2f}".format(total_credit) if total_credit > 0 else '0.00',
                    'total_receivables': "{0:.2f}".format(total_receivables) if total_receivables > 0 else '0.00',
                    'day_data': day_data
                }

                logging.debug(f"Data Row: {data_row}")

                # 누적 합계
                sum_total_debit += total_debit
                sum_total_credit += total_credit
                sum_total_receivables += total_receivables

                logging.debug(f"After adding row: sum_total_debit = {sum_total_debit}, sum_total_credit = {sum_total_credit}, sum_total_receivables = {sum_total_receivables}")

                data.append(data_row)

            # 최종 합계는 이미 누적됨
            logging.debug(f"Final Sum - Debit: {sum_total_debit}, Credit: {sum_total_credit}, Receivables: {sum_total_receivables}")

            # 전체 합계 포맷팅
            formatted_sum_total_debit = f"{sum_total_debit:,.2f}" if sum_total_debit > 0 else '-'
            formatted_sum_total_credit = f"{sum_total_credit:,.2f}" if sum_total_credit > 0 else '-'
            formatted_sum_total_receivables = f"{sum_total_receivables:,.2f}" if sum_total_receivables > 0 else '-'  # 전체 합계 포맷팅 추가

            return render_template(
                'view_daily_transactions.html',
                data=data,
                selected_year=selected_year,
                selected_month=selected_month,
                search_outlet=search_outlet,
                sum_total_debit=formatted_sum_total_debit,
                sum_total_credit=formatted_sum_total_credit,
                sum_total_receivables=formatted_sum_total_receivables  # 전체 합계 전달
            )
    except mysql.connector.Error as db_err:
        logging.error(f"데이터 조회 실패: {db_err}")
        flash('데이터를 조회하는 중 오류가 발생했습니다.', 'danger')
        return redirect(url_for('index'))
    except Exception as e:
        logging.error(f"예기치 않은 오류: {e}")
        flash('데이터를 처리하는 중 오류가 발생했습니다.', 'danger')
        return redirect(url_for('index'))
    finally:
        db.close()

        
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

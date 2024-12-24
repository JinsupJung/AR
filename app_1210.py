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

# MySQL 연결 설정
try:
    db = mysql.connector.connect(
        host="175.196.7.45",
        user="nolboo",
        password="2024!puser",
        database="nolboo",
        charset='utf8mb4'
    )
    cursor = db.cursor(dictionary=True)
    logging.info("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
except mysql.connector.Error as err:
    logging.error(f"MySQL 연결 오류: {err}")
    raise



# MySQL 연결 설정 (기존 설정 유지)
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

    cursor = db.cursor(dictionary=True)
    
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
            # 거래처 마스터에서 필요한 정보 조회 (대표 코드 기준, 첫 번째 레코드)
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
            db.commit()

            # ARTransactionsLedger 테이블에 식자재 매출 기록 삽입
            insert_ledger_query = """
                INSERT INTO ARTransactionsLedger (
                    transaction_date,
                    representative_code,
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
                representative_code,
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

    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("SELECT representative_code FROM ARClientMaster WHERE client_code = %s LIMIT 1", (client_code,))
        result = cursor.fetchone()
        if result:
            return jsonify({'representative_code': result['representative_code']})
        else:
            return jsonify({'error': 'Client not found.'}), 404
    except mysql.connector.Error as db_err:
        logging.error(f"대표 코드 조회 실패: {db_err}")
        return jsonify({'error': 'Database error occurred.'}), 500


@app.route('/upload_bank_payments', methods=['GET', 'POST'])
def upload_bank_payments():
    form = UploadForm()
    if form.validate_on_submit():
        file = form.file.data
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            # Create the upload directory if it does not exist
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

            try:
                # Read the Excel file and identify the header row
                df = pd.read_excel(file_path, header=None)
                logging.info(f"엑셀 파일을 성공적으로 읽었습니다: {filename}")
                
                # Find the header row: contains '입금일자', '입금시간', '매출처코드', '콜렉트키', '가상계좌번호', '입금금액'
                header_row_index = None
                for i, row in df.iterrows():
                    if all(col in row.values for col in ['입금일자', '입금시간', '매출처코드', '콜렉트키', '가상계좌번호', '입금금액']):
                        header_row_index = i
                        break

                if header_row_index is None:
                    logging.warning("엑셀 파일에서 헤더를 찾을 수 없습니다.")
                    flash('엑셀 파일에서 헤더를 찾을 수 없습니다.', 'danger')
                    return redirect(request.url)
                
                # Extract data starting from the header row
                df = pd.read_excel(file_path, header=header_row_index)
                logging.debug(f"헤더가 발견된 행: {header_row_index}")
                logging.debug(f"데이터프레임 샘플:\n{df.head()}")

                # Select only the required columns
                required_columns = ['입금일자', '입금시간', '매출처코드', '콜렉트키', '가상계좌번호', '입금금액']
                if not all(col in df.columns for col in required_columns):
                    missing_cols = [col for col in required_columns if col not in df.columns]
                    logging.warning(f"엑셀 파일에 누락된 필드: {missing_cols}")
                    flash(f'엑셀 파일에 누락된 필드가 있습니다: {missing_cols}', 'danger')
                    return redirect(request.url)

                df = df[required_columns]

                # Force the correct data types
                df['입금일자'] = pd.to_datetime(df['입금일자'], errors='coerce').dt.date
                df['입금시간'] = pd.to_datetime(df['입금시간'], format='%H:%M:%S', errors='coerce').dt.time
                df['매출처코드'] = df['매출처코드'].astype(str).str.strip()
                df['콜렉트키'] = df['콜렉트키'].astype(str).str.strip()
                df['가상계좌번호'] = df['가상계좌번호'].astype(str).str.strip()
                df['입금금액'] = pd.to_numeric(df['입금금액'], errors='coerce')

                logging.debug(f"형변환 후 데이터프레임 샘플:\n{df.head()}")

                # Prepare data for insertion
                inserted_records = 0
                for index, row in df.iterrows():
                    payment_date = row['입금일자']
                    payment_time = row['입금시간']
                    client_code = row['매출처코드']
                    collector_key = row['콜렉트키']
                    virtual_account_number = row['가상계좌번호']
                    payment_amount = row['입금금액']

                    logging.debug(f"데이터 삽입 준비 - Row {index}: client_code={client_code}, payment_date={payment_date}, payment_amount={payment_amount}")

                    # Query the ARClientMaster table to get the representative_code and client_name
                    try:
                        cursor.execute("SELECT representative_code, client_name FROM ARClientMaster WHERE client_code = %s LIMIT 1", (client_code,))
                        client_data = cursor.fetchone()
                        if client_data:
                            representative_code = client_data['representative_code']
                            client_name = client_data['client_name']
                        else:
                            representative_code = ''
                            client_name = ''
                            logging.warning(f"client_code '{client_code}'에 해당하는 거래처를 찾을 수 없습니다.")
                            flash(f"Row {index + 1}: client_code '{client_code}'에 해당하는 거래처를 찾을 수 없습니다.", 'warning')
                            continue
                    except mysql.connector.Error as db_err:
                        logging.error(f"ARClientMaster에서 representative_code 조회 실패 - Row {index}: {db_err}")
                        continue  # Skip to the next row

                    # Insert data into ARBankPaymentDetails
                    insert_bank_payment_query = """
                        INSERT INTO ARBankPaymentDetails (
                            payment_date, payment_time, client_code, collector_key, virtual_account_number, payment_amount
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    try:
                        cursor.execute(insert_bank_payment_query, (
                            payment_date, payment_time, client_code, collector_key, virtual_account_number, payment_amount
                        ))
                        logging.debug(f"ARBankPaymentDetails 삽입 성공 - Row {index}")
                    except mysql.connector.Error as db_err:
                        logging.error(f"ARBankPaymentDetails 삽입 실패 - Row {index}: {db_err}")
                        flash(f"Row {index + 1}: ARBankPaymentDetails 삽입 실패.", 'danger')
                        continue

                    # Insert data into ARTransactionsLedger with client_name as outlet_name
                    insert_ledger_query = """
                        INSERT INTO ARTransactionsLedger (
                            transaction_date, representative_code, client, outlet_name, debit, credit, cash_deposit
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    try:
                        cursor.execute(insert_ledger_query, (
                            payment_date,
                            representative_code,
                            client_code,  # Use the client_code
                            client_name,  # Use the client_name from ARClientMaster as outlet_name
                            0,  # debit
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
            
            # Create the upload directory if it does not exist
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

            try:
                # Read the Excel file and identify the header row
                df = pd.read_excel(file_path, header=None)
                logging.info(f"엑셀 파일을 성공적으로 읽었습니다: {filename}")
                
                # Find the header row: contains 'order_date', 'representative_code', 'order_amount', 'collector_key'
                header_row_index = None
                for i, row in df.iterrows():
                    if all(col in row.values for col in ['order_date', 'representative_code', 'order_amount', 'collector_key']):
                        header_row_index = i
                        break

                if header_row_index is None:
                    logging.warning("엑셀 파일에서 헤더를 찾을 수 없습니다.")
                    flash('엑셀 파일에서 헤더를 찾을 수 없습니다.', 'danger')
                    return redirect(request.url)
                
                # Extract data starting from the header row
                df = pd.read_excel(file_path, header=header_row_index)
                logging.debug(f"헤더가 발견된 행: {header_row_index}")
                logging.debug(f"데이터프레임 샘플:\n{df.head()}")

                # Select only the required columns
                required_columns = ['order_date', 'representative_code', 'order_amount', 'collector_key']
                if not all(col in df.columns for col in required_columns):
                    missing_cols = [col for col in required_columns if col not in df.columns]
                    logging.warning(f"엑셀 파일에 누락된 필드: {missing_cols}")
                    flash(f'엑셀 파일에 누락된 필드가 있습니다: {missing_cols}', 'danger')
                    return redirect(request.url)

                df = df[required_columns]

                # Force the correct data types
                df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce').dt.date
                df['representative_code'] = df['representative_code'].astype(str).str.strip()
                df['order_amount'] = pd.to_numeric(df['order_amount'], errors='coerce')
                df['collector_key'] = df['collector_key'].astype(str).str.strip()

                logging.debug(f"형변환 후 데이터프레임 샘플:\n{df.head()}")

                # Fetch necessary data from ARClientMaster
                representative_codes = df['representative_code'].unique().tolist()
                format_strings = ','.join(['%s'] * len(representative_codes))
                cursor.execute(f"SELECT representative_code, client_code, client_name, manager FROM ARClientMaster WHERE representative_code IN ({format_strings})", tuple(representative_codes))
                clients = cursor.fetchall()
                client_dict = {client['representative_code']: client for client in clients}

                # Prepare data for AROrderDetails and ARTransactionsLedger
                order_details_data = []
                ledger_data = []
                for index, row in df.iterrows():
                    representative_code = row['representative_code']
                    order_date = row['order_date']
                    order_amount = row['order_amount']
                    collector_key = row['collector_key']
                    
                    client = client_dict.get(representative_code)
                    if not client:
                        logging.warning(f"representative_code '{representative_code}'가 ARClientMaster 테이블에 존재하지 않습니다.")
                        flash(f"Row {index + 1}: representative_code '{representative_code}'가 ARClientMaster 테이블에 존재하지 않습니다.", 'warning')
                        continue
                    client_code = client['client_code']
                    client_name = client['client_name']
                    manager = client['manager']

                    # Prepare data for AROrderDetails (client_code is empty as per requirement)
                    order_details_data.append((
                        representative_code,
                        "",  # client_code is empty
                        client_name,
                        collector_key,
                        manager,
                        order_date,
                        order_amount
                    ))

                    # Prepare data for ARTransactionsLedger
                    ledger_data.append((
                        order_date,
                        representative_code,
                        "",  # client is empty as per requirement
                        client_name,  # outlet_name is the client_name from ARClientMaster
                        order_amount,  # debit (식자재 매출) is the order_amount
                        0,       # credit
                        order_amount,  # food_material_sales is the order_amount
                        0,       # royalty_sales
                        0,       # pos_usage_fee
                        0,       # cash_deposit will be set from the bank payment amount
                        0,       # card_deposit
                        0        # receivables is empty for this case
                    ))

                # Insert into AROrderDetails (batch insert)
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
                
                # Insert into ARTransactionsLedger (batch insert)
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
                        pos_usage_fee,
                        cash_deposit,
                        card_deposit,
                        receivables
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                cursor.executemany(insert_ledger_query, ledger_data)
                logging.debug(f"ARTransactionsLedger 삽입 성공: {len(ledger_data)}건")

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
    return render_template('upload_orders.html', form=form)

# 미수금액 조회
@app.route('/view_receivables', methods=['GET'])
def view_receivables():
    db = get_db_connection()
    if not db:
        flash('Database connection failed.', 'danger')
        return redirect(url_for('index'))

    cursor = db.cursor(dictionary=True)
    
    try:
        # 검색 파라미터 가져오기
        search_outlet = request.args.get('search_outlet', '').strip()
        
        # 기본 쿼리
        query = """
            SELECT 
                representative_code,
                outlet_name,
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
            query += " WHERE outlet_name LIKE %s"
            params.append(f"%{search_outlet}%")
        
        query += " GROUP BY representative_code, outlet_name"
        
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
    
# 새로운 라우트: 일별 발주 및 입금 내역 조회
@app.route('/view_daily_transactions', methods=['GET'])
def view_daily_transactions():
    try:
        # 검색 파라미터 가져오기
        search_outlet = request.args.get('search_outlet', '').strip()
        selected_year = request.args.get('year', datetime.now().year, type=int)
        selected_month = request.args.get('month', datetime.now().month, type=int)
        
        # logging.debug(f"조회 요청 - 연도: {selected_year}, 월: {selected_month}, 매출처: {search_outlet}")
        
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
        
        # 데이터베이스 연결
        db = get_db_connection()
        if not db:
            flash('데이터베이스 연결에 실패했습니다.', 'danger')
            return redirect(url_for('index'))
        
        try:
            cursor = db.cursor(dictionary=True)
            
            # 수정된 SQL 쿼리 (JOIN 사용)
            query = """
                SELECT 
                    A.representative_code,
                    A.outlet_name,
                    C.collector_key,
                    C.manager,
            """
            
            # 동적 SQL 쿼리 생성: 각 일자별로 debit과 credit 합계를 계산
            for day in range(1, 32):
                query += f"""
                    SUM(CASE WHEN DAY(A.transaction_date) = {day} THEN A.debit ELSE 0 END) AS `day_{day}_debit`,
                    SUM(CASE WHEN DAY(A.transaction_date) = {day} THEN A.credit ELSE 0 END) AS `day_{day}_credit`,
                """
            
            # 마지막 콤마와 공백 제거
            query = query.rstrip(', \n')
            
            # FROM 절과 JOIN 절
            query += """
                FROM 
                    ARTransactionsLedger A
                JOIN 
                    ARClientMaster C ON A.representative_code = C.representative_code
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
            
            # GROUP BY 절
            query += """
                GROUP BY 
                    A.representative_code, A.outlet_name, C.collector_key, C.manager
                ORDER BY 
                    A.representative_code, A.outlet_name
            """
            
            logging.debug(f"실행할 쿼리: {query}")
            logging.debug(f"쿼리 파라미터: {params}")
            
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
            
            # logging.debug(f"조회 결과 수: {len(results)}")
            
            # 합계 계산 (옵션)
            sum_total_debit = 0
            sum_total_credit = 0
            for row in results:
                for day in range(1, 32):
                    sum_total_debit += row.get(f'day_{day}_debit', 0) or 0
                    sum_total_credit += row.get(f'day_{day}_credit', 0) or 0
            
            # 전달할 데이터 구조를 준비
            data = []
            for row in results:
                data_row = {
                    'representative_code': row['representative_code'],
                    'outlet_name': row['outlet_name'],
                    'collector_key': row['collector_key'],
                    'manager': row['manager'],
                }
                for day in range(1, 32):
                    debit_key = f'day_{day}_debit'
                    credit_key = f'day_{day}_credit'
                    debit_value = row.get(debit_key, 0) or 0
                    credit_value = row.get(credit_key, 0) or 0
                    data_row[f'{day}일발주'] = f"{int(debit_value):,}" if debit_value else '-'
                    data_row[f'{day}일입금'] = f"{int(credit_value):,}" if credit_value else '-'
                data.append(data_row)
            
            cursor.close()
            db.close()
            
            logging.info(f"{len(results)}개의 일별 발주 및 입금 내역이 조회되었습니다.")
            return render_template(
                'view_daily_transactions.html',
                data=data,
                selected_year=selected_year,
                selected_month=selected_month,
                search_outlet=search_outlet,
                sum_total_debit=f"{int(sum_total_debit):,}" if sum_total_debit else '-',
                sum_total_credit=f"{int(sum_total_credit):,}" if sum_total_credit else '-'
            )
        
        except mysql.connector.Error as db_err:
            logging.error(f"데이터 조회 실패: {db_err}")
            flash('데이터를 조회하는 중 오류가 발생했습니다.', 'danger')
            cursor.close()
            db.close()
            return redirect(url_for('index'))
    except Exception as e:
        logging.error(f"예기치 않은 오류: {e}")
        flash('데이터를 처리하는 중 오류가 발생했습니다.', 'danger')
        cursor.close()
        db.close()
        return redirect(url_for('index'))



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

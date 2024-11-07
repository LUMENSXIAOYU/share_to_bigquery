import pandas as pd
from google.cloud import bigquery
import os
import json
import tempfile
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from datetime import datetime
from google.oauth2 import service_account

# 将 Google Cloud 凭证环境变量写入临时文件
credentials_info = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not credentials_info:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set or is empty.")

# 创建临时文件来存储 Google Cloud 凭证
with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as temp_credentials_file:
    temp_credentials_file.write(credentials_info)
    temp_credentials_path = temp_credentials_file.name

# 使用临时凭证文件创建 BigQuery 客户端
try:
    credentials = service_account.Credentials.from_service_account_file(temp_credentials_path)
    client = bigquery.Client(credentials=credentials)
    print("[Ok] Google Cloud credentials loaded successfully.")
finally:
    # 删除临时文件以确保安全
    os.remove(temp_credentials_path)

# OneDrive 用户名和密码
username = "XIAOYU.ZENG@lumens.sg"
pw = "Sft8253h!"

# OneDrive 路径
url = "https://lumensautopl-my.sharepoint.com/personal/jiawen_lee_lumens_sg/"
file_urls = [
    "Documents/Finance%20%2D%20External/DTB/2410 October Daily Transaction Book - Lumens.xlsx",
    "Documents/Finance%20%2D%20External/DTB/2411 November Daily Transaction Book - Lumens.xlsx"
]
sheet_name = "Billing Record (CRM)"  # 指定工作表的名称

# SharePoint 客户端设置
ctx = ClientContext(url).with_credentials(UserCredential(username, pw))

# 下载并处理文件的函数
def download_from_one_drive(file_path, file_url):
    try:
        with open(file_path, "wb") as local_file:
            ctx.web.get_file_by_server_relative_url(file_url).download(local_file).execute_query()
        print(f"[Ok] File has been downloaded to: {file_path}")
    except Exception as e:
        print(f"[Error] Failed to download file from {file_url}. Error: {e}")

# 临时文件夹，用于存储下载的文件和生成的 CSV
with tempfile.TemporaryDirectory() as temp_dir:
    df_list = []
    for file_url in file_urls:
        # 定义文件路径
        file_path = os.path.join(temp_dir, file_url.split('/')[-1])
        download_from_one_drive(file_path, file_url)

        # 读取 Excel 文件
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=2)
            print(f"[Ok] {file_path} loaded:")
            print(df.head())  # 打印数据的前几行
        except Exception as e:
            print(f"[Error] Failed to load Excel file: {file_path}. Error: {e}")
            continue

        # 数据清理
        df = df.iloc[:, :16]  # 选择前16列
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace(r'[^\w]', '', regex=True)
        df = df.replace(r"[^\w\s]", '', regex=True)
        df['billing_date'] = pd.to_datetime(df['billing_date'], errors='coerce')
        df['billing_date'] = df['billing_date'].dt.strftime('%Y-%m-%d')
        df_list.append(df)

    # 合并 DataFrame
    combined_df = pd.concat(df_list, ignore_index=True)

    # 保存合并后的数据为 CSV 文件
    csv_file_path = os.path.join(temp_dir, 'lumens_combined_data.csv')
    combined_df.to_csv(csv_file_path, index=False)
    print(f"[Ok] Data successfully combined and saved to {csv_file_path}")

    # BigQuery 配置
    dataset_id = 'auto_data_ingest'
    table_id = 'billing_record_crm'
    table_ref = client.dataset(dataset_id).table(table_id)

    # 删除表（如果存在）
    try:
        client.delete_table(table_ref)
        print(f"[Ok] Deleted table {dataset_id}.{table_id}.")
    except Exception as e:
        print(f"[Warning] Table {dataset_id}.{table_id} does not exist or could not be deleted. Error: {e}")

    # 配置加载作业
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    # 上传到 BigQuery
    try:
        with open(csv_file_path, 'rb') as csv_file:
            load_job = client.load_table_from_file(csv_file, table_ref, job_config=job_config)
        load_job.result()  # 等待作业完成
        print(f"[Ok] Data loaded to BigQuery table {dataset_id}.{table_id}")
    except Exception as e:
        print(f"[Error] Failed to load data into BigQuery. Error: {e}")

    # 验证数据加载
    try:
        destination_table = client.get_table(table_ref)
        print(f"[Ok] Loaded {destination_table.num_rows} rows into {dataset_id}.{table_id}.")
    except Exception as e:
        print(f"[Error] Could not retrieve table information. Error: {e}")

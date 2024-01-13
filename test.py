import unittest
from unittest.mock import Mock, patch
import pandas as pd

from dags.Recent30DaysStockData import upload_df_to_s3


class TestUploadDfToS3(unittest.TestCase):
    @patch('airflow.providers.amazon.aws.hooks.s3.S3Hook')
    def test_upload_df_to_s3(self, mock_s3hook):
        # Mock 객체 설정
        context = {'task_instance': Mock()}
        df = pd.DataFrame({'data': [1, 2, 3]})
        context['task_instance'].xcom_pull.return_value = df
        bucket_name = 'my_bucket'
        s3_key = 'my_key'

        # 함수 실행
        upload_df_to_s3(**context, bucket_name=bucket_name, s3_key=s3_key)

        # Mock 객체의 메소드가 호출되었는지 확인
        mock_s3hook_instance = mock_s3hook.return_value
        mock_s3hook_instance.load_string.assert_called_once()

if __name__ == '__main__':
    unittest.main()
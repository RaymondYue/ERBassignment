Purpose: 

1. use python (exportcsvfile.py) to export the table from database for backup purpose.
2. use python (importcsvfile.py) to import the data into database for restore purpose.


For export and import purpose, you have to write information below variables in input.py:

import psycopg2
# connect Database
conn = psycopg2.connect(
        host="localhost",
        dbname="dishdb",
        user="postgres",
        password="1234"
    )

# parameters for exportcsvfile.py
table_to_export = "public.comments_comment_rate"  # 包含schema的表名
output_folder="/Users/mac/Downloads/erbhomework/demo"


# parameters for importcsvfile.py
csv_file_path = "/Users/mac/Downloads/erbhomework/demo/public.comments_comment_rate_20250624_105851.csv"
target_table = "public.comments_comment_rate"  # 包含schema的表名

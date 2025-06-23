import psycopg2
# connect DB
conn = psycopg2.connect(
        host="localhost",
        dbname="dishdb",
        user="postgres",
        password="1234"
    )

# parameter for exportcsvfile.py
table_to_export = "public.adminusers_adminuser"  # 包含schema的表名
# public.adminusers_adminuser
#public.listings_two_dish_rice
output_folder="/Users/mac/Downloads/erbhomework/demo"



# parameter for importcsvfile.py
csv_file_path = "/Users/mac/Downloads/erbhomework/demo/public.adminusers_adminuser_20250623_153531export.csv"
target_table = "public.adminusers_adminuser"  # 包含schema的表名


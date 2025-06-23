import psycopg2
import csv
import os
from datetime import datetime
from input import conn, table_to_export, output_folder


def export_table_to_csv(conn, table_name, output_folder):
    """
    将数据库表导出为 CSV 文件

    参数:
        conn: 数据库连接对象
        table_name: 要导出的表名
        output_folder: 输出文件夹路径
    """
    try:
        # 创建输出目录（如果不存在）
        os.makedirs(output_folder, exist_ok=True)

        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"{table_name}_{timestamp}.csv"
        csv_path = os.path.join(output_folder, csv_filename)

        with conn.cursor() as cur, open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            # 获取表结构信息 - 修复：直接获取字段名称
            cur.execute(f"SELECT * FROM {table_name} LIMIT 0")  # 只获取元数据，不获取实际数据
            columns = [desc[0] for desc in cur.description]  # 从游标描述中提取字段名称

            # 创建 CSV writer 并写入表头
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # 写入正确的字段名称

            # 导出表数据
            cur.execute(f"SELECT * FROM {table_name}")
            for row in cur:
                writer.writerow(row)

        print(f"✅ 表 '{table_name}' 已成功导出至: {csv_path}")
        return csv_path

    except Exception as e:
        print(f"❌ 导出表 '{table_name}' 失败: {str(e)}")
        return None

# 以下代码保持不变...
try:
    # 建立数据库连接
    # conn = psycopg2.connect(
    #     host="localhost",
    #     dbname="dishdb",
    #     user="postgres",
    #     password="1234"
    # )

    # 创建游标对象
    cur = conn.cursor()
    #
    # 执行简单的测试查询
    cur.execute("SELECT 1")

    # cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'listings_two_dish_rice' ORDER BY ordinal_position")
    result = cur.fetchone()

    if result and result[0] == 1:
    # if result and result[0]:
        print("✅ 数据库连接成功！")

        # 导出表功能 - 使用带schema的表名
        # table_to_export = "public.adminusers_adminuser"  # 包含schema的表名


        # 导出表到CSV
        exported_file = export_table_to_csv(conn, table_to_export, output_folder)

        if exported_file:
            # 可选：显示导出文件的信息
            file_size = os.path.getsize(exported_file) / 1024  # KB
            print(f"📦 导出文件大小: {file_size:.2f} KB")

            # 可选：预览前几行
            with open(exported_file, 'r', encoding='utf-8') as f:
                print("\n预览前3行数据:")
                for i, line in enumerate(f):
                    if i < 4:  # 表头 + 3行数据
                        print(line.strip())
                    else:
                        break

    else:
        print("⚠️ 连接测试失败，未返回预期结果")

    # 关闭游标和连接
    cur.close()
    conn.close()

except psycopg2.OperationalError as e:
    print(f"❌ 数据库连接失败: {str(e)}")
    print("请检查以下配置:")
    print(f"- 主机: localhost")
    print(f"- 数据库名: dishdb")
    print(f"- 用户名: postgres")
    print(f"- 密码: 1234")
    print(f"- 错误详情: {e}")

except Exception as e:
    print(f"❌ 发生意外错误: {str(e)}")
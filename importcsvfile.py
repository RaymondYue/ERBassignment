import psycopg2
import csv
import os
import re
import pytz
from datetime import datetime, time, date
from input import csv_file_path, target_table, conn

# 设置香港时区
HONG_KONG_TZ = pytz.timezone('Asia/Hong_Kong')


def clean_data(value, data_type, col_name):
    """
    根据数据库字段类型清洗数据

    参数:
        value: 原始数据值
        data_type: PostgreSQL数据类型
        col_name: 字段名称

    返回:
        清洗后的值
    """
    # 处理空值
    if value == '' or value is None:
        return None

    try:
        # 根据数据类型进行清洗
        if 'int' in data_type or 'serial' in data_type:
            # 整型处理
            return int(value) if value.strip() != '' else 0

        elif 'numeric' in data_type or 'real' in data_type or 'double' in data_type or 'float' in data_type:
            # 浮点数处理
            return float(value) if value.strip() != '' else 0.0

        elif 'bool' in data_type:
            # 布尔值处理
            true_values = ['true', 't', 'yes', 'y', '1', '是']
            return value.lower() in true_values

        # 特别注意: 处理带时区的时间戳
        elif 'timestamp with time zone' in data_type.lower() or col_name == 'list_date':
            # 时间戳处理 (带时区)
            try:
                # 尝试解析为带时区的日期时间
                dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S%z')
                if not dt.tzinfo:
                    # 如果没有时区信息，添加香港时区
                    return HONG_KONG_TZ.localize(dt)
                return dt
            except:
                try:
                    # 尝试ISO格式
                    dt = datetime.fromisoformat(value)
                    if not dt.tzinfo:
                        return HONG_KONG_TZ.localize(dt)
                    return dt
                except:
                    # 尝试其他常见格式
                    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y/%m/%d %H:%M:%S', '%d-%m-%Y %H:%M:%S',
                                '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
                        try:
                            dt = datetime.strptime(value, fmt)
                            return HONG_KONG_TZ.localize(dt)
                        except:
                            continue
                    # 使用当前时间 (带香港时区)
                    return datetime.now(HONG_KONG_TZ)

        # 处理日期字段
        elif 'date' in data_type.lower() or col_name == 'edit_date':
            # 日期处理
            try:
                return datetime.strptime(value, '%Y-%m-%d').date()
            except:
                # 尝试其他常见格式
                for fmt in ('%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y'):
                    try:
                        return datetime.strptime(value, fmt).date()
                    except:
                        continue
                return date.today()  # 默认今天日期

        # 处理时间字段
        elif 'time' in data_type.lower() or 'hour' in col_name.lower():
            # 时间处理
            try:
                return datetime.strptime(value, '%H:%M:%S').time()
            except:
                # 尝试其他格式
                for fmt in ('%H:%M', '%I:%M %p', '%H:%M:%S.%f'):
                    try:
                        return datetime.strptime(value, fmt).time()
                    except:
                        continue
                return time(0, 0)  # 默认午夜时间

        # 处理普通时间戳 (不带时区)
        elif 'timestamp' in data_type.lower():
            # 时间戳处理 (不带时区)
            try:
                return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            except:
                # 尝试ISO格式和其他常见格式
                for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y/%m/%d %H:%M:%S', '%d-%m-%Y %H:%M'):
                    try:
                        return datetime.strptime(value, fmt)
                    except:
                        continue
                return datetime.now()  # 默认当前时间

        elif 'json' in data_type or 'jsonb' in data_type:
            # JSON处理
            try:
                # 尝试解析JSON
                import json
                return json.loads(value)
            except:
                # 返回空字典
                return {}

        else:
            # 文本类型处理
            # 移除多余空格和不可见字符
            cleaned = re.sub(r'\s+', ' ', value).strip()

            # 特殊字段处理
            if 'email' in col_name.lower():
                # 基本邮箱格式验证
                if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', cleaned):
                    return cleaned
                return 'invalid@example.com'

            elif 'phone' in col_name.lower() or 'tel' in col_name.lower():
                # 移除非数字字符
                digits = re.sub(r'\D', '', cleaned)
                if len(digits) >= 10:
                    return digits[:15]
                return '0000000000'

            elif 'url' in col_name.lower() or 'website' in col_name.lower():
                # 基本URL验证
                if re.match(r'^https?://', cleaned, re.IGNORECASE):
                    return cleaned
                return f'http://example.com/{cleaned[:50]}'

            return cleaned

    except Exception as e:
        print(f"⚠️ 清洗数据失败 - 列: {col_name}, 值: '{value}', 类型: {data_type}, 错误: {str(e)}")
        # 根据类型返回默认值
        if 'int' in data_type:
            return 0
        elif 'float' in data_type or 'numeric' in data_type:
            return 0.0
        elif 'bool' in data_type:
            return False
        elif 'date' in data_type:
            return date.today()
        elif 'time' in data_type:
            return time(0, 0)
        elif 'timestamp' in data_type:
            return datetime.now(HONG_KONG_TZ) if 'time zone' in data_type.lower() else datetime.now()
        else:
            return 'INVALID_DATA'


def get_table_schema(conn, table_name):
    """
    获取表的完整结构信息

    返回:
        dict: {列名: {'data_type': 数据类型, 'is_nullable': 是否允许空值}}
    """
    try:
        schema, table = table_name.split('.') if '.' in table_name else ('public', table_name)

        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name, udt_name, is_nullable, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = '{schema}' 
                AND table_name = '{table}'
            """)
            schema_info = {}
            for row in cur.fetchall():
                col_name, udt_name, is_nullable, max_length = row
                # 将UDT名称映射到标准数据类型
                if udt_name == 'timestamptz':
                    data_type = 'timestamp with time zone'
                elif udt_name == 'timestamp':
                    data_type = 'timestamp without time zone'
                elif udt_name == 'date':
                    data_type = 'date'
                elif udt_name == 'time':
                    data_type = 'time'
                else:
                    data_type = udt_name

                schema_info[col_name] = {
                    'data_type': data_type,
                    'is_nullable': is_nullable == 'YES',
                    'max_length': max_length
                }
            return schema_info
    except Exception as e:
        print(f"❌ 获取表结构失败: {str(e)}")
        return {}


def upload_csv_to_table(conn, table_name, csv_path, delimiter=',', encoding='utf-8'):
    """
    将CSV文件数据上传到PostgreSQL数据库表（带数据清洗）

    参数:
        conn: 数据库连接对象
        table_name: 目标表名（包含schema，如public.table_name）
        csv_path: CSV文件路径
        delimiter: CSV分隔符
        encoding: 文件编码
    """
    try:
        # 验证CSV文件是否存在
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV文件不存在: {csv_path}")

        # 获取表结构信息
        table_schema = get_table_schema(conn, table_name)
        if not table_schema:
            raise ValueError("无法获取表结构信息")

        with conn.cursor() as cur:
            # 获取CSV文件列名
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f, delimiter=delimiter)
                header = next(reader)
                columns = header

            # 检查CSV列是否在数据库中存在
            missing_columns = [col for col in columns if col not in table_schema]
            if missing_columns:
                print(f"⚠️ 警告: CSV中有数据库不存在的列: {', '.join(missing_columns)}")
                # 只保留数据库存在的列
                columns = [col for col in columns if col in table_schema]

            # 生成列名占位符
            col_names = ', '.join([f'"{col}"' for col in columns])
            placeholders = ', '.join(['%s'] * len(columns))

            # 准备插入语句
            sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

            # 读取并上传数据
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f, delimiter=delimiter)
                next(reader)  # 跳过标题行

                # 分批上传以提高性能
                batch_size = 1000
                batch = []
                processed_rows = 0
                skipped_rows = 0

                for i, row in enumerate(reader):
                    try:
                        cleaned_row = []
                        for idx, value in enumerate(header):
                            if idx >= len(row):
                                # 行数据少于列数
                                cell_value = ''
                            else:
                                cell_value = row[idx]

                            # 只处理数据库存在的列
                            if value not in columns:
                                continue

                            col_info = table_schema[value]
                            data_type = col_info['data_type']
                            is_nullable = col_info['is_nullable']
                            max_length = col_info['max_length']

                            # 清洗数据
                            cleaned_value = clean_data(cell_value, data_type, value)

                            # 处理空值
                            if cleaned_value is None and not is_nullable:
                                # 根据列名提供智能默认值
                                if 'photo' in value.lower():
                                    cleaned_value = 'default_image.jpg'
                                elif 'date' in value.lower():
                                    cleaned_value = date.today()
                                elif 'time' in value.lower() or 'hour' in value.lower():
                                    cleaned_value = time(0, 0)
                                elif 'price' in value.lower() or 'amount' in value.lower():
                                    cleaned_value = 0
                                elif 'boolean' in value.lower() or value.lower().startswith('is_'):
                                    cleaned_value = False
                                else:
                                    cleaned_value = 'N/A'

                            # 检查字符串长度限制
                            if max_length and isinstance(cleaned_value, str) and len(cleaned_value) > max_length:
                                cleaned_value = cleaned_value[:max_length]

                            cleaned_row.append(cleaned_value)

                        # 确保清洗后的行长度与列数一致
                        if len(cleaned_row) == len(columns):
                            batch.append(cleaned_row)
                            processed_rows += 1
                        else:
                            skipped_rows += 1
                            print(f"⚠️ 跳过行 {i + 2}: 清洗后列数不匹配")

                    except Exception as e:
                        skipped_rows += 1
                        print(f"⚠️ 处理行 {i + 2} 失败: {str(e)}")
                        continue

                    # 批量插入
                    if len(batch) >= batch_size:
                        try:
                            cur.executemany(sql, batch)
                            conn.commit()
                            batch = []
                            print(f"✓ 已上传 {processed_rows} 行数据...")
                        except Exception as e:
                            print(f"❌ 批量插入失败: {str(e)}")
                            conn.rollback()
                            # 尝试逐行插入以定位问题行
                            for row_data in batch:
                                try:
                                    cur.execute(sql, row_data)
                                    conn.commit()
                                except Exception as e2:
                                    print(f"❌ 行插入失败: {str(e2)}")
                                    print(f"    问题行数据: {row_data}")
                                    conn.rollback()
                            batch = []

                # 插入剩余数据
                if batch:
                    try:
                        cur.executemany(sql, batch)
                        conn.commit()
                        print(f"✓ 已上传 {processed_rows} 行数据...")
                    except Exception as e:
                        print(f"❌ 批量插入剩余数据失败: {str(e)}")
                        conn.rollback()
                        # 尝试逐行插入以定位问题行
                        for row_data in batch:
                            try:
                                cur.execute(sql, row_data)
                                conn.commit()
                            except Exception as e2:
                                print(f"❌ 行插入失败: {str(e2)}")
                                print(f"    问题行数据: {row_data}")
                                conn.rollback()

        print(f"✅ 数据导入完成! 成功: {processed_rows}, 跳过: {skipped_rows}")
        print(f"📁 表 '{table_name}' 已成功导入数据")
        return True

    except Exception as e:
        print(f"❌ 导入失败: {str(e)}")
        conn.rollback()
        return False


# 使用示例
if __name__ == "__main__":
    # conn = None
    try:
        # 建立数据库连接
        # conn = psycopg2.connect(
        #     host="localhost",
        #     dbname="dishdb",
        #     user="postgres",
        #     password="1234"
        # )
        #
        # # # 设置参数
        # csv_file_path = "/Users/mac/Downloads/erbhomework/public.adminusers_adminuser_20250621_215118.csv"
        # target_table = "public.adminusers_adminuser"  # 包含schema的表名


        # 执行上传
        print("🚀 开始导入数据...")
        success = upload_csv_to_table(
            conn,
            table_name=target_table,
            csv_path=csv_file_path,
            delimiter=',',
            encoding='utf-8'
        )

        if success:
            # 验证上传行数
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {target_table}")
                count = cur.fetchone()[0]
                print(f"📊 表 {target_table} 现在共有 {count} 条记录")

    except Exception as e:
        print(f"❌ 主程序错误: {str(e)}")
    finally:
        # 关闭数据库连接
        if conn:
            conn.close()
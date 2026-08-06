from databases import Database

# DATABASE_URL_2 = "mysql+aiomysql://admin_mhems:Spero@68@2025$@10.108.1.68:3306/Bigdata_2025"
# DATABASE_URL_2 = "postgresql+asyncpg://postgres:spero%40123%232025%24@122.176.232.35:5433/hadoop_2025"
DATABASE_URL_2 = "postgresql+asyncpg://cams_admin:Spero%402026@172.16.60.60:5432/cams_main"
# DATABASE_URL = (
#     "postgresql+asyncpg://postgres:spero%40123%232025%24@122.176.232.35:5433/mems_dash_replica"
# )


database2 = Database(DATABASE_URL_2, min_size=1, max_size=10)


from databases import Database

# DATABASE_URL = "mysql+aiomysql://admin_mhems:Spero@68@2025$@10.108.1.68:3306/mhems_2023"
DATABASE_URL = "mysql+aiomysql://bigdata:JaesbIgbySperoDTeam@172.16.60.66:3306/jaemsmp_2022"
# DATABASE_URL = (
#     "postgresql+asyncpg://postgres:spero%40123%232025%24@122.176.232.35:5433/mems_dash_replica"
# )


database = Database(DATABASE_URL, min_size=1, max_size=10)
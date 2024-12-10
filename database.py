import psycopg
import config

class Database: 
    _db = None

    def _init(self):
        if self._db is None:
            try: 
                self._db = psycopg.connect(f"host={config.DB_HOST} dbname={config.DB_NAME} port={config.DB_PORT} user={config.DB_USER} password={config.DB_PASSWORD}")
                print("Conexão feita com sucesso!")
            except psycopg.OperationalError as e:
                print(f"Erro ao criar a conexão com o banco de dados: [{e}]")
            except Exception as e:
                print(f"Erro ao criar a conexão com o banco de dados: [{e}]")
    
    def get_connection(self):
        self._init()
        return self._db
    
    def close_connection(self):
        self._db.close()
    
    def insert_data(self, query: str | dict, data: str | dict ) -> None:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(query, data)
            print("Inserindo dados no banco...")
            conn.commit()
        except Exception as e:
            print(e)
            
    

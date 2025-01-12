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
    def delete_all(self, query: str):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(query)
            print("Limpando tabela!")
        except Exception as e:
            print("Erro ao limpar tabela", e)

    def insert_data(self, query: str | dict, data: str | dict ) -> None:
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute(query, data)
            print("Inserindo dados no banco...")
            conn.commit()
        except psycopg.errors.UniqueViolation:
            print("Dados já existentes no banco")
            conn.rollback()
        except Exception as e:
            print(e)
    

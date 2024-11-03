import psycopg2
import config

class Database: 
    _db = None

    def _init(self):
        if self._db is None:
            try: 
                self._db = psycopg2.connect(
                    host=config.DB_HOST, 
                    database=config.DB_NAME, 
                    user=config.DB_USER, 
                    password=config.DB_PASSWORD
                )
                print("Conexão feita com sucesso!")
            except psycopg.OperationalError as e:
                print(f"Erro ao criar a conexão com o banco de dados: [{e}]")
            except Exception as e:
                print(f"Erro ao criar a conexão com o banco de dados: [{e}]")
    
    def get_connection(self):
        self._init()
        return self._db
    
    def insert_data(self, data: str | dict ) -> None:
        conn = self.get_connection()
        cur = conn.cursor()
        if type(data) == str:
            cur.execute("INSERT INTO table (nome, endereco, preco) VALUES (%s)", data)
        if type(data) == dict:
            cur.execute("INSERT INTO table (nome, endereco, preco) VALUES (%s, %s, %s)", data["title"], data["description"], data["announce_date"], data["price"], data["size", data["address", data["link"]]])

        conn.commit()
        cur.close()
        conn.close()
    
    def insert_data_query(self, query: str | dict , data: str | dict) -> None:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(query, data)
        conn.commit()
        cur.close()
        conn.close()
        

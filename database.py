import psycopg
import config

class Database: 
    _db = None

    def _init(self):
        if self._db is None:
            try: 
                self._db = psycopg.connect(f"host={config.DB_HOST} dbname={config.DB_NAME} user={config.DB_USER} password={config.DB_PASSWORD}")
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
            # if type(data) == str:
            #     cur.execute("INSERT INTO olx (wscp_titulo, wscp_data_hora, wscp_tamanho, wscp_valor, wscp_descricao, wscp_link, wscp_endereco) VALUES %s", (data))
            # if type(data) == dict:
            #     cur.execute("INSERT INTO olx (wscp_titulo, wscp_data_hora, wscp_tamanho, wscp_valor, wscp_descricao, wscp_link, wscp_endereco) VALUES (%s, %s, %s, %s, %s, %s, %s)", data["title"], data["description"], data["announce_date"], data["price"], data["size", data["address", data["link"]]])
            cur.execute(query, data)
            print("Inserindo dados no banco...")
            conn.commit()
        except Exception as e:
            print(e)
            
    

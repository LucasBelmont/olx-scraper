import cloudscraper
import re as r
from bs4 import BeautifulSoup
from database import Database

class Olx:    
    _base_url = "https://www.olx.com.br/imoveis/venda"
    #URL Filtrada por fazendas
    _base_url_filter = "https://www.olx.com.br/imoveis/terrenos/fazendas"
    #Filtro de Estados
    _estado = { 
        "AC": "estado-ac",
        "AL": "estado-al",
        "AP": "estado-ap",
        "AM": "estado-am",
        "BA": "estado-ba",
        "CE": "estado-ce",
        "DF": "estado-df",
        "ES": "estado-es",
        "GO": "estado-go",
        "MA": "estado-ma",
        "MT": "estado-mt",
        "MS": "estado-ms",
        "MG": "estado-mg",
        "PA": "estado-pa",
        "PB": "estado-pb",
        "PR": "estado-pr",
        "PE": "estado-pe",
        "PI": "estado-pi",
        "RJ": "estado-rj",
        "RN": "estado-rn",
        "RS": "estado-rs",
        "RO": "estado-ro",
        "RR": "estado-rr",
        "SC": "estado-sc",
        "SP": "estado-sp",
        "SE": "estado-se",
        "TO": "estado-to"
    }
    _url = ""

#Método que coleta os dados dos links e alguns dados dos cards de cada página por estado
    def get_imoveis_page(self) -> list[str]:
        scraper = cloudscraper.create_scraper()
        infos = []
        for st, es in self._estado.items():
            for i in range(1, 101):
                self._url = self._base_url_filter + "/" + es + "?o=" + str(i)
                print("Acessando página ", self._url)
                response = scraper.get(self._url)
                soup = BeautifulSoup(response.content, 'html.parser')
                cards = soup.find_all("section", attrs={"data-ds-component": "DS-AdCard"})
                #Verificação se existem dados na página se não tiver pula pro próximo estado
                if cards == [] or cards == None:
                    break
                for card in cards:
                    print("\t\t\t\t\t\t Buscando Dados...")
                    a = card.contents[0]
                    size = card.find("ul", {"data-testid" : "labelGroup"}).text if card.find("ul", {"data-testid" : "labelGroup"}) else "N/D"
                    city = card.contents[2].find(class_="olx-ad-card__bottom").contents[0].find(class_="olx-text olx-text--caption olx-text--block olx-text--regular").text
                    infos.append({"link": a['href'], "size": size, "state": st, "city": city})
        return infos  

#Método que limpa os dados antigos do banco, coleta os todos os dados dos imóveis e insere no banco
    def get_imovel_info(self) -> list[dict]:
        DATABASE = Database()
        DATABASE.delete_all("DELETE FROM olx")
        infos = self.get_imoveis_page()
        scraper = cloudscraper.create_scraper()
        # imoveis = []
        print(f"\t\t\t\t\t\t Foram coletados {len(infos)} resultados!")
        for info in infos:
            link = info["link"] 
            print(f"Visitando: {link}")
            response = scraper.get(link)
            soup = BeautifulSoup(response.content, "html.parser")
            try:
                announce_date = soup.find(class_="olx-color-neutral-100").text if soup.find(class_="olx-color-neutral-100") != None else "N/D"
                title = soup.find("div", id="description-title").find("span", class_="olx-text olx-text--title-medium olx-text--block ad__sc-1l883pa-2 bdcWAn").text if soup.find("div", id="description-title") != None else "Sem Título"
                description = soup.find(attrs={"data-section" : "description"}).find("span", attrs={"data-ds-component": "DS-Text"}).text if soup.find(attrs={"data-section" : "description"}) != None else "Sem Descrição"
                prices = soup.find("span", class_="olx-text olx-text--title-large olx-text--block").text if soup.find("span", class_="olx-text olx-text--title-large olx-text--block") != None else 0
                #Remove o R$ da string
                price = prices.replace("R$", "").strip() if prices != 0 else 0
                #Remove os pontos da string
                price = price.replace(".", "").strip() if prices != 0 else 0
                #Transforma valor de string em float
                price = float(price) if price != 0 else 0
                location = soup.find(id="location").find_all(attrs={"data-ds-component": "DS-Text"})
                #Remove o título da div Location
                location.pop(0)
                city = info["city"]
                #Remove bairro se tiver junto com a cidade separado por vírgula
                city = city.split(",")[0] if r.search(",", city) != None else city
                state = info["state"]
                address = location[0].text + " " + location[1].text
                size = info["size"]
                #Remove os caracteres que não sejam números
                hectares = r.findall("[0-9]", size)
                #Transforma os tamanho separados em arrays em string
                hectares = "".join(hectares) if hectares != [] else 0
                #Converte o valor em float e converte o tamanho em hectares
                hectares = float(hectares) / 10000 if hectares != 0 else 0
                QUERY = """
                    INSERT INTO olx (wscp_titulo, wscp_data_hora, wscp_tamanho, wscp_valor, wscp_descricao, wscp_link, wscp_endereco, wscp_hectares, 
                    wscp_municipio, wscp_estado) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)  
                """
                VALUES = (title, announce_date, size, price, description, link, address, hectares, city, state)
                
                if hectares > 1:
                    DATABASE.insert_data(QUERY, VALUES)
            except Exception as e:
                print("Erro ao acessar e salvar o imovel! ", e)
            finally:
                DATABASE.close_connection()
        print("\t \t \t \t Scraping finalizado!")
        # imovel = [{
        #     "title": title, 
        #     "description": description, 
        #     "announce_date": announce_date,
        #     "price": price, 
        #     "size": size,
        #     "address": address,
        #     "link": link, 
        #     "hectares": hectares,
        #     "state": state,
        #     "city": city,
        # }]
        # imoveis.append(imovel)
        
        # return imoveis
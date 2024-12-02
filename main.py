import cloudscraper
import re as r
from bs4 import BeautifulSoup
from database import Database

class Olx:
    
    _base_url = "https://www.olx.com.br/imoveis/venda"
    _base_url_filter = "https://www.olx.com.br/imoveis/terrenos"
    _estado = { 
    # "AC": "estado-ac",
    # "AL": "estado-al",
    # "AP": "estado-ap",
    # "AM": "estado-am",
    # "BA": "estado-ba",
    # "CE": "estado-ce",
    # "DF": "estado-df",
    # "ES": "estado-es",
    # "GO": "estado-go",
    # "MA": "estado-ma",
    # "MT": "estado-mt",
    # "MS": "estado-ms",
    "MG": "estado-mg",
    # "PA": "estado-pa",
    # "PB": "estado-pb",
    # "PR": "estado-pr",
    # "PE": "estado-pe",
    # "PI": "estado-pi",
    "RJ": "estado-rj",
    # "RN": "estado-rn",
    # "RS": "estado-rs",
    # "RO": "estado-ro",
    # "RR": "estado-rr",
    # "SC": "estado-sc",
    "SP": "estado-sp",
    # "SE": "estado-se",
    # "TO": "estado-to"
}
    _url = ""

    def get_imoveis_page(self) -> list[str]:
        scraper = cloudscraper.create_scraper()
        infos = []
        for es in self._estado.values():
            self._url = self._base_url_filter + "/" + es + "?o=1"
            response = scraper.get(self._url)
            soup = BeautifulSoup(response.content, 'html.parser')
            cards = soup.find_all("section", attrs={"data-ds-component": "DS-AdCard"})
            for card in cards:
                print("\t\t\t\t\t\t Buscando Dados...")
                a = card.contents[0]
                size = card.find("ul", {"data-testid" : "labelGroup"}).text if card.find("ul", {"data-testid" : "labelGroup"}) else "N/D"
                infos.append({"link": a['href'], "size": size})
        return infos  

    def get_imovel_info(self) -> list[dict]:
        DATABASE = Database()
        infos = self.get_imoveis_page()
        scraper = cloudscraper.create_scraper()
        imoveis = []
        print(f"\t\t\t\t\t\t Foram coletados {len(infos)} resultados!")
        for info in infos:
            link = info["link"] 
            print(f"Visitando: {link}")
            response = scraper.get(link)
            soup = BeautifulSoup(response.content, "html.parser")
            announce_date = soup.find(class_="olx-color-neutral-100").text
            title = soup.find(id="description-title").find(attrs={"data-ds-component": "DS-Container"}).contents[0].text
            description = soup.find(attrs={"data-section" : "description"}).find(attrs={"data-ds-component": "DS-Text"}).text
            prices = soup.find(id="price-box-container").find_all(attrs={"data-ds-component": "DS-Text"})
            price = prices[0].text if len(prices) > 0 else "Preço Não Registrado" 
            location = soup.find(id="location").find_all(attrs={"data-ds-component": "DS-Text"})
            location.pop(0)
            address = location[0].text + " " + location[1].text
            size = info["size"]
            hectares = r.findall("[0-9]", size)
            hectares = "".join(hectares) if hectares != [] else 0
            hectares = float(hectares) / 10000 if hectares != 0 else 0
            QUERY = """
                INSERT INTO olx (wscp_titulo, wscp_data_hora, wscp_tamanho, wscp_valor, wscp_descricao, wscp_link, wscp_endereco, wscp_hectares) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)  
            """
            VALUES = (title, announce_date, size, price, description, link, address, hectares)
            DATABASE.insert_data(QUERY, VALUES)
            imovel = [{
                "title": title, 
                "description": description, 
                "announce_date": announce_date,
                "price": price, 
                "size": size,
                "address": address,
                "link": link, 
                "hectares": hectares
            }]
            imoveis.append(imovel)
        
        DATABASE.close_connection()
        return imoveis



if __name__ == "__main__":
    olx = Olx()
    olx.get_imovel_info()
    
import cloudscraper 
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
        links = []
        for es in self._estado.values():
            self._url = self._base_url_filter + "/" + es + "?o=1"
            response = scraper.get(self._url)
            soup = BeautifulSoup(response.content, 'html.parser')
            a = soup.find_all(id=True, attrs={"data-ds-component" : "DS-NewAdCard-Link"})
            for link in a:
                print("\t\t\t\t\t\t Buscando Links...")
                links.append(link['href'])
              
        return links  

    def get_imovel_info(self) -> list[dict]:
        DATABASE = Database()
        links = self.get_imoveis_page()
        scraper = cloudscraper.create_scraper()
        imoveis = []
        print(f"\t\t\t\t\t\t Foram coletados {len(links)} resultados!")
        for link in links: 
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
            size = soup.find(class_="olx-flex") if soup.find(class_="olx-flex") else "N/D"
            # size = size.contents[1].text if size.contents[0].text.upper() == "ÁREA ÚTIL" or size.contents[0].text.upper() == "ÁREA CONSTRUÍDA" else "Não possui tamanho"
            QUERY = """
                INSERT INTO olx (wscp_titulo, wscp_data_hora, wscp_tamanho, wscp_valor, wscp_descricao, wscp_link, wscp_endereco) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)  
            """
            VALUES = (title, announce_date, size, price, description, link, address)
            DATABASE.insert_data(QUERY, VALUES)
            imovel = [{
                "title": title, 
                "description": description, 
                "announce_date": announce_date,
                "price": price, 
                "size": size,
                "address": address,
                "link": link, 
            }]
            imoveis.append(imovel)
        
        DATABASE.close_connection()
        return imoveis



if __name__ == "__main__":
    olx = Olx()
    olx.get_imovel_info()
    
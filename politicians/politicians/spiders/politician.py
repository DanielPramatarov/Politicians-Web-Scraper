
import scrapy
from scrapy import Request, Selector
import sqlite3
from jsonschema import validate
import json 


class SpiderSpider(scrapy.Spider):
    name = 'politician'
    allowed_domains = ['www.parliament.bg/bg/MP']
    start_urls = ['http://www.parliament.bg/bg/MP/']


    
    def parse(self, response):
        
        hrefs = response.xpath('//*[@class="MPBlock"]/div[@class="MPBlock_columns"][2]/div[@class="MPinfo"]/a/@href').getall()
        for href in hrefs:
            new_url = "https://www.parliament.bg" + href
            yield  scrapy.Request(new_url, self.parse_info,dont_filter=True)

    def parse_info(self, response):
        conn = sqlite3.connect('Politicians.db')
        c = conn.cursor()
        
        schema = {
        "type" : "object",
        "properties" : {
            "full_name" : {"type" : "string"},
            "birhday_date" : {"type" : "string"},
            "email" : {"type" : "string"},
            "job" : {"type" : "string"},
            "languages" : {"type" : "string"},
            "political_party" : {"type" : "string"},
            "place_of_birth" : {"type" : "string"},
        },
        }

        
        try:
            c.execute("CREATE TABLE politicians(full_name varchar(120), birthday_date varchar(20), email varchar(25), job varchar(40), languages varchar(80), political_party varchar(180), place_of_birth varchar(180))")
        except:
            pass
        

        first_name = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[3]/strong[1]/text()").get()
        middle_name = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[3]/text()").get()
        surname =  response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[3]/strong[2]/text()").get()
        full_name = first_name + " " + middle_name + " " + surname
        birhday_date = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[1]/text()").get().split()[4]

        if len(response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[1]/text()").get().split()) == 7:

            place_of_birth = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[1]/text()").get().split()[5]+ " " +response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[1]/text()").get().split()[6]
        elif len(response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[1]/text()").get().split()) == 8:
            place_of_birth = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[1]/text()").get().split()[6]
        else:
            place_of_birth = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[1]/text()").get().split()[5]






        if response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul").get().count("<li>") == 7:
            try:

                email = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[7]/a/text()").get()
                job = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[2]/text()").get().split(": ")[1][:-1]
                languages =  ', '.join(response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[3]/text()").get().split(": ")[1:][0][:-1].split(";"))
                political_party = ' '.join(response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[4]/text()").get().split(": ")[1].split()[:-1])
                

                
                validate(instance={"full_name" : full_name, "birhday_date" : birhday_date, "place_of_birth":place_of_birth,"email": email,"job":job,"languages":languages,"political_party":political_party }, schema=schema)
                
                c.execute("INSERT INTO politicians (full_name,birthday_date,email,job,languages,political_party,place_of_birth) VALUES(?,?,?,?,?,?,?)",(full_name,birhday_date,email,job,languages,political_party,place_of_birth))
                conn.commit()
                jsonstr1 = json.dumps({"full_name" : full_name, "birhday_date" : birhday_date, "place_of_birth":place_of_birth,"email": email,"job":job,"languages":languages,"political_party":political_party },ensure_ascii=True, indent=4) 
                with open('data.json', 'w',encoding='ascii') as f:
                    json.dump(jsonstr1,f, ensure_ascii=False,indent=4)


            except:
                pass
       
       
        elif response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul").get().count("<li>") == 5:
            
            try:
                political_party  = ' '.join(response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[2]").get().split(": ")[1].split()[:-1])
                email = response.xpath("/html/body/div/div[5]/div[1]/div[3]/div[4]/div[2]/div/ul/li[5]/a/text()").get()
                validate(instance={"full_name" : full_name, "birhday_date" : birhday_date, "place_of_birth":place_of_birth,"email": email,"job":job,"languages":languages,"political_party":political_party }, schema=schema)

                c.execute("INSERT INTO politicians (full_name,birthday_date,email,job,languages,political_party,place_of_birth) VALUES(?,?,?,?,?,?,?)",(full_name,birhday_date,email,"NONE","NONE",political_party,"NONE"))
                conn.commit()
                jsonstr1 = json.dumps({"full_name" : full_name, "birhday_date" : birhday_date, "place_of_birth":place_of_birth,"email": email,"job":job,"languages":languages,"political_party":political_party }, ensure_ascii=True, indent=4) 
                with open('data.json', 'w',encoding='ascii') as f:
                    json.dump(jsonstr1,f,  ensure_ascii=False,indent=4)
            except:
                pass


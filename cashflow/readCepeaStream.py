#' title......: HTTP, HTTPS, Scraping
#' description: Leitura por stream indicadores etanol cepea.esalq em xls
#' file.......: readCepeaStream.py
#' version....: 0.1.5
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2023-03-14
#' update.....: 2023-03-15
#' library....: requests, scrap, xlrd, os, pandas
#' sample.....: from readCepeaStream import ReadEsalq ; cepeaEtanol=ReadEsalq(hist_ignore=True,indicator=111) ; cepeaEtanol.writeXls()
#'https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx
class ReadEsalq():
    WORKFILE="CEPEAindicator_.xlsx"
    def __init__(self,hist_ignore=False,cepeafile=None,indicator=111,timeout=15):
        self.hist_ignore=True if (hist_ignore!=False) else False
        self.indicatorid=indicator
        self.__confid__()
        self.cepeafile=self.WORKFILE.replace("_",str(indicator)) \
            if (cepeafile is None) else cepeafile
        self.timeout=timeout
        if (self.hist_ignore==False):
            self.dataframe=self.readXLS(optional=False)
            print('Finish with dataframe:[',self.dataframe.shape,'], local loads.')
        else:
            self.__startScrap__()
    def __confid__(self):
        from pandas import DataFrame
        dicIndicators={111:['DIÁRIO','Posto Paulínia','SÃO PAULO','ETANOL','P0020',\
            'ETANOL HIDRATADO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-diario-paulinia.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-diario-paulinia.aspx?id=111',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            53:['DIARIO','SÃO PAULO','SÃO PAULO','AÇÚCAR','P0005','AÇÚCAR CRISTAL BRANCO','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/acucar.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/acucar.aspx?id=53',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],
            113:['DIÁRIO','SÃO PAULO','SÃO PAULO','AÇÚCAR','P0003','AÇÚCAR CRISTAL EMPACOTADO','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/acucar-cristal-empacotado-cepea-esalq-sao-paulo.aspx',
            'https://www.cepea.esalq.usp.br/br/indicador/series/acucar-cristal-empacotado-cepea-esalq-sao-paulo.aspx?id=113',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            114:['DIÁRIO','SÃO PAULO','SÃO PAULO','AÇÚCAR','P0009','AÇÚCAR REFINADO AMORFO','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/acucar-refinado-amorfo-sp.aspx',
            'https://www.cepea.esalq.usp.br/br/indicador/series/acucar-refinado-amorfo-sp.aspx?id=114',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],
            143:['DIÁRIO','SÃO PAULO','SANTOS','AÇÚCAR','P0052','AÇÚCAR CRISTAL ESALQ/BVMF - SANTOS','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/acucar.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/acucar.aspx?id=143',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            50:['MENSAL','Mercado Interno','ALAGOAS','AÇÚCAR','P0005','Açúcar Cristal MI','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/acucar-alagoas-mercado-interno.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/acucar-alagoas-mercado-interno.aspx?id=50',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            207:['SEMANAL','Mercado Interno','ALAGOAS','AÇÚCAR','P0005','Açúcar Cristal MI','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/acucar-alagoas-mercado-interno.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/acucar-alagoas-mercado-interno.aspx?id=207',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            149:['MENSAL','Mercado Interno','PARAÍBA','AÇÚCAR','P0005','Açúcar Cristal MI','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/acucar-paraiba-mercado-interno.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/acucar-paraiba-mercado-interno.aspx?id=149',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',4,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            35:['MENSAL','Mercado Interno','PERNAMBUCO','AÇÚCAR','P0005','Açúcar Cristal MI','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/acucar-pernambuco-mercado-interno.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/acucar-pernambuco-mercado-interno.aspx?id=35',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            103:['SEMANAL','SÃO PAULO','SÃO PAULO','ETANOL','P0020','ETANOL HIDRATADO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol.aspx?id=103',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            104:['SEMANAL','SÃO PAULO','SÃO PAULO','ETANOL','P0016','ETANOL ANIDRO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol.aspx?id=104',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            85:['SEMANAL','SÃO PAULO','SÃO PAULO','ETANOL','P0022','ETANOL HIDRATADO OUTROS FINS','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol.aspx?id=85',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            76:['SEMANAL','MATO GROSSO','MATO GROSSO','ETANOL','P0020','ETANOL HIDRATADO CARB.','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-mt.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-mt.aspx?id=76',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            75:['SEMANAL','MATO GROSSO','MATO GROSSO','ETANOL','P0016','ETANOL ANIDRO CARB.','COM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-mt.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-mt.aspx?id=75',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            100:['SEMANAL','PERNAMBUCO','PERNAMBUCO','ETANOL','P0020','ETANOL HIDRATADO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-pe.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-pe.aspx?id=100',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            101:['SEMANAL','PERNAMBUCO','PERNAMBUCO','ETANOL','P0016','ETANOL ANIDRO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-pe.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-pe.aspx?id=101',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            209:['SEMANAL','ALAGOAS','ALAGOAS','ETANOL','P0020','ETANOL HIDRATADO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-al.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-al.aspx?id=209',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            208:['SEMANAL','ALAGOAS','ALAGOAS','ETANOL','P0016','ETANOL ANIDRO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-al.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-al.aspx?id=208',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            210:['SEMANAL','PARAÍBA','PARAÍBA','ETANOL','P0020','ETANOL HIDRATADO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-pb.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-pb.aspx?id=210',\
            DataFrame(index=[], columns=['id','data','valor']).astype({'id':int,'valor':float}),\
            ['data','valor'],'data',3,['id','data','valor','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','valor':float},'A:B','B:K'],\
            211:['SEMANAL','PARAÍBA','PARAÍBA','ETANOL','P0016','ETANOL ANIDRO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-pb.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-pb.aspx?id=211',\
            DataFrame(index=[], columns=['id','data','valor']).astype({'id':int,'valor':float}),\
            ['data','valor'],'data',3,['id','data','valor','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','valor':float},'A:B','B:K'],\
            120:['SEMANAL','Vendas Internas','GOIÁS','ETANOL','P0020','ETANOL HIDRATADO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-go.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-go.aspx?id=120',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            119:['SEMANAL','GOIÁS','GOIÁS','ETANOL','P0016','ETANOL ANIDRO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-go.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-go.aspx?id=119',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L'],\
            125:['SEMANAL','Vendas para outros Estados','GOIÁS','ETANOL','P0020','ETANOL HIDRATADO CARB.','SEM',\
            'https://www.cepea.esalq.usp.br/br/indicador/etanol-semanal-go.aspx',\
            'https://www.cepea.esalq.usp.br/br/indicador/series/etanol-semanal-go.aspx?id=125',\
            DataFrame(index=[], columns=['id','data','real','usd']).astype({'id':int,'real':float,'usd':float}),\
            ['data','real', 'usd'],'data',3,['id','data','real','usd','freq','mercado','estado','produto',\
            'codProd','descProd','imposto'],{'id':int,'data':'datetime64[ns]','real':float,'usd':float},'A:C','B:L']\
            }
        assert (dicIndicators.get(self.indicatorid) is not None), 'confid: Invalid Indicator={}! Available:'+\
            str(dicIndicators.keys()).replace(' ','').replace('dict_keys(','').replace(')','').format(self.indicatorid)
        self.frequency=dicIndicators.get(self.indicatorid)[0]
        self.market=dicIndicators.get(self.indicatorid)[1]
        self.city=dicIndicators.get(self.indicatorid)[2]
        self.product=dicIndicators.get(self.indicatorid)[3]
        self.prodCode=dicIndicators.get(self.indicatorid)[4]
        self.descProd=dicIndicators.get(self.indicatorid)[5]
        self.tax=dicIndicators.get(self.indicatorid)[6]
        self.source=dicIndicators.get(self.indicatorid)[7]
        self.link=dicIndicators.get(self.indicatorid)[8]
        self.__emptydf=dicIndicators.get(self.indicatorid)[9]
        self.__sourcecols=dicIndicators.get(self.indicatorid)[10]
        self.__idfrom=dicIndicators.get(self.indicatorid)[11]
        self.__sourceskip=dicIndicators.get(self.indicatorid)[12]
        self.__localcols=dicIndicators.get(self.indicatorid)[13]
        self.__localtypes=dicIndicators.get(self.indicatorid)[14]
        self.__sourcexcols=dicIndicators.get(self.indicatorid)[15]
        self.__localxcols=dicIndicators.get(self.indicatorid)[16]
    def __startScrap__(self):
        from cashflow.scrap.scrap import ScrapingO
        from pandas import DataFrame, read_excel, to_datetime
        import xlrd
        tmpCount=0
        try:
            cepeaStream=ScrapingO(url=self.link,timeout=self.timeout,stream=True,headers={'User-Agent':\
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '+\
                '(KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'})
        except Exception as reqErr:
            raise Exception('Fail on scrapping, verify current dataframe status, cause: ['+str(reqErr)+']')
        if cepeaStream.ok==False:
            raise Exception('Fail on scrapping, this source was not ok: ['+str(cepeaStream.ok)+']! Verify timeout.')
        tmpBook=xlrd.open_workbook(file_contents=cepeaStream.response.content,ignore_workbook_corruption=True)
        dfCepea=read_excel(tmpBook,sheet_name=0,usecols=self.__sourcexcols,skiprows=self.__sourceskip,engine='xlrd')
        dfCepea.columns=self.__sourcecols
        if (self.__idfrom is not None):
            dfCepea['id']=dfCepea[self.__idfrom].apply(lambda x:x.split("/")[2])+\
                dfCepea[self.__idfrom].apply(lambda x:x.split("/")[1])+\
                dfCepea[self.__idfrom].apply(lambda x:x.split("/")[0])
            dfCepea.data=to_datetime(dfCepea.id,format='%Y%m%d')
        dfCepea['freq']=self.frequency
        dfCepea['mercado']=self.market
        dfCepea['estado']=self.city
        dfCepea['produto']=self.product
        dfCepea['codProd']=self.prodCode
        dfCepea['descProd']=self.descProd
        dfCepea['imposto']=self.tax
        self.dataframe=dfCepea[self.__localcols].astype(self.__localtypes)
        tmpCount+=1
        print('Finish with dataframe:[',self.dataframe.shape,'], scraps:[',tmpCount,'].')
    def writeXls(self):
        from pandas import DataFrame
        assert isinstance(self.dataframe, DataFrame), 'writeXls() parameter dataframe={} not a Pandas DataFrame'.format(self.dataframe)
        self.dataframe.to_excel(self.cepeafile)
        print('File:[',self.cepeafile,'] exported!')
    def readXLS(self,optional=False):
        import os.path
        from pandas import ExcelFile, DataFrame
        tmpExist=False
        tmpExist=os.path.isfile(self.cepeafile)
        if (tmpExist==False):
            if (optional==True):
                return self.__emptydf
            else:
                raise Exception('Historical file:['+self.cepeafile+'] not available!')
        self.dataXLS=ExcelFile(self.cepeafile)
        if not 'Sheet1' in self.dataXLS.sheet_names:
            raise Exception('ReadPresBR historical data not available! [Sheet1]')
        histData=self.dataXLS.parse('Sheet1',index_col=None,na_values=['NA'],usecols=self.__localxcols)
        return histData
#import requests
#from pandas import read_excel
#import xlrd
#data=requests.get('https://www.cepea.esalq.usp.br/br/indicador/series/etanol-diario-paulinia.aspx?id=111',stream=True,headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}).raw.read(decode_content=True)
#book = xlrd.open_workbook(file_contents=data, ignore_workbook_corruption=True)
#df = read_excel(book,sheet_name=0,usecols='A:C',skiprows=3,engine='xlrd')
#print(df.head())

#' title......: HTTP, HTTPS, Scraping
#' description: Leitura do site COMEX stat, importacao e exportacao geral 
#' file.......: readComexStat.py
#' version....: 0.1.1
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2022-03-03
#' update.....: 2022-03-09
#' library....: requests, scrap, BeautifulSoup, datetime, json, time, os, pandas, dateutil
#' sample.....: from readComexStat import ReadCOMEX ; comexData=ReadCOMEX(start_date='2023-01-01',end_date='2023-02-01',hist_ignore=True) ; comexData.writeXls()
#'http://comexstat.mdic.gov.br/pt/geral/75535
#'Filtro: NCM - Nomenclatura Comum Mercosul (cesta:22071010/1090/2011/2019), consultar por ano (1Ano), Mes (limitar a 3, detalhando), tipo: exportacao/importacao
#'Detalhamento: NCM, UF do Produto, URF, País, Via, CGCE Nivel 3, ISIC Classe, (bloco economico, seção, CUCI item)
#'Valores: FOB, Quilograma, Quantidade Estatistica, tipo de exibicao:vertical(ou horizontal)
#'http://api.comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22:%222022%22,%22yearEnd%22:%222022%22,%22typeForm%22:2,%22typeOrder%22:2,%22filterList%22:%5B%7B%22id%22:%22noNcmpt%22,%22text%22:%22NCM%20-%20Nomenclatura%20Comum%20do%20Mercosul%22,%22route%22:%22/pt/product/ncm%22,%22type%22:%222%22,%22group%22:%22sh%22,%22groupText%22:%22Sistema%20Harmonizado%20(SH)%22,%22hint%22:%22fieldsForm.general.noNcm.description%22,%22placeholder%22:%22NCM%22%7D%5D,%22filterArray%22:%5B%7B%22item%22:%5B%2222071010%22,%2222071090%22,%2222072019%22,%2222072011%22%5D,%22idInput%22:%22noNcmpt%22%7D%5D,%22rangeFilter%22:%5B%5D,%22detailDatabase%22:%5B%7B%22id%22:%22noNcmpt%22,%22text%22:%22NCM%20-%20Nomenclatura%20Comum%20do%20Mercosul%22,%22parentId%22:%22coNcm%22,%22parent%22:%22C%C3%B3digo%20NCM%22%7D,%7B%22id%22:%22noUf%22,%22text%22:%22UF%20do%20Produto%22%7D,%7B%22id%22:%22noUrf%22,%22text%22:%22URF%22%7D,%7B%22id%22:%22noPaispt%22,%22text%22:%22Pa%C3%ADs%22%7D%5D,%22monthDetail%22:true,%22metricFOB%22:true,%22metricKG%22:true,%22metricStatistic%22:true,%22monthStart%22:%2201%22,%22monthEnd%22:%2212%22,%22formQueue%22:%22general%22,%22langDefault%22:%22pt%22,%22monthStartName%22:%22Janeiro%22,%22monthEndName%22:%22Dezembro%22%7D
#'http://api.comexstat.mdic.gov.br/general?filter={"yearStart"...
class ReadCOMEX():
    WORKFILE="COMEXStat.xlsx"
    def __init__(self,start_date=None,end_date=None,stepby=1,typeorder=0,filterconf=None,hist_ignore=False,comexfile=None):
        from datetime import date
        from dateutil.relativedelta import relativedelta
        if (typeorder not in [0,1,2]):
            raise Exception('Tipo de movimentacao invalida! informar typeorder=0 (Ambos), 1 (Exportacao) ou 2 (Importacao).')
        try:
            self.typeorder=typeorder
            self.start_date=date.today() if (start_date is None) \
                else date.fromisoformat(start_date)
            self.end_date=date.today()+relativedelta(months=1) if (end_date is None) \
                else date.fromisoformat(end_date)
            self.stepby=stepby
            self.hist_ignore=True if (hist_ignore!=False) else False
            self.comexfile=self.WORKFILE if (comexfile is None) else comexfile
        except Exception as dtError:
            raise Exception('Data invalida! yyyy-mm-dd[start_date:'+start_date+', end_date:'+\
                end_date+', stepby='+str(stepby)+']'+str(dtError))
        if (int((self.end_date.year-self.start_date.year)*12+self.end_date.month-self.start_date.month)<0):
            raise Exception('Data final invalida! informar acima da inicial, yyyy-mm-dd[start_date:'\
                +start_date+', end_date:'+end_date+', stepby='+str(stepby)+']')
        self.filterconf=filterconf
        self.__filterconf__()
        self.__startLoop__()
    def __daterange__(self):
        from dateutil.relativedelta import relativedelta
        tmpMonths=int((self.end_date.year-self.start_date.year)*12+self.end_date.month-self.start_date.month)+1
        tmpPrev=None
        for m in range(0,tmpMonths,self.stepby):
            if (tmpPrev!=None):
                yield tmpPrev,self.start_date+relativedelta(months=m)
            tmpPrev=self.start_date+relativedelta(months=m+(1 if tmpPrev!=None else 0))
    def __filterconf__(self):
        import json
        if (self.filterconf==None):
            tmpJson=json.loads('{"yearStart":"2022","yearEnd":"2022","typeForm":1,"typeOrder":1,"filterList":'\
               +'[{"id":"noNcmen","text":"NCM","route":"/en/product/ncm","type":"2","group":"sh","groupText":"Harmonized System (HS)",'\
               +'"hint":"fieldsForm.general.noNcm.description","placeholder":"NCM"}],"filterArray":'\
               +'[{"item":["22071010","22071090","22072010","22072019"],"idInput":"noNcmen"}],"rangeFilter":[],'\
               +'"detailDatabase":[{"id":"noBlocoen","text":"Economic Block"},{"id":"noUf","text":"State"},'\
               +'{"id":"noPaisen","text":"Country"},{"id":"noVia","text":"Via"},{"id":"noUrf","text":"URF"},'\
               +'{"id":"noNcmen","text":"NCM","parentId":"coNcm","parent":"NCM Code"},{"id":"noSh6en","text":"Subheading (SH6)",'\
               +'"parentId":"coSh6","parent":"SH6 Code"},{"id":"noSh4en","text":"Heading (SH4)","parentId":"coSh4","parent":"SH4 Code"}'\
               +',{"id":"noSh2en","text":"Chapter (SH2)","parentId":"coSh2","parent":"SH2 Code"},{"id":"noSecen","text":"Section",'\
               +'"parentId":"coNcmSecrom","parent":"Section Code"},{"id":"noCgceN3en","text":"BEC Level 3","parentId":"coCgceN3",'\
               +'"parent":"BEC Level 3 Code"},{"id":"noCgceN2en","text":"BEC Level 2","parentId":"coCgceN2","parent":"BEC Level 2 Code"}'\
               +',{"id":"noCgceN1en","text":"BEC Level 1","parentId":"coCgceN1","parent":"BEC Level 1 Code"},{"id":"noCuciItemen",'\
               +'"text":"SITC Basic Heading","parentId":"coCuciItem","parent":"SITC Basic Heading Code"},{"id":"noCuciSuben",'\
               +'"text":"SITC Subgroup","parentId":"coCuciSub","parent":"SITC Subgroup Code"},{"id":"noCuciPosen","text":"SITC Group"'\
               +',"parentId":"coCuciPos","parent":"SITC Group Code"},{"id":"noCuciCapen","text":"SITC Division","parentId":"coCuciCap",'\
               +'"parent":"SITC Division Code"},{"id":"noCuciSecen","text":"SITC Section","parentId":"coCuciSec",'\
               +'"parent":"SITC Section Code"},{"id":"noIsicClassen","text":"ISIC Class","parentId":"coIsicClass","parent":"ISIC Class Code"}'\
               +',{"id":"noIsicGroupen","text":"ISIC Group","parentId":"coIsicGroup","parent":"ISIC Group Code"},'\
               +'{"id":"noIsicDivisionen","text":"ISIC Division","parentId":"coIsicDivision","parent":"ISIC Division Code"},'\
               +'{"id":"noIsicSectionen","text":"ISIC Section","parentId":"coIsicSection","parent":"ISIC Section Code"}],'\
               +'"monthDetail":true,"metricFOB":true,"metricKG":true,"metricStatistic":true,"monthStart":"01",'\
               +'"monthEnd":"01","formQueue":"general","langDefault":"en","monthStartName":"January","monthEndName":"January"}')
            #tmpJson=json.loads('{"yearStart":"2023","yearEnd":"2023","typeForm":1,"typeOrder":2,"filterList":[{"id":"noNcmpt","text":"NCM - Nomenclatura Comum do Mercosul"'\
            #   +',"route":"/pt/product/ncm","type":"2","group":"sh","groupText":"Sistema Harmonizado (SH)","hint":"fieldsForm.general.noNcm.description","placeholder":"NCM"}]'\
            #   +',"filterArray":[{"item":["22071010","22071090","22072010","22072019"],"idInput":"noNcmpt"}],"rangeFilter":[],'\
            #   +'"detailDatabase":[{"id":"noBlocopt","text":"Bloco Econômico"},{"id":"noUf","text":"UF do Produto"},{"id":"noVia","text":"Via"}'\
            #   +',{"id":"noUrf","text":"URF"},{"id":"noNcmpt","text":"NCM - Nomenclatura Comum do Mercosul","parentId":"coNcm","parent":"Código NCM"}'\
            #   +',{"id":"noPaispt","text":"País"},{"id":"noSecpt","text":"Seção","parentId":"coNcmSecrom","parent":"Codigo Seção"},{"id":"noCgceN3pt",'\
            #   +'"text":"CGCE Nível 3","parentId":"coCgceN3","parent":"Código CGCE Nível 3"},{"id":"noCuciItempt","text":"CUCI Item","parentId":"coCuciItem"'\
            #   +',"parent":"Código CUCI Item"},{"id":"noCgceN1pt","text":"CGCE Nível 1","parentId":"coCgceN1","parent":"Código CGCE Nível 1"},'\
            #   +'{"id":"noIsicClasspt","text":"ISIC Classe","parentId":"coIsicClass","parent":"Código ISIC Classe"}],'\
            #   +'"monthDetail":true,"metricFOB":true,"metricKG":true,"metricStatistic":true,"monthStart":"01","monthEnd":"01",'\
            #   +'"formQueue":"general","langDefault":"pt","monthStartName":"Janeiro","monthEndName":"Janeiro"}')
        else:
            tmpJson=json.loads(self.filterconf)
        if ('typeOrder' not in tmpJson or tmpJson['typeOrder'] not in [1,2]):
            raise Exception('Tipo de movimentacao invalida! informar typeOrder=1 (Exportacao) ou 2 (Importacao).')
        #tmpJson.['typeOrder']=1
        self.filterconf=tmpJson
    def __startLoop__(self):
        from cashflow.scrap.scrap import ScrapingO
        import time, json #, urllib, locale
        from pandas import DataFrame, json_normalize, concat #, Series
        self.listCOMEX={}
        tmpCount=0
        #locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8') #nao precisa traduzir app suporta en
        if (self.hist_ignore==False):
            self.dataframe=self.readXLS(optional=True)
        else:
            self.dataframe=DataFrame(index=[], columns=['year','month','ncmCode','ncmDescription','country',\
                'economicBlock','state','via','urf','sh6Code','sh6Description','sh4Code',\
                'sh4Description','sh2Code','sh2Description','sectionCode','sectionDescription',\
                'becLevel3','becLevel3Description','becLevel2','becLevel2Description',\
                'becLevel1','becLevel1Description','sitcBHCode','sitcBHDescription',\
                'sitcSGCode','sitcSGDescription','sitcGCode','sitcGDescription',\
                'sitcDivCode','sitcDivDescription','sitcSecCode','sitcSecDescription',\
                'isicClassCode','isicClassDescription','isicGroupCode','isicGroupDescription',\
                'isicDivCode','isicDivDescription','isicSecCode','isicSecDescription','vlrFob',\
                'netWeight','quantity','startDate','endDate','typeOrder',\
                'NameOrder','success','message','process_info'])
        self.dataframe=self.dataframe.astype({'year':int,'month':int,'ncmCode':str,\
            'ncmDescription':str,'country':str,'economicBlock':str,'state':str,\
            'via':str,'urf':str,'sh6Code':str,'sh6Description':str,'sh4Code':str,'sh4Description':str,\
            'sh2Code':str,'sh2Description':str,'sectionCode':str,'sectionDescription':str,'becLevel3':str,\
            'becLevel3Description':str,'becLevel2':str,'becLevel2Description':str,'becLevel1':str,\
            'becLevel1Description':str,'sitcBHCode':str,'sitcBHDescription':str,'sitcSGCode':str,\
            'sitcSGDescription':str,'sitcGCode':str,'sitcGDescription':str,'sitcDivCode':str,\
            'sitcDivDescription':str,'sitcSecCode':str,'sitcSecDescription':str,'isicClassCode':str,\
            'isicClassDescription':str,'isicGroupCode':str,'isicGroupDescription':str,'isicDivCode':str,\
            'isicDivDescription':str,'isicSecCode':str,'isicSecDescription':str,'vlrFob':float,\
            'netWeight':float,'quantity':float,'startDate':int,'endDate':int,\
            'typeOrder':int,'NameOrder':str,'success':bool,'message':str,'process_info':str})
        for startDate,endDate in self.__daterange__():
            print([startDate.strftime("%Y-%B"),endDate.strftime("%Y-%B"),self.typeorder])
            self.filterconf["yearStart"]=str(startDate.year)
            self.filterconf["yearEnd"]=str(endDate.year)
            self.filterconf["monthStart"]=str(startDate.month).zfill(2)
            self.filterconf["monthEnd"]=str(endDate.month).zfill(2)
            self.filterconf["monthStartName"]=startDate.strftime('%B').title()
            self.filterconf["monthEndName"]=endDate.strftime('%B').title()
            lstTypeOrder=[self.typeorder] if (self.typeorder in [1,2]) else [1,2]
            for loopTypeOrder in lstTypeOrder:
                if (len(self.dataframe.loc[(self.dataframe['startDate']\
                    ==int(startDate.strftime("%Y%m")))&(self.dataframe['endDate']\
                    ==int(endDate.strftime("%Y%m")))&(self.dataframe.typeOrder\
                    ==loopTypeOrder)&(self.dataframe['success']==True),'startDate'])>0):
                    continue
                self.filterconf["typeOrder"]=loopTypeOrder
                tmpURL='http://api.comexstat.mdic.gov.br/general?filter='+json.dumps(self.filterconf,separators=(',', ':')) #urllib.parse.quote(str(self.filterconf))
                try:
                    tmpComexStat=ScrapingO(url=tmpURL,timeout=10)
                except Exception as reqErr:
                    raise Exception('Fail on scrapping, verify current dataframe/listCOMEX status, cause: ['+str(reqErr)+']')
                if tmpComexStat.ok==True:
                    #self.comexobj=tmpComexStat
                    self.listCOMEX[int(startDate.strftime("%Y%m"))]={'retorno':json.loads(tmpComexStat.content)} #json
                    tmpDFret=json_normalize(json.loads(tmpComexStat.content),max_level=1)\
                        [['success','message','processo_info']] #le cabecalho nivel 0
                    tmpDFcomex=json_normalize(json.loads(tmpComexStat.content),record_path=[['data','list']]).\
                        rename(index=str,columns={'coAno':'year','coMes':'month',\
                            'coNcm':'ncmCode','noNcmen':'ncmDescription',\
                            'noPaisen':'country','noBlocoen':'economicBlock',\
                            'noUf':'state','noVia':'via',\
                            'noUrf':'urf','coSh6':'sh6Code',\
                            'noSh6en':'sh6Description','coSh4':'sh4Code',\
                            'noSh4en':'sh4Description','coSh2':'sh2Code',\
                            'noSh2en':'sh2Description','coNcmSecrom':'sectionCode',\
                            'noSecen':'sectionDescription','coCgceN3':'becLevel3',\
                            'noCgceN3en':'becLevel3Description',\
                            'coCgceN2':'becLevel2',\
                            'noCgceN2en':'becLevel2Description',\
                            'coCgceN1':'becLevel1',\
                            'noCgceN1en':'becLevel1Description',\
                            'coCuciItem':'sitcBHCode',\
                            'noCuciItemen':'sitcBHDescription',\
                            'coCuciSub':'sitcSGCode',\
                            'noCuciSuben':'sitcSGDescription',\
                            'coCuciPos':'sitcGCode',\
                            'noCuciPosen':'sitcGDescription',\
                            'coCuciCap':'sitcDivCode',\
                            'noCuciCapen':'sitcDivDescription',\
                            'coCuciSec':'sitcSecCode',\
                            'noCuciSecen':'sitcSecDescription',\
                            'coIsicClass':'isicClassCode',\
                            'noIsicClassen':'isicClassDescription',\
                            'coIsicGroup':'isicGroupCode',\
                            'noIsicGroupen':'isicGroupDescription',\
                            'coIsicDivision':'isicDivCode','noIsicDivisionen':'isicDivDescription',\
                            'coIsicSection':'isicSecCode','noIsicSectionen':'isicSecDescription',\
                            'vlFob':'vlrFob','kgLiquido':'netWeight','qtEstat':'quantity'})
                    tmpDFcomex['startDate']=int(startDate.strftime("%Y%m"))
                    tmpDFcomex['endDate']=int(endDate.strftime("%Y%m"))
                    tmpDFcomex['typeOrder']=loopTypeOrder
                    tmpDFcomex['NameOrder']={1:'Exportacao',2:'Importacao'}.get(loopTypeOrder)
                    tmpDFcomex['success']=tmpDFret.success.values[0].astype(bool)
                    tmpDFcomex['message']=tmpDFret.message.values[0]
                    tmpDFcomex['process_info']=tmpDFret.processo_info.values[0]
                    if (len(tmpDFcomex.index)>0):
                        tmpDFcomex=tmpDFcomex.astype({'year':int,'month':int,'ncmCode':str,\
                            'ncmDescription':str,'country':str,'economicBlock':str,'state':str,\
                            'via':str,'urf':str,'sh6Code':str,'sh6Description':str,'sh4Code':str,'sh4Description':str,\
                            'sh2Code':str,'sh2Description':str,'sectionCode':str,'sectionDescription':str,'becLevel3':str,\
                            'becLevel3Description':str,'becLevel2':str,'becLevel2Description':str,'becLevel1':str,\
                            'becLevel1Description':str,'sitcBHCode':str,'sitcBHDescription':str,'sitcSGCode':str,\
                            'sitcSGDescription':str,'sitcGCode':str,'sitcGDescription':str,'sitcDivCode':str,\
                            'sitcDivDescription':str,'sitcSecCode':str,'sitcSecDescription':str,'isicClassCode':str,\
                            'isicClassDescription':str,'isicGroupCode':str,'isicGroupDescription':str,'isicDivCode':str,\
                            'isicDivDescription':str,'isicSecCode':str,'isicSecDescription':str,'vlrFob':float,\
                            'netWeight':float,'quantity':float,'startDate':int,'endDate':int,\
                            'typeOrder':int,'NameOrder':str,'success':bool,'message':str,'process_info':str})
                        self.dataframe=concat([self.dataframe,tmpDFcomex],ignore_index=True)
                    tmpCount+=1
                time.sleep(0.5) #loopTypeOrder
            time.sleep(1) #loopDate
        print('Finish with dataframe:[',self.dataframe.shape,'], scraps:[',tmpCount,'].')
    def writeXls(self):
        from pandas import DataFrame
        assert isinstance(self.dataframe, DataFrame), 'writeXls() parameter dataframe={} not a Pandas DataFrame'.format(self.dataframe)
        self.dataframe.to_excel(self.comexfile)
        print('File:[',self.comexfile,'] exported!')
    def readXLS(self,optional=False):
        import os.path
        from pandas import ExcelFile, DataFrame
        tmpExist=False
        tmpExist=os.path.isfile(self.comexfile)
        if (tmpExist==False):
            if (optional==True):
                return DataFrame(index=[], columns=['year','month','ncmCode','ncmDescription','country',\
                    'economicBlock','state','via','urf','sh6Code','sh6Description','sh4Code',\
                    'sh4Description','sh2Code','sh2Description','sectionCode','sectionDescription',\
                    'becLevel3','becLevel3Description','becLevel2','becLevel2Description',\
                    'becLevel1','becLevel1Description','sitcBHCode','sitcBHDescription',\
                    'sitcSGCode','sitcSGDescription','sitcGCode','sitcGDescription',\
                    'sitcDivCode','sitcDivDescription','sitcSecCode','sitcSecDescription',\
                    'isicClassCode','isicClassDescription','isicGroupCode','isicGroupDescription',\
                    'isicDivCode','isicDivDescription','isicSecCode','isicSecDescription','vlrFob',\
                    'netWeight','quantity','startDate','endDate','typeOrder',\
                    'NameOrder','success','message','process_info'])
            else:
                raise Exception('Historical file:['+self.comexfile+'] not available!')
        self.dataXLS=ExcelFile(self.comexfile)
        if not 'Sheet1' in self.dataXLS.sheet_names:
            raise Exception('ReadCOMEX historical data not available! [Sheet1]')
        histData=self.dataXLS.parse('Sheet1',index_col=None,na_values=['NA'],usecols="B:AZ",converters=\
            {'year':int,'month':int,'ncmCode':str,'ncmDescription':str,'country':str,'economicBlock':str,'state':str,\
             'via':str,'urf':str,'sh6Code':str,'sh6Description':str,'sh4Code':str,'sh4Description':str,\
             'sh2Code':str,'sh2Description':str,'sectionCode':str,'sectionDescription':str,'becLevel3':str,\
             'becLevel3Description':str,'becLevel2':str,'becLevel2Description':str,'becLevel1':str,\
             'becLevel1Description':str,'sitcBHCode':str,'sitcBHDescription':str,'sitcSGCode':str,\
             'sitcSGDescription':str,'sitcGCode':str,'sitcGDescription':str,'sitcDivCode':str,\
             'sitcDivDescription':str,'sitcSecCode':str,'sitcSecDescription':str,'isicClassCode':str,\
             'isicClassDescription':str,'isicGroupCode':str,'isicGroupDescription':str,'isicDivCode':str,\
             'isicDivDescription':str,'isicSecCode':str,'isicSecDescription':str,'vlrFob':float,\
             'netWeight':float,'quantity':float,'startDate':int,'endDate':int,\
             'typeOrder':int,'NameOrder':str,'success':bool,'message':str,'process_info':str})
        return histData

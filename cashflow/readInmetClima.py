#' title......: HTTP, HTTPS, Scraping
#' description: Leitura por stream indicadores etanol cepea.esalq em xls
#' file.......: readInmetClima
#' version....: 0.1.2
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2023-03-17
#' update.....: 2023-03-22
#' library....: requests, scrap, xlrd, os, pandas, datetime
#' sample.....: from readInmetClima import ReadInmet ; climaSP=ReadInmet(hist_ignore=True) ; climaSP.writeXls()
#'https://portal.inmet.gov.br/dadoshistoricos climaSP1=ReadInmet(retdetail=2,startyear=2023)
class ReadInmet():
    WORKFILE="climaInmet_lvl.xlsx"
    def __init__(self,hist_ignore=False,inmetfile=None,matchrule=None,timeout=15,startyear=None,endyear=None,retdetail=0):
        from datetime import date
        self.hist_ignore=True if (hist_ignore!=False) else False
        self.startyear=date.today().year if (startyear is None) else int(startyear)
        self.endyear=date.today().year if (endyear is None) else int(endyear)
        assert ((self.endyear-self.startyear)>=0),\
            'Final year:{} cannot be less than initial year!'.format(self.endyear)
        assert (retdetail in [0,1,2]),\
            'Use returns detail:[0.source,1.4timeavg,2.1dayavg]! invalid retdetail {}.'.format(retdetail)
        self.matchrule=['SAO PAULO - MIRANTE'] if (matchrule is None) else matchrule
        self.retdetail=retdetail
        self.inmetfile=self.WORKFILE.replace("_",str(self.retdetail)) \
            if (inmetfile is None) else inmetfile
        self.timeout=timeout
        self.currentyear=date.today().year
        self.__startScrap__()
    def __matches__(self,zipFilesList=None):
        if (type(self.matchrule)!=list):
            self.matchrule=[self.matchrule]
        self.fileread=[]
        for l in range(0,len(self.matchrule)):
            if (len(list(filter(lambda x:self.matchrule[l] in x,zipFilesList)))>0):
                self.fileread=list(filter(lambda x:self.matchrule[l] in x,zipFilesList)) if \
                    (len(self.fileread)==0) else \
                    self.fileread.append(list(filter(lambda x:self.matchrule[l] in x,zipFilesList)))
        return len(self.fileread)
    def __startScrap__(self):
        from cashflow.scrap.scrap import ScrapingO
        from pandas import DataFrame, concat, to_datetime
        import zipfile, io
        tmpCount=0
        if (self.hist_ignore==False):
            self.dataframe=self.readXLS(optional=True)
        else:
            if (self.retdetail==0):
                self.dataframe=DataFrame(index=[],columns=['id','data',\
                    'hora','datahora','regiao',\
                    'uf','estacao','codigo_wmo','latitude','longitude',\
                    'altitude','fundacao','precTotal_mm','pressao',\
                    'pressao_max','pressao_min','radiacao_kjm2',\
                    'tempseco_c','temporvalho_c','temps_maxc',\
                    'temps_minc','temporv_maxc','temporv_minc',\
                    'umidrel_max','umidrel_min','umidrel','vento_gr',\
                    'vento_rajmax','vento_velmax_ms']).astype({'id':int})
            else:
                self.dataframe=DataFrame(index=[],columns=['id','data',\
                    'hora_first','hora_last','datahora_first','datahora_last',\
                    'regiao','fundacao','uf','estacao','codigo_wmo',\
                    'latitude','longitude','altitude','precTotal_mm_min',\
                    'precTotal_mm_max','precTotal_mm_mean','precTotal_mm_median',\
                    'pressao_min','pressao_max','pressao_mean','pressao_median',\
                    'pressao_max_min','pressao_max_max','pressao_max_mean',\
                    'pressao_max_median','pressao_min_min','pressao_min_max',\
                    'pressao_min_mean','pressao_min_median','radiacao_kjm2_min',\
                    'radiacao_kjm2_max','radiacao_kjm2_mean','radiacao_kjm2_median',\
                    'tempseco_c_min','tempseco_c_max','tempseco_c_mean',\
                    'tempseco_c_median','temporvalho_c_min','temporvalho_c_max',\
                    'temporvalho_c_mean','temporvalho_c_median','temps_maxc_min',\
                    'temps_maxc_max','temps_maxc_mean','temps_maxc_median',\
                    'temps_minc_min','temps_minc_max','temps_minc_mean',\
                    'temps_minc_median','temporv_maxc_min','temporv_maxc_max',\
                    'temporv_maxc_mean','temporv_maxc_median','temporv_minc_min',\
                    'temporv_minc_max','temporv_minc_mean','temporv_minc_median',\
                    'umidrel_max_min','umidrel_max_max','umidrel_max_mean',\
                    'umidrel_max_median','umidrel_min_min','umidrel_min_max',\
                    'umidrel_min_mean','umidrel_min_median','umidrel_min','umidrel_max',\
                    'umidrel_mean','umidrel_median','vento_gr_min','vento_gr_max',\
                    'vento_gr_mean','vento_gr_median','vento_rajmax_min',\
                    'vento_rajmax_max','vento_rajmax_mean','vento_rajmax_median',\
                    'vento_velmax_ms_min','vento_velmax_ms_max','vento_velmax_ms_mean',\
                    'vento_velmax_ms_median']).astype({'id':int})
        for y in range(self.startyear,self.endyear+1,1):
            if ((y!=self.currentyear)&(len(self.dataframe.loc[(self.dataframe['id']\
                >int(str(y)+'1200'+(('00' if self.retdetail==1 else '0000') \
                if self.retdetail!=2 else '')))&(self.dataframe['id']\
                <int(str(y)+'1232'+(('00' if self.retdetail==1 else '0000') \
                if self.retdetail!=2 else ''))),'id'])>0)):
                continue
            zipURL='https://portal.inmet.gov.br/uploads/dadoshistoricos/'+str(y)+'.zip'
            try:
                inmetZip=ScrapingO(url=zipURL,timeout=self.timeout,stream=True,headers={'User-Agent':\
                    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, '+\
                    'like Gecko) Chrome/56.0.2924.76 Safari/537.36'})
            except Exception as reqErr:
                raise Exception('Fail on scrapping, verify current dataframe status, cause: ['+str(reqErr)+']')
            if inmetZip.ok==False:
                raise Exception('Fail on scrapping, this source was not ok: ['+str(inmetZip.ok)+']! Verify timeout.')
            qtMatches=self.__matches__(zipfile.ZipFile(io.BytesIO(inmetZip.content), mode="r").namelist())
            assert (qtMatches>-1),'File matche rule:{} not reached!'.format(self.matchrule)
            for d in range(0,qtMatches): #.infolist() / .printdir() / .namelist())
                csvFile=zipfile.ZipFile(io.BytesIO(inmetZip.content),mode="r").read(self.fileread[d])
                dfZip=DataFrame([row.split(';') for row in csvFile.decode('latin').splitlines()])
                print(zipURL,self.fileread[d])
                tmpKey=dfZip[:8][0].values
                tmpVal=dfZip[:8][1].values
                tmpDF=DataFrame(dfZip[9:])
                tmpDF.columns=['data','hora','precTotal_mm','pressao','pressao_max','pressao_min',\
                    'radiacao_kjm2','tempseco_c','temporvalho_c','temps_maxc','temps_minc','temporv_maxc',\
                    'temporv_minc','umidrel_max','umidrel_min','umidrel','vento_gr','vento_rajmax',\
                    'vento_velmax_ms','_']
                del tmpDF['_']
                tmpDF['hora']=tmpDF['hora'].replace('UTC','',regex=True).\
                    replace(' ','',regex=True).replace(':','',regex=True) #.astype(int)
                try:
                    tmpDF['data']=to_datetime(tmpDF['data'],format='%Y-%m-%d')
                except Exception as dtFormat:
                    tmpDF['data']=to_datetime(tmpDF['data'],format='%Y/%m/%d')
                tmpDF['datahora']=to_datetime(tmpDF['data'].dt.strftime('%Y-%m-%d')+' '+\
                    tmpDF['hora'],format='%Y-%m-%d %H%M')
                tmpDF['regiao']=tmpVal[0] if (tmpKey[0].lower().find('regi')>-1) else None
                tmpDF['uf']=tmpVal[1] if (tmpKey[1].lower().strip(' :')=='uf') else None
                tmpDF['estacao']=tmpVal[2] if (tmpKey[2].lower().find('esta')>-1) else None
                tmpDF['codigo_wmo']=tmpVal[3] if (tmpKey[3].lower().find('codigo')>-1) else None
                tmpDF['latitude']=tmpVal[4] if (tmpKey[4].lower().strip(' :')=='latitude') else None
                tmpDF['longitude']=tmpVal[5] if (tmpKey[5].lower().strip(' :')=='longitude') else None
                tmpDF['altitude']=tmpVal[6] if (tmpKey[6].lower().strip(' :')=='altitude') else None
                tmpDF.loc[tmpDF['radiacao_kjm2']=='','radiacao_kjm2']=None
                tmpDF.loc[tmpDF['precTotal_mm']=='','precTotal_mm']=None
                tmpDF.loc[tmpDF['pressao']=='','pressao']=None
                tmpDF.loc[tmpDF['pressao_max']=='','pressao_max']=None
                tmpDF.loc[tmpDF['pressao_min']=='','pressao_min']=None
                tmpDF.loc[tmpDF['tempseco_c']=='','tempseco_c']=None
                tmpDF.loc[tmpDF['temporvalho_c']=='','temporvalho_c']=None
                tmpDF.loc[tmpDF['temps_maxc']=='','temps_maxc']=None
                tmpDF.loc[tmpDF['temps_minc']=='','temps_minc']=None
                tmpDF.loc[tmpDF['temporv_maxc']=='','temporv_maxc']=None
                tmpDF.loc[tmpDF['temporv_minc']=='','temporv_minc']=None
                tmpDF.loc[tmpDF['umidrel_max']=='','umidrel_max']=None
                tmpDF.loc[tmpDF['umidrel_min']=='','umidrel_min']=None
                tmpDF.loc[tmpDF['umidrel']=='','umidrel']=None
                tmpDF.loc[tmpDF['vento_gr']=='','vento_gr']=None
                tmpDF.loc[tmpDF['vento_rajmax']=='','vento_rajmax']=None
                tmpDF.loc[tmpDF['vento_velmax_ms']=='','vento_velmax_ms']=None
                tmpDF.loc[tmpDF['altitude']=='','altitude']=None
                try:
                    tmpDF['fundacao']=to_datetime(tmpVal[7] if (tmpKey[7].lower().find('data de funda')>-1) \
                        else None,format='%Y-%m-%d')
                except Exception as dtFormat:
                    tmpDF['fundacao']=to_datetime(tmpVal[7] if (tmpKey[7].lower().find('data de funda')>-1) \
                        else None,format='%y/%m/%d')
                tmpDF['precTotal_mm']=tmpDF['precTotal_mm'].replace(',','.',regex=True).astype(float)
                tmpDF['pressao']=tmpDF['pressao'].replace(',','.',regex=True).astype(float)
                tmpDF['pressao_max']=tmpDF['pressao_max'].replace(',','.',regex=True).astype(float)
                tmpDF['pressao_min']=tmpDF['pressao_min'].replace(',','.',regex=True).astype(float)
                tmpDF['radiacao_kjm2']=tmpDF['radiacao_kjm2'].replace(',','.',regex=True).astype(float)
                tmpDF['tempseco_c']=tmpDF['tempseco_c'].replace(',','.',regex=True).astype(float)
                tmpDF['temporvalho_c']=tmpDF['temporvalho_c'].replace(',','.',regex=True).astype(float)
                tmpDF['temps_maxc']=tmpDF['temps_maxc'].replace(',','.',regex=True).astype(float)
                tmpDF['temps_minc']=tmpDF['temps_minc'].replace(',','.',regex=True).astype(float)
                tmpDF['temporv_maxc']=tmpDF['temporv_maxc'].replace(',','.',regex=True).astype(float)
                tmpDF['temporv_minc']=tmpDF['temporv_minc'].replace(',','.',regex=True).astype(float)
                tmpDF['umidrel_max']=tmpDF['umidrel_max'].replace(',','.',regex=True).astype(float)
                tmpDF['umidrel_min']=tmpDF['umidrel_min'].replace(',','.',regex=True).astype(float)
                tmpDF['umidrel']=tmpDF['umidrel'].replace(',','.',regex=True).astype(float)
                tmpDF['vento_gr']=tmpDF['vento_gr'].replace(',','.',regex=True).astype(float)
                tmpDF['vento_rajmax']=tmpDF['vento_rajmax'].replace(',','.',regex=True).astype(float)
                tmpDF['vento_velmax_ms']=tmpDF['vento_velmax_ms'].replace(',','.',regex=True).astype(float)
                tmpDF['altitude']=tmpDF['altitude'].replace(',','.',regex=True).astype(float)
                tmpDF['id']=None
                if (self.retdetail==0): #0.detalhado
                    tmpDF['id']=tmpDF['data'].dt.strftime('%Y%m%d')+tmpDF.hora
                    tmpDF=tmpDF[['id','data','hora','datahora','regiao',\
                        'uf','estacao','codigo_wmo','latitude','longitude',\
                        'altitude','fundacao','precTotal_mm','pressao',\
                        'pressao_max','pressao_min','radiacao_kjm2',\
                        'tempseco_c','temporvalho_c','temps_maxc',\
                        'temps_minc','temporv_maxc','temporv_minc',\
                        'umidrel_max','umidrel_min','umidrel','vento_gr',\
                        'vento_rajmax','vento_velmax_ms']]
                elif (self.retdetail==1): #1.por faixa de 4h
                    tmpDF.loc[tmpDF.hora<'0400','id']=tmpDF.loc[tmpDF.hora<'0400','data'].dt.strftime('%Y%m%d')+'00'
                    tmpDF.loc[(tmpDF.hora>='0400')&(tmpDF.hora<'0800'),'id']=\
                        tmpDF.loc[(tmpDF.hora>='0400')&(tmpDF.hora<'0800'),'data'].dt.strftime('%Y%m%d')+'01'
                    tmpDF.loc[(tmpDF.hora>='0800')&(tmpDF.hora<'1200'),'id']=\
                        tmpDF.loc[(tmpDF.hora>='0800')&(tmpDF.hora<'1200'),'data'].dt.strftime('%Y%m%d')+'02'
                    tmpDF.loc[(tmpDF.hora>='1200')&(tmpDF.hora<'1600'),'id']=\
                        tmpDF.loc[(tmpDF.hora>='1200')&(tmpDF.hora<'1600'),'data'].dt.strftime('%Y%m%d')+'03'
                    tmpDF.loc[(tmpDF.hora>='1600')&(tmpDF.hora<'2000'),'id']=\
                        tmpDF.loc[(tmpDF.hora>='1600')&(tmpDF.hora<'2000'),'data'].dt.strftime('%Y%m%d')+'04'
                    tmpDF.loc[tmpDF.hora>='2000','id']=tmpDF.loc[tmpDF.hora>='2000','data'].dt.strftime('%Y%m%d')+'05'
                else: #2.resumir em 1 linha por dia com variacoes
                    tmpDF['id']=tmpDF['data'].dt.strftime('%Y%m%d')
                if (self.retdetail>0):
                    tmpDF=tmpDF.groupby(['id']).agg({\
                        'data':'first','hora':['first','last'],\
                        'precTotal_mm':['min','max','mean','median'],\
                        'pressao':['min','max','mean','median'],'pressao_max':['min','max','mean','median'],\
                        'pressao_min':['min','max','mean','median'],'radiacao_kjm2':['min','max','mean','median'],\
                        'tempseco_c':['min','max','mean','median'],'temporvalho_c':['min','max','mean','median'],\
                        'temps_maxc':['min','max','mean','median'],'temps_minc':['min','max','mean','median'],\
                        'temporv_maxc':['min','max','mean','median'],'temporv_minc':['min','max','mean','median'],\
                        'umidrel_max':['min','max','mean','median'],'umidrel_min':['min','max','mean','median'],\
                        'umidrel':['min','max','mean','median'],'vento_gr':['min','max','mean','median'],\
                        'vento_rajmax':['min','max','mean','median'],'vento_velmax_ms':['min','max','mean','median'],\
                        'datahora':['first','last'],'regiao':lambda x:', '.join(x.unique()),\
                        'uf':lambda x:', '.join(x.unique()),'estacao':lambda x:', '.join(x.unique()),\
                        'codigo_wmo':lambda x:', '.join(x.unique()),'latitude':lambda x:', '.join(x.unique()),\
                        'longitude':lambda x:', '.join(x.unique()),'altitude':'first',\
                        'fundacao':'first'})
                    tmpDF['id']=tmpDF.index
                    tmpDF.columns=tmpDF.columns.map('_'.join)
                    tmpDF=tmpDF.rename(columns={'id_':'id','data_first':'data',\
                        'regiao_<lambda>':'regiao','uf_<lambda>':'uf','estacao_<lambda>':'estacao',\
                        'codigo_wmo_<lambda>':'codigo_wmo','latitude_<lambda>':'latitude',\
                        'longitude_<lambda>':'longitude','altitude_first':'altitude',\
                        'fundacao_first':'fundacao'})[['id','data','hora_first','hora_last',\
                        'datahora_first','datahora_last','regiao','fundacao',\
                        'uf','estacao','codigo_wmo','latitude','longitude','altitude',\
                        'precTotal_mm_min','precTotal_mm_max','precTotal_mm_mean',\
                        'precTotal_mm_median','pressao_min','pressao_max','pressao_mean',\
                        'pressao_median','pressao_max_min','pressao_max_max','pressao_max_mean',\
                        'pressao_max_median','pressao_min_min','pressao_min_max',\
                        'pressao_min_mean','pressao_min_median','radiacao_kjm2_min',\
                        'radiacao_kjm2_max','radiacao_kjm2_mean','radiacao_kjm2_median',\
                        'tempseco_c_min','tempseco_c_max','tempseco_c_mean',\
                        'tempseco_c_median','temporvalho_c_min','temporvalho_c_max',\
                        'temporvalho_c_mean','temporvalho_c_median','temps_maxc_min',\
                        'temps_maxc_max','temps_maxc_mean','temps_maxc_median',\
                        'temps_minc_min','temps_minc_max','temps_minc_mean',\
                        'temps_minc_median','temporv_maxc_min','temporv_maxc_max',\
                        'temporv_maxc_mean','temporv_maxc_median','temporv_minc_min',\
                        'temporv_minc_max','temporv_minc_mean','temporv_minc_median',\
                        'umidrel_max_min','umidrel_max_max','umidrel_max_mean',\
                        'umidrel_max_median','umidrel_min_min','umidrel_min_max',\
                        'umidrel_min_mean','umidrel_min_median','umidrel_min','umidrel_max',\
                        'umidrel_mean','umidrel_median','vento_gr_min','vento_gr_max',\
                        'vento_gr_mean','vento_gr_median','vento_rajmax_min',\
                        'vento_rajmax_max','vento_rajmax_mean','vento_rajmax_median',\
                        'vento_velmax_ms_min','vento_velmax_ms_max','vento_velmax_ms_mean',\
                        'vento_velmax_ms_median']]
                tmpDF['id']=tmpDF['id'].astype(int)
                self.dataframe=concat([self.dataframe.set_index('id'),tmpDF.set_index('id')]).\
                    reset_index().drop_duplicates(subset='id', keep='last')
            tmpCount+=1
        self.dataframe['id']=self.dataframe['id'].astype(int)
        print('Finish with dataframe:[',self.dataframe.shape,'], scraps:[',tmpCount,'].')
    def writeXls(self):
        from pandas import DataFrame
        assert isinstance(self.dataframe, DataFrame), 'writeXls() parameter dataframe={} not a Pandas DataFrame'\
            .format(self.dataframe)
        self.dataframe.to_excel(self.inmetfile)
        print('File:[',self.inmetfile,'] exported!')
    def readXLS(self,optional=False):
        import os.path
        from pandas import ExcelFile, DataFrame
        tmpExist=False
        tmpExist=os.path.isfile(self.inmetfile)
        if (tmpExist==False):
            if (optional==True):
                if (self.retdetail==0):
                    return DataFrame(index=[],columns=['id','data',\
                        'hora','datahora','regiao',\
                        'uf','estacao','codigo_wmo','latitude','longitude',\
                        'altitude','fundacao','precTotal_mm','pressao',\
                        'pressao_max','pressao_min','radiacao_kjm2',\
                        'tempseco_c','temporvalho_c','temps_maxc',\
                        'temps_minc','temporv_maxc','temporv_minc',\
                        'umidrel_max','umidrel_min','umidrel','vento_gr',\
                        'vento_rajmax','vento_velmax_ms']).astype({'id':int})
                else:
                    return DataFrame(index=[],columns=['id','data','hora_first',\
                        'hora_last','datahora_first','datahora_last',\
                        'regiao','fundacao','uf','estacao','codigo_wmo',\
                        'latitude','longitude','altitude','precTotal_mm_min',\
                        'precTotal_mm_max','precTotal_mm_mean','precTotal_mm_median',\
                        'pressao_min','pressao_max','pressao_mean','pressao_median',\
                        'pressao_max_min','pressao_max_max','pressao_max_mean',\
                        'pressao_max_median','pressao_min_min','pressao_min_max',\
                        'pressao_min_mean','pressao_min_median','radiacao_kjm2_min',\
                        'radiacao_kjm2_max','radiacao_kjm2_mean','radiacao_kjm2_median',\
                        'tempseco_c_min','tempseco_c_max','tempseco_c_mean',\
                        'tempseco_c_median','temporvalho_c_min','temporvalho_c_max',\
                        'temporvalho_c_mean','temporvalho_c_median','temps_maxc_min',\
                        'temps_maxc_max','temps_maxc_mean','temps_maxc_median',\
                        'temps_minc_min','temps_minc_max','temps_minc_mean',\
                        'temps_minc_median','temporv_maxc_min','temporv_maxc_max',\
                        'temporv_maxc_mean','temporv_maxc_median','temporv_minc_min',\
                        'temporv_minc_max','temporv_minc_mean','temporv_minc_median',\
                        'umidrel_max_min','umidrel_max_max','umidrel_max_mean',\
                        'umidrel_max_median','umidrel_min_min','umidrel_min_max',\
                        'umidrel_min_mean','umidrel_min_median','umidrel_min','umidrel_max',\
                        'umidrel_mean','umidrel_median','vento_gr_min','vento_gr_max',\
                        'vento_gr_mean','vento_gr_median','vento_rajmax_min',\
                        'vento_rajmax_max','vento_rajmax_mean','vento_rajmax_median',\
                        'vento_velmax_ms_min','vento_velmax_ms_max','vento_velmax_ms_mean',\
                        'vento_velmax_ms_median']).astype({'id':int})
            else:
                raise Exception('Historical file:['+self.inmetfile+'] not available!')
        self.dataXLS=ExcelFile(self.inmetfile)
        if not 'Sheet1' in self.dataXLS.sheet_names:
            raise Exception('ReadInmet historical data not available! [Sheet1]')
        retConvert=({'id':int,'hora_first':str,'hora_last':str} if (self.retdetail>0) \
            else {'id':int,'hora':str})
        histData=self.dataXLS.parse('Sheet1',index_col=None,na_values=['NA'],usecols=('B:CE' \
            if (self.retdetail>0) else 'B:AD'),converters=retConvert)
        return histData
#import zipfile, io
#from cashflow.scrap.scrap import ScrapingO
#import pandas as pd
#inmetClima=ScrapingO(url='https://portal.inmet.gov.br/uploads/dadoshistoricos/2010.zip',timeout=15,stream=True,headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'})
#zipfile.ZipFile(io.BytesIO(inmetClima.content), mode="r").printdir()
#z=zipfile.ZipFile(io.BytesIO(inmetClima.content), mode="r").read('2010/INMET_SE_SP_A701_SAO PAULO - MIRANTE_01-01-2010_A_31-12-2010.CSV')
##pd.read_csv(io.StringIO(z.decode('latin').splitlines()))
##pd.read_csv(z.decode('latin'),skiprows=[8],sep = ';',lineterminator='\n')#,usecols=range(19)
##pd.read_csv('|'.join(z.decode('latin').splitlines()),sep=';',lineterminator='|')
##df = pd.DataFrame([row.split(';') for row in str(z).split('\\n')])
#df = pd.DataFrame([row.split(';') for row in z.decode('latin').splitlines()])
#[corpo_fisico:ok]->[energias_himunidade:ok]->[gosta_emocoes:ok]->[sabe_conhecimento:ok]->[vontade:ok] 4:ferramentas(ciclica)+vontade/entusiasmo(estavel,quem pucha) - tudo ok
#dominar hemocoes e mente como domina suas pernas e bracos para ir muito mais longe, se identificar com a consiencia para domina-la, 
#as pessoas se identificam e respeitam quem consideram superior, nao os iguais/amigos , como animais e criancas
#apatia, falta de desejo por algo - quem dira por vontade, piramides sao um exemplo de pegada gigante - com muita vontade mesmo ao construir, rastro das obras, suas vontates expressam seus rastos (como aviao monomotor x caca)
#onde ha vontade real ha um caminho, pessoas superam suas dificuldades sem duvidas de que vai dar certo quando ha vontade e desafio ao portador
#onde estao as capacidades - no agora/fisico mais pessoal, no futuro/historia mais geral e distribuido, como a escalibur=mais resistente vai mais longe
#so saimos de um problema pela vontade, use a vontade para resolver quando identificar a causa e aplicar a solucao correta, so achar a causa nao adianta
#as armas magicas dos contos sao seus poderes internos, como vc resolve quando nao acha que nao vai muda de ideia e segue
#determinacao faz historia, vontade pode fazer fortuna
#jovens seguem pessoas que demostram vontade, as vezes um traficante e esta pessoa para ele, seja alguem com muita vontade para ele (referencia que sobrepoe outras)

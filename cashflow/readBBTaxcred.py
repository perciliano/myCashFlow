#' title......: Taxas de Juros
#' description: Leitura de excel das taxas - informacoes gerais
#' file.......: readBBTaxcred.py
#' version....: 0.1.0
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2023-03-31
#' update.....: 2023-04-03
#' library....: requests, scrap, xlrd, os, pandas
#' sample.....: from readBBTaxcred import ReadCredTax ; taxaCreditoBR=ReadCredTax(hist_ignore=True) ; taxaCreditoBR.writeXls()
#'https://www.bcb.gov.br/estatisticas/txjuros taxCredBR=ReadCredTax(matchbanks=['ITAÚ'],txcredfile=tmpOSDir[1]+'taxCredBR_.xlsx')
class ReadCredTax():
    WORKFILE="taxCredBR_.xlsx"
    def __init__(self,hist_ignore=False,txcredfile=None,matchbanks=None,timeout=5):
        self.hist_ignore=True if (hist_ignore!=False) else False
        self.matchbanks=matchbanks if (type(matchbanks)==list) else None \
            if (matchbanks is None) else [matchbanks]
        self.bankread=[]
        self.txcredfile=self.WORKFILE.replace("_",'' \
            if (matchbanks is None) else str(matchbanks[0])) \
            if (txcredfile is None) else txcredfile
        self.timeout=timeout
        if (self.hist_ignore==False):
            self.dataframe=self.readXLS(optional=False)
            print('Finish with dataframe:[',self.dataframe.shape,'], from local.')
        else:
            self.__startScrap__()
    def __startScrap__(self):
        from cashflow.scrap.scrap import ScrapingO
        from pandas import DataFrame, ExcelFile, to_datetime, concat, merge
        from pandas.tseries.offsets import MonthEnd
        tmpCount=0
        self.dataframe=DataFrame(index=[],columns=['id','idmodalidade','modalidade',\
            'idencargo','encargo','idtppessoa','tipopessoa','posicao','instituicao',\
            'txMedia_paa','txMedia_pam','tipotaxa','periodoini','periodofim']).\
            astype({'idmodalidade':int,'idtppessoa':int,'idencargo':int,\
            'posicao':int,'txMedia_paa':float,'txMedia_pam':float})
        tmpURL='https://www.bcb.gov.br/conteudo/txcred/Documents/taxascredito.xls'
        try:
            tmpSocket=ScrapingO(url=tmpURL,timeout=self.timeout,stream=True,headers={'User-Agent':\
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '+\
                '(KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'})
        except Exception as reqErr:
            raise Exception('Fail on scrapping, verify current dataframe status, cause: ['+str(reqErr)+']')
        if tmpSocket.ok==False:
            raise Exception('Fail on scrapping, this source was not ok: ['+str(tmpSocket.ok)+']! Verify timeout.')
        tmpXLS=ExcelFile(tmpSocket.content)
        for sheet in tmpXLS.sheet_names:
            [tmpTPessoa,tmpTPtax]=sheet.replace('Taxas','').split('-')
            tmpCols={'diarias':'A:G','mensais':'A:E'}.get(tmpTPtax.lower().strip())
            tmpData=tmpXLS.parse(sheet,index_col=None,na_values=['NA',' ',''],usecols=tmpCols,header=None)
            tmpPerIni=tmpData[:1][4].values[0] if (tmpData[:1][3].values[0]=='Período:') else None
            if (str(type(tmpPerIni)).find('datetime.datetime')<0 and len(tmpPerIni.split(' - '))>1):
                tmpPerIni=tmpPerIni.split(' - ')[1]+\
                    {'janeiro':'01','fevereiro':'02','março':'03','abril':'04','maio':'05','junho':'06',\
                    'julho':'07','agosto':'08','setembro':'09','outubro':'10','novembro':'11','dezembro':'12'\
                    }.get(tmpPerIni.split(' - ')[0].lower().strip())
                tmpPerFim=to_datetime(tmpPerIni,format="%Y%m")+MonthEnd(0)
                tmpPerIni=to_datetime(tmpPerIni+'01',format="%Y%m%d")
            else:
                tmpPerFim=tmpData[:1][6].values[0] if (tmpData[:1][3].values[0]=='Período:') else None
            tmpDF=tmpData.loc[tmpData[2].isnull()==False][[0,1,2,3,4]].iloc[1:] #tmpData.iloc[6:][[0,1,2,3,4]] .replace(' ',None)
            tmpDF[0].fillna(method='ffill',inplace=True)
            tmpDF.columns=['modalidade','posicao','instituicao','txMedia_paa','txMedia_pam']
            tmpDF[['modalidade','encargo']]=tmpDF['modalidade'].str.rsplit(' - ',1,expand=True) #split do ultimo delimitador apenas
            tmpDF['tipopessoa']=tmpTPessoa
            tmpDF['idtppessoa']={'pf':1,'pj':2}.get(tmpTPessoa.lower().strip())
            tmpDF['tipotaxa']=tmpTPtax
            tmpDF['periodoini']=tmpPerIni
            tmpDF['periodofim']=tmpPerFim #tmpDF.iloc[1,:]
            tmpDF['idencargo']=None
            for encargo in tmpDF.encargo.unique():
                tmpDF.loc[tmpDF['encargo']==encargo,'idencargo']={'pré-fixado':101,\
                    'pós-fixado referenciado em ipca':203,\
                    'pós-fixado referenciado em tr':201,\
                    'pós-fixado referenciado em moeda estrangeira':205,\
                    'pós-fixado referenciado em juros flutuantes':204}.get(encargo.lower().strip())
            tmpDF['idmodalidade']=None
            for modalidade in tmpDF.modalidade.unique():
                tmpDF.loc[tmpDF['modalidade']==modalidade,'idmodalidade']={'vendor':404,\
                    'adiantamento sobre contratos de câmbio (acc)':502,\
                    'antecipação de faturas de cartão de crédito':303,\
                    'capital de giro com prazo até 365 dias':210,\
                    'capital de giro com prazo superior a 365 dias':211,\
                    'cheque especial':216,'conta garantida':217,\
                    'desconto de cheques':302,'desconto de duplicatas':301,\
                    'financiamento imobiliário com taxas de mercado':903,
                    'financiamento imobiliário com taxas reguladas':905,\
                    'aquisição de outros bens':402,'aquisição de veículos':401,\
                    'arrendamento mercantil de veículos':1205,\
                    'cartão de crédito - parcelado':215,\
                    'cartão de crédito - rotativo total':204,\
                    'crédito pessoal consignado inss':218,\
                    'crédito pessoal consignado privado':219,\
                    'crédito pessoal consignado público':220,\
                    'crédito pessoal não-consignado':221}.get(modalidade.lower().strip())
            tmpDF['id']=(tmpDF['idtppessoa'].map(str)+tmpDF['idmodalidade'].map(str)+\
                tmpDF['idencargo'].map(str)).map(int)
            tmpDF=tmpDF[['id','idmodalidade','modalidade','idencargo','encargo','idtppessoa',\
                'tipopessoa','posicao','instituicao','txMedia_paa','txMedia_pam',\
                'tipotaxa','periodoini','periodofim']].astype({'idmodalidade':int,\
                'idtppessoa':int,'idencargo':int,'posicao':int,\
                'txMedia_paa':float,'txMedia_pam':float})
            #dataframe original origem
            self.dataframe=concat([self.dataframe,tmpDF],ignore_index=True)
        tmpInst=self.dataframe.instituicao.unique()
        if (self.matchbanks!=None and len(tmpInst)>0): #valida existencia das instituicoes
            for lbank in self.matchbanks:
                if (len(list(filter(lambda x:lbank.upper() in x,tmpInst)))>0):
                    self.bankread=list(filter(lambda x:lbank.upper() in x,tmpInst)) if \
                        (len(self.bankread)==0) else \
                        self.bankread.append(list(filter(lambda x:lbank.upper() in x,tmpInst)))
        if (len(self.bankread)>0):
            tmpDF2=self.dataframe.groupby(['id']).agg({\
                'idmodalidade':'first','idencargo':'first',\
                'idtppessoa':'first','periodoini':'first','periodofim':'first',\
                'txMedia_paa':['min','max','mean','median'],'txMedia_pam':['min','max','mean','median'],\
                'modalidade':lambda x:','.join(x.unique()),'encargo':lambda x:', '.join(x.unique()),\
                'tipotaxa':lambda x:','.join(x.unique()),'tipopessoa':lambda x:', '.join(x.unique())})
            tmpDF2['id']=tmpDF2.index
            tmpDF2.columns=tmpDF2.columns.map('_'.join)
            #dataframe das instituicoes solicitadas:matchbanks
            self.dataframe=merge(self.dataframe.loc[self.dataframe.instituicao.isin(self.bankread)],\
                tmpDF2[['id_',\
                'txMedia_paa_min','txMedia_paa_max','txMedia_paa_mean','txMedia_paa_median',\
                'txMedia_pam_min','txMedia_pam_max','txMedia_pam_mean','txMedia_pam_median']],\
                left_on='id', right_on='id_')
        tmpCount+=1
        print('Finish with dataframe:[',self.dataframe.shape,'], scraps:[',tmpCount,'].')
    def writeXls(self):
        from pandas import DataFrame
        assert isinstance(self.dataframe, DataFrame), 'writeXls: parameter dataframe={} not a Pandas DataFrame'.format(self.dataframe)
        self.dataframe.to_excel(self.txcredfile)
        print('File:[',self.txcredfile,'] exported!')
    def readXLS(self,optional=False):
        import os.path
        from pandas import ExcelFile, DataFrame
        tmpExist=False
        tmpExist=os.path.isfile(self.txcredfile)
        if (tmpExist==False):
            if (optional==True):
                return DataFrame(index=[],columns=['id','idmodalidade','modalidade',\
                    'idencargo','encargo','idtppessoa','tipopessoa','posicao','instituicao',\
                    'txMedia_paa','txMedia_pam','tipotaxa','periodoini','periodofim']).\
                    astype({'idmodalidade':int,'idtppessoa':int,'idencargo':int,\
                    'posicao':int,'txMedia_paa':float,'txMedia_pam':float})
            else:
                raise Exception('Historical file:['+self.txcredfile+'] not available!')
        self.dataXLS=ExcelFile(self.txcredfile)
        tmpCols='B:O' if (self.matchbanks is None) else 'B:X'
        if not 'Sheet1' in self.dataXLS.sheet_names:
            raise Exception('ReadPresBR historical data not available! [Sheet1]')
        histData=self.dataXLS.parse('Sheet1',index_col=None,na_values=['NA'],usecols=tmpCols)
        return histData


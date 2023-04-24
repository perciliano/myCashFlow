#' title......: HTTP, HTTPS, Scraping
#' description: Leitura de websites para scraping por objetos
#' file.......: readBBCota.py
#' version....: 0.1.1
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2022-07-30
#' update.....: 2022-08-01
#' library....: requests, scrap, BeautifulSoup, datetime, json, time, os, pandas
#' sample.....: from readBBCota import ReadBBCota ; moedas=ReadBBCota(start_date='2010-01-01',end_date='2010-04-01',hist_ignore=True) ; moedas.writeXls()
#'https://www.bcb.gov.br/conversao
#'https://www3.bcb.gov.br/bc_moeda/rest/cotacao/fechamento/ultima/1/220/2011-01-01
class ReadBBCota():
    WORKFILE="cotacoesBB.xlsx"
    def __init__(self,start_date=None,end_date=None,stepby=1,currency_dict=None,hist_ignore=False,cotationfile=None):
        from datetime import date, timedelta
        try:
            self.start_date=date.today() if (start_date is None) \
                else date.fromisoformat(start_date)
            self.end_date=date.today()+timedelta(days=1) if (end_date is None) \
                else date.fromisoformat(end_date)
            self.stepby=stepby
            self.hist_ignore=True if (hist_ignore!=False) else False
            self.cotationfile=self.WORKFILE if (cotationfile is None) else cotationfile
        except Exception as dtError:
            raise Exception('Data invalida! yyyy-mm-dd[start_date:'+start_date+', end_date:'+\
                end_date+', stepby='+str(stepby)+']'+str(dtError))
        if (int((self.end_date.year-self.start_date.year)*12+self.end_date.month-self.start_date.month)<0):
            raise Exception('Data final invalida! informar acima da inicial, yyyy-mm-dd[start_date:'+start_date+', end_date:'+\
                end_date+', stepby='+str(stepby)+']')
        try:
            self.currency_dict=dict([('Peso chileno/CLP',715),('Boliviano/BOB', 30),\
                ('Peso uruguaio/UYU',745),('Euro/EUR',978),\
                ('Emirados Arabes/AED',145),('Dólar/USD',220),('Iene/JPY',470),\
                ('Dólar australiano/AUD',150),('Dólar canadense/CAD',165),\
                ('Novo sol/PEN',660),('Ouro/XAU',998)]) if (currency_dict is None) \
                else dict(currency_dict)
        except Exception as dictError:
            raise Exception('Dicionario de moedas invalido! exemplo:([(\'Dólar/USD\',978)],...),[currency_dict:'+\
                currency_dict+']'+str(dictError))
        self.__startLoop__()
    def __daterange__(self):
        from datetime import timedelta
        for n in range(0,int((self.end_date - self.start_date).days),self.stepby):
            yield self.start_date + timedelta(n)
    def __startLoop__(self):
        from cashflow.scrap.scrap import ScrapingO
        import time, json
        from pandas import DataFrame, json_normalize, concat
        self.listCota={}
        tmpCount=0
        if (self.hist_ignore==False):
            self.dataframe=self.readXLS(optional=True)
        else:
            self.dataframe=DataFrame(index=[], columns=['id','moeda','codigoMoeda','cotacaoBoletim',\
                'cotacaoContabilidade','dataHoraCotacao','paridadeCompra','paridadeVenda','taxaCompra',\
                'taxaVenda','tipoCotacao','data','tipoMoeda','horaBoletim','numero','nomeMoeda',\
                'siglaMoeda']).astype(\
                {'id':int,'moeda':str,'codigoMoeda':int,'cotacaoBoletim':bool,'cotacaoContabilidade':bool,\
                'paridadeCompra':float,'paridadeVenda':float,'taxaCompra':float,'taxaVenda':float,\
                'tipoCotacao':str,'tipoMoeda':str,'numero':int,'nomeMoeda':str,'nomeMoeda':str})
        for single_date in self.__daterange__():
            print(single_date.strftime("%Y-%m-%d"))
            for key in self.currency_dict:
                if (len(self.dataframe.loc[self.dataframe['id']\
                    ==int(str(self.currency_dict[key])+single_date.strftime("%Y%m%d")),'id'])>0):
                    continue
                #print(key, '->', self.currency_dict[key],int(str(self.currency_dict[key])+single_date.strftime("%Y%m%d")))
                url='https://www3.bcb.gov.br/bc_moeda/rest/cotacao/fechamento/ultima/1/'+\
                    str(self.currency_dict[key])+'/'+single_date.strftime("%Y-%m-%d")
                try:
                    bc=ScrapingO(url=url)
                except Exception as reqErr:
                    raise Exception('Fail on scrapping, verify current dataframe/listcota status, cause: ['+str(reqErr)+']')
                if bc.ok==True:
                    bc.xmlRead()
                    self.listCota[int(str(self.currency_dict[key])+\
                        single_date.strftime("%Y%m%d"))]={'leitura':json.loads(json.dumps(bc.dict)),'moeda':key} #dict
                    tmpDF2=json_normalize({'id':int(str(self.currency_dict[key])+\
                        single_date.strftime("%Y%m%d")),'moeda':key,'retorno':json.loads(json.dumps(bc.dict))}).\
                        rename(index=str,columns={'retorno.cotacao.codigoMoeda':'codigoMoeda',\
                            'retorno.cotacao.cotacaoBoletim':'cotacaoBoletim',\
                            'retorno.cotacao.cotacaoContabilidade':'cotacaoContabilidade',\
                            'retorno.cotacao.cotacoes.dataHoraCotacao':'dataHoraCotacao',\
                            'retorno.cotacao.cotacoes.paridadeCompra':'paridadeCompra',\
                            'retorno.cotacao.cotacoes.paridadeVenda':'paridadeVenda',\
                            'retorno.cotacao.cotacoes.taxaCompra':'taxaCompra',\
                            'retorno.cotacao.cotacoes.taxaVenda':'taxaVenda',\
                            'retorno.cotacao.cotacoes.tipoCotacao':'tipoCotacao',\
                            'retorno.cotacao.data':'data',\
                            'retorno.cotacao.tipoMoeda':'tipoMoeda',\
                            'retorno.cotacao.cotacoes.horaBoletim':'horaBoletim',\
                            'retorno.cotacao.cotacoes.numero':'numero'})
                    self.dataframe=concat([self.dataframe,tmpDF2],ignore_index=True)
                    tmpCount+=1
                time.sleep(0.5)
            time.sleep(1)
        self.dataframe[['nomeMoeda','siglaMoeda']]=self.dataframe.moeda.str.split('/',expand=True)
        #df2=DataFrame(index=[], columns=[]) ; df2=concat([df,df2],ignore_index=True)
        print('Finish with dataframe:[',self.dataframe.shape,'], scraps:[',tmpCount,'].')
    def writeXls(self):
        from pandas import DataFrame
        assert isinstance(self.dataframe, DataFrame), 'writeXls() parameter dataframe={} not a Pandas DataFrame'.format(self.dataframe)
        self.dataframe.to_excel(self.cotationfile)
        print('File:[',self.cotationfile,'] exported!')
    def readXLS(self,optional=False):
        import os.path
        from pandas import ExcelFile, DataFrame
        tmpExist=False #os.path.exists('cotacoesBB.xlsx')
        tmpExist=os.path.isfile(self.cotationfile)
        if (tmpExist==False):
            if (optional==True):
                return DataFrame(index=[], columns=['id','moeda','codigoMoeda','cotacaoBoletim',\
                'cotacaoContabilidade','dataHoraCotacao','paridadeCompra','paridadeVenda','taxaCompra',\
                'taxaVenda','tipoCotacao','data','tipoMoeda','horaBoletim','numero','nomeMoeda',\
                'siglaMoeda']).astype(\
                {'id':int,'moeda':str,'codigoMoeda':int,'cotacaoBoletim':bool,'cotacaoContabilidade':bool,\
                'paridadeCompra':float,'paridadeVenda':float,'taxaCompra':float,'taxaVenda':float,\
                'tipoCotacao':str,'tipoMoeda':str,'numero':int,'nomeMoeda':str,'nomeMoeda':str})
            else:
                raise Exception('Historical file:['+self.cotationfile+'] not available!')
        self.dataXLS=ExcelFile(self.cotationfile)
        if not 'Sheet1' in self.dataXLS.sheet_names:
            raise Exception('ReadBBCota historical data not available! [Sheet1]')
        histData=self.dataXLS.parse('Sheet1',index_col=None,na_values=['NA'],usecols="B:P",converters=\
            {'id':int,'moeda':str,'codigoMoeda':int,'cotacaoBoletim':bool,'cotacaoContabilidade':bool,\
             'paridadeCompra':float,'paridadeVenda':float,'taxaCompra':float,'taxaVenda':float,\
             'tipoCotacao':str,'tipoMoeda':str,'numero':int,'nomeMoeda':str,'nomeMoeda':str})
        #histData.columns=['id','moeda','codigoMoeda','cotacaoBoletim',\
        #    'cotacaoContabilidade','dataHoraCotacao','paridadeCompra','paridadeVenda',\
        #    'taxaCompra','taxaVenda','tipoCotacao','data','tipoMoeda','horaBoletim','numero']
        histData=histData[histData.id.notnull()]
        return histData


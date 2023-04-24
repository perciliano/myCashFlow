#' title......: HTTP, HTTPS, Scraping
#' description: Leitura da listagem de presidentes do Brasil na Wikipedia
#' file.......: readPresBRwiki.py
#' version....: 0.1.0
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2023-02-25
#' update.....: 2023-02-25
#' library....: requests, scrap, BeautifulSoup, re, os, pandas
#' sample.....: from readPresBRwiki import ReadPresBR ; presidentes=ReadPresBR(hist_ignore=True) ; presidentes.writeXls()
#'https://pt.wikipedia.org/wiki/Lista_de_presidentes_do_Brasil
#'http://www.biblioteca.presidencia.gov.br/presidencia/ex-presidentes
class ReadPresBR():
    WORKFILE="presidentesBRw.xlsx"
    def __init__(self,hist_ignore=False,presidentsfile=None):
        self.hist_ignore=True if (hist_ignore!=False) else False
        self.presidentsfile=self.WORKFILE if (presidentsfile is None) else presidentsfile
        if (self.hist_ignore==False):
            self.dataframe=self.readXLS(optional=False)
            print('Finish with dataframe:[',self.dataframe.shape,'], local loads.')
        else:
            self.__startScrap__()
    def __startScrap__(self):
        from cashflow.scrap.scrap import ScrapingO
        from pandas import DataFrame, read_html, to_datetime
        import re
        tmpCount=0
        self.dataframe=DataFrame(index=[], columns=['id','nr','presidente','foto','mandato',\
            'partido','vice','notas','eleicao','inicio','fim','periodo',\
            'qt_mandato','qt_partido','qt_vice','qt_notas','qt_eleicao']).\
            astype({'id':int,'nr':int,'presidente':str,'foto':str,'mandato':str,'partido':str,\
            'vice':str,'notas':str,'eleicao':str,'periodo':str,\
            'qt_mandato':int,'qt_partido':int,'qt_vice':int,'qt_notas':int,'qt_eleicao':int})
        url='https://pt.wikipedia.org/wiki/Lista_de_presidentes_do_Brasil'
        try:
            presWiki=ScrapingO(url=url)
        except Exception as reqErr:
            raise Exception('Fail on scrapping, verify current dataframe status, cause: ['+str(reqErr)+']')
        if presWiki.ok==False:
            raise Exception('Fail on scrapping, this source was not ok: ['+str(presWiki.ok)+']')
        presWiki.htmlRead()
        dfTablePres=read_html(str(presWiki.soup.find_all('table')[0]))[0] #MultiIndex DF in a multiList source
        dfPresident=self.dataframe
        dfPresident.nr=dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)[0]]
        dfPresident.presidente=dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)[1]]
        dfPresident.foto=dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)[2]]
        dfPresident.mandato=dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)[3]]
        dfPresident.partido=dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)[4]]
        dfPresident.vice=dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)[5]]
        dfPresident.notas=dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)[6]]
        dfPresident.eleicao=dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)[7]]
        dfPresident.loc[dfPresident.nr=='—','nr']='0'
        dfPresident.mandato=dfPresident.mandato.str.replace('–','-').str.encode('ascii', 'ignore').str.decode('ascii')
        #dfPresident.mandato=dfPresident.mandato.str.split().str.join(' ') #remove unicode u'\xa0'
        dfPresident.inicio=dfPresident.mandato.str.split('-',expand=True)[0]
        dfPresident.fim=dfPresident.mandato.str.split('-',expand=True)[1]
        if (max(dfPresident.mandato.str.count('-'))>1):
            dfPresident.loc[dfPresident.mandato.str.count('-')>1,'fim']=\
                dfPresident.loc[dfPresident.mandato.str.count('-')>1,'mandato'].str.split('-',expand=True)[2]
        dfPresident.loc[dfPresident.nr.apply(lambda x: str(x).isnumeric()==False),'periodo']=\
            dfPresident.loc[dfPresident.nr.apply(lambda x: str(x).isnumeric()==False),'nr']
        dfPresident['periodo'].fillna(method = 'ffill', inplace = True)
        dfPresident.loc[dfPresident.periodo.isnull(),'periodo']=str(dfTablePres.loc[:,dfTablePres.columns.get_level_values(0)\
            [0]].iloc[:1]).strip().replace('\n0                                                  1', '').encode('ascii', 'ignore').decode('ascii')
        dfPresident['id']=dfPresident.index
        dfPresident.qt_mandato=0
        dfPresident.first=0
        dfPresident.qt_partido=0
        dfPresident.qt_vice=0
        dfPresident.qt_notas=0
        dfPresident.qt_eleicao=0
        self.dataframe=dfPresident.loc[dfPresident.nr.apply(lambda x: str(x).isnumeric())]\
            .groupby(['nr','presidente']).agg({'id':'last','nr':'first','presidente':'first','foto':'first',\
            'mandato': lambda x: ', '.join(x.unique()),'partido': lambda x: ', '.join(x.unique()),\
            'vice': lambda x: ', '.join(x.unique()),'notas': lambda x: ', '.join(x.unique()),\
            'eleicao': lambda x: ', '.join(x.unique()),'inicio':'first','fim':'last','periodo':'first',\
            'qt_mandato':'first','qt_partido':'first','qt_vice':'first','qt_notas':'first',\
            'qt_eleicao':'first'}).sort_values(['id'],ascending=True)
        self.dataframe.reset_index(drop=True, inplace=True)
        self.dataframe.loc[self.dataframe.nr!='0','qt_mandato']=self.dataframe.loc[self.dataframe.nr!='0','mandato'].str.count(',')+1
        self.dataframe.loc[self.dataframe.partido.replace({'nenhum':''},regex=True)>'','qt_partido']=\
            self.dataframe.loc[self.dataframe.partido.replace({'nenhum':''},regex=True)>'','partido'].str.count(',')+1
        self.dataframe.loc[self.dataframe.vice.replace({'nenhum':''},regex=True)>'','qt_vice']=\
            self.dataframe.loc[self.dataframe.vice.replace({'nenhum':''},regex=True)>'','vice'].str.count(',')+1
        self.dataframe.loc[self.dataframe.notas>'','qt_notas']=self.dataframe.loc[self.dataframe.notas>'','notas'].str.count(r'\[')
        self.dataframe.loc[self.dataframe.eleicao.replace({'-':''},regex=True)>'','qt_eleicao']=\
            self.dataframe.loc[self.dataframe.eleicao.replace({'-':''},regex=True)>'','eleicao'].str.count(',')+1
        tmpExpDate=r'\d{1,4}(?P<delim>[.\-/]|\s+de\s+)(\d{1,2}|(janeiro|fevereiro|maro|marco|abril|maio|'+\
                    'junho|julho|agosto|setembro|outubro|novembro|dezembro))(?P=delim)\d{1,4}'
        tmpMes={' de janeiro de ':'-1-',' de fevereiro de ':'-2-',' de maro de ':'-3-',' de marco de ':'-3-',' de abril de ':'-4-',\
                ' de maio de ':'-5-',' de junho de ':'-6-',' de julho de ':'-7-',' de agosto de ':'-8-',' de setembro de ':'-9-',\
                ' de outubro de ':'-10-',' de novembro de ':'-11-',' de dezembro de ':'-12-'}
        for i in range(0, len(self.dataframe)):
            #self.dataframe.loc[self.dataframe.fim.str.contains(tmpExpDate)==False,'fim']=None
            if (self.dataframe.iloc[i].inicio is not None):
                self.dataframe.loc[i,'inicio']=None if (re.match(tmpExpDate,self.dataframe.iloc[i].inicio,flags=re.IGNORECASE) is None)\
                    else re.match(tmpExpDate,self.dataframe.iloc[i].inicio,flags=re.IGNORECASE).group(0)
            if (self.dataframe.iloc[i].fim is not None):
                self.dataframe.loc[i,'fim']=None if (re.match(tmpExpDate,self.dataframe.iloc[i].fim,flags=re.IGNORECASE) is None)\
                    else re.match(tmpExpDate,self.dataframe.iloc[i].fim,flags=re.IGNORECASE).group(0)
            if (self.dataframe.iloc[i].fim is not None):
                #self.dataframe.loc[i,'fim']=self.dataframe.iloc[i].fim[:self.dataframe.iloc[i].fim.find('(')]
                self.dataframe.loc[i,'fim']=self.dataframe.loc[i,'fim'].replace(self.dataframe.loc[i,'fim']\
                    [self.dataframe.loc[i,'fim'].find(" de"):self.dataframe.loc[i,'fim'].rfind("de ")+3],\
                    tmpMes.get(self.dataframe.loc[i,'fim'][self.dataframe.loc[i,'fim'].\
                    find(" de"):self.dataframe.loc[i,'fim'].rfind("de ")+3]))
            if (self.dataframe.iloc[i].inicio is not None):
                self.dataframe.loc[i,'inicio']=self.dataframe.loc[i,'inicio'].replace(self.dataframe.loc[i,'inicio']\
                    [self.dataframe.loc[i,'inicio'].find(" de"):self.dataframe.loc[i,'inicio'].rfind("de ")+3],\
                    tmpMes.get(self.dataframe.loc[i,'inicio'][self.dataframe.loc[i,'inicio'].\
                    find(" de"):self.dataframe.loc[i,'inicio'].rfind("de ")+3]))
        self.dataframe.inicio=to_datetime(self.dataframe.inicio,format='%d-%m-%Y')
        self.dataframe.fim=to_datetime(self.dataframe.fim,format='%d-%m-%Y')
        self.dataframe.nr=self.dataframe.nr.astype(str).astype(int)
        tmpCount+=1
        print('Finish with dataframe:[',self.dataframe.shape,'], scraps:[',tmpCount,'].')
    def writeXls(self):
        from pandas import DataFrame
        assert isinstance(self.dataframe, DataFrame), 'writeXls() parameter dataframe={} not a Pandas DataFrame'.format(self.dataframe)
        self.dataframe.to_excel(self.presidentsfile)
        print('File:[',self.presidentsfile,'] exported!')
    def readXLS(self,optional=False):
        import os.path
        from pandas import ExcelFile, DataFrame
        tmpExist=False
        tmpExist=os.path.isfile(self.presidentsfile)
        if (tmpExist==False):
            if (optional==True):
                return DataFrame(index=[], columns=['id','nr','presidente','foto','mandato',\
            'partido','vice','notas','eleicao','inicio','fim','periodo',\
            'qt_mandato','qt_partido','qt_vice','qt_notas','qt_eleicao']).\
            astype({'id':int,'nr':str,'presidente':str,'foto':str,'mandato':str,'partido':str,\
            'vice':str,'notas':str,'eleicao':str,'periodo':str,\
            'qt_mandato':int,'qt_partido':int,'qt_vice':int,'qt_notas':int,'qt_eleicao':int})
            else:
                raise Exception('Historical file:['+self.presidentsfile+'] not available!')
        self.dataXLS=ExcelFile(self.presidentsfile)
        if not 'Sheet1' in self.dataXLS.sheet_names:
            raise Exception('ReadPresBR historical data not available! [Sheet1]')
        histData=self.dataXLS.parse('Sheet1',index_col=None,na_values=['NA'],usecols="B:R",converters=\
            {'id':int,'nr':int,'presidente':str,'foto':str,'mandato':str,'partido':str,\
             'vice':str,'notas':str,'eleicao':str,'periodo':str,\
             'qt_mandato':int,'qt_partido':int,'qt_vice':int,'qt_notas':int,'qt_eleicao':int})
        histData=histData[histData.id.notnull()]
        return histData


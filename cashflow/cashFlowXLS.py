#' title......: Modelo para Analise de Fluxo de Caixa de Arquivo Excel, Fluxo_de_Caixa2020.xlsx
#' description: Monta dataframes de fluxos, planos, contas, parametros
#' file.......: cashFlowXLS.py
#' version....: 0.1.1
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2020-10-13
#' update.....: 2020-11-03
#' library....: os.path, getpass, datetime, pandas, re, KMeans
#' sample.....: from cashFlowXLS import *  ;  cf=CashFlow('Fluxo_de_Caixa2022.xlsx') ; cashPar=cf.loadParam() ; flowCash=cf.loadCash('coerce')
#'  bi=cf.mergeCashParam(flowCash,cashPar,moedas.dataframe) ; flowCash; cashPar.df[5] ; bi ; cf.writeXls(bi)
class CashFlow():
    def __init__(self,fileName='Fluxo_de_Caixa2023.xlsx'):
        self.user=self.__user() #import warnings
        self.host=self.__host()
        self.startDate=self.__now()
        self.fileName=fileName
        self.homeDir=self.__home()
        self.isOK=self.__fileExists(self.fileName)
        self.dataXLS=None
        self.biFile=None
        if self.isOK==False:
            self.fileName=self.homeDir+'/Documents/'+self.fileName
            self.isOK=self.__fileExists(self.fileName)
        if self.isOK==True:
           self.__loadXls()
        else:
            raise Exception('Cashflow file:['+self.fileName+'] not available!')
    def __fileExists(self,fileName):
        import os.path
        result=False #os.path.exists('Documents/Fluxo_de_Caixa2020.xlsx')
        result=os.path.isfile(fileName)
        return result
    def __user(self):
        import getpass
        return getpass.getuser()
    def __host(self):
        import os.path
        try:
            return os.uname()[1]
        except:
            return os.name
    def __home(self):
        import os.path
        return os.path.expanduser("~") if (os.path.split(self.fileName)[0]=='') \
            else os.path.split(self.fileName)[0]
    def __now(self):
        from datetime import datetime
        return datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    def __loadXls(self):
        if self.isOK==True:
            from pandas import ExcelFile
            self.dataXLS=ExcelFile(self.fileName)
        if self.dataXLS==[]:
            raise Exception('Cashflow parameters data not loaded!')
        else:
            print('File:[',self.fileName,'] loaded!')
    # limits=cf.__limits__(dfValues=bi[['dtCotation','med_temperatura_c','med_pressao']].copy(),breakby='y')
    def __limits__(self,dfValues=None,breakby='m'):
        from pandas import DataFrame, concat
        assert isinstance(dfValues, DataFrame), 'limits: dfValues={} not a Pandas DataFrame'.format(dfValues)
        assert (breakby in ['f','y','s','t','b','m','q','w','d','h']),'Use only! [f,y,s,t,b,m,q,w,d,h].'
        lstTypes=str(dfValues.dtypes).split('\n')
        dtCol=[] #dfValues=bi[['dtCotation','med_temperatura_c']].copy()
        vlCol=[]
        for d in lstTypes:
            if (d.find('dtype:')>-1):
                continue
            lDt=d[:d.rfind(' ')+1].strip() if (d[d.rfind(' ')+1:].strip().find('date')>-1) else None
            lVl=d[:d.rfind(' ')+1].strip() if (d[d.rfind(' ')+1:].strip().find('float')>-1) else \
                d[:d.rfind(' ')+1].strip() if (d[d.rfind(' ')+1:].strip().find('int')>-1) else None
            if (lDt is not None):
               dtCol.append(lDt)
            if (lVl is not None):
               vlCol.append(lVl)
        assert (len(dtCol)==1), 'limits: Only one date or datetime is column alowed! {}'.format(dtCol)
        assert (len(vlCol)>0), 'limits: Value colluns not in format int or float! {}'.format(vlCol)
        dfValues['_year']=dfValues[dtCol[0]].dt.year #dfValues=bi[['dtCotation','med_temperatura_c']].copy()
        dfValues['_month']=(dfValues['_year'].map(str)+dfValues[dtCol[0]].dt.month.map(str).str.zfill(2)).map(int)
        dfValues['_day']=dfValues[dtCol[0]].dt.strftime("%Y%m%d").map(int)
        dfValues['_quarter']=(dfValues['_year'].map(str)+dfValues[dtCol[0]].dt.quarter.map(str)).map(int)
        dfValues['_weekofyear']=(dfValues['_year'].map(str)+dfValues[dtCol[0]].dt.isocalendar().week.map(str)).map(int)
        dfValues['_hour']=(dfValues['_day'].map(str)+dfValues[dtCol[0]].dt.hour.map(str).str.zfill(2)).map(int)
        dfValues['_semester']=(dfValues['_year'].map(str)+dfValues['_quarter'].replace(2,1).replace(4,2).replace(3,2).map(str)).map(int)
        dfValues['_twomonths']=dfValues['_month']
        dfValues.loc[dfValues['_twomonths']%2==0,'_twomonths']=dfValues.loc[dfValues['_twomonths']%2==0,'_twomonths']-1
        dfValues['_biweekly']=dfValues['_weekofyear']
        dfValues.loc[dfValues['_biweekly']%2==0,'_biweekly']=dfValues.loc[dfValues['_biweekly']%2==0,'_biweekly']-1
        tmpRules={'f':[dtCol[0],['']],
                  'y':['_year',dfValues['_year'].unique()],
                  's':['_semester',dfValues['_semester'].unique()],
                  't':['_quarter',dfValues['_quarter'].unique()],
                  'b':['_twomonths',dfValues['_twomonths'].unique()],
                  'm':['_month',dfValues['_month'].unique()],
                  'q':['_biweekly',dfValues['_biweekly'].unique()],
                  'w':['_weekofyear',dfValues['_weekofyear'].unique()],
                  'd':['_day',dfValues['_day'].unique()],
                  'h':['_hour',dfValues['_hour'].unique()]}
        tmpCol=tmpRules.get(breakby)[0]
        tmpLoopBy=tmpRules.get(breakby)[1]
        retDF=DataFrame(columns=['id','column','q1','median','q3','iqr','ul','ll','outliers'])
        for c in tmpLoopBy:
            for lVCol in vlCol:
                if (breakby=='f'):
                    [tmpQ1,tmpQ2,tmpQ3]=dfValues[lVCol].quantile([0.25,0.5,0.75]).values
                else:
                    [tmpQ1,tmpQ2,tmpQ3]=dfValues.loc[dfValues[tmpCol]==c,lVCol].quantile([0.25,0.5,0.75]).values
                tmpIQR=tmpQ3-tmpQ1
                tmpULimit=tmpQ3+1.5*tmpIQR
                tmpLLimit=tmpQ1-1.5*tmpIQR
                if (breakby=='f'):
                    tmpOutliers=len(dfValues.loc[(dfValues[lVCol]<tmpLLimit)])+\
                        len(dfValues.loc[(dfValues[lVCol]>tmpULimit)])
                else:
                    tmpOutliers=len(dfValues.loc[(dfValues[tmpCol]==c)&(dfValues[lVCol]<tmpLLimit)])+\
                        len(dfValues.loc[(dfValues[tmpCol]==c)&(dfValues[lVCol]>tmpULimit)])
                tmpDF=DataFrame([[c,lVCol,tmpQ1,tmpQ2,tmpQ3,tmpIQR,tmpULimit,tmpLLimit,tmpOutliers]],\
                    columns=['id','column','q1','median','q3','iqr','ul','ll','outliers'])
                retDF=concat([retDF,tmpDF],ignore_index=True)
        return retDF
    def loadParam(self):
        if self.isOK==False:
            raise Exception('Cashflow file not available! ['+self.fileName+']')
        from pandas import DataFrame
        if not 'Plano_Contas' in self.dataXLS.sheet_names:
            raise Exception('Cashflow parameters data not available! [Plano_Contas]')
        if not 'Empresas' in self.dataXLS.sheet_names:
            raise Exception('Cashflow companies not available! [Empresas]')
        if not 'Parametros' in self.dataXLS.sheet_names:
            raise Exception('Cashflow parameters not available! [Parametros]')
        resData0=self.dataXLS.parse('Plano_Contas',index_col=None,na_values=['NA']\
                                   ,usecols="A:F",skiprows=[0])
        resData0.columns=['id','idConta','subConta','idCat','desConta','label']
        resData0=resData0[resData0.id.notnull()]
        resData0=resData0.astype({'id':'int64','idConta':'int64','subConta':'int64',\
                                  'idCat':'int64','desConta':'category','label':'category'})
        resData1=self.dataXLS.parse('Plano_Contas',index_col=None,na_values=['NA']\
                                   ,usecols="H:J",skiprows=[0])
        resData1.columns=['idConta','desGrupo','idTipo']
        resData1=resData1[resData1.desGrupo.notnull()]
        resData1=resData1.astype({'idConta':'int64','desGrupo':'category','idTipo':'category'})
        resData2=self.dataXLS.parse('Plano_Contas',index_col=None,na_values=['NA']\
                                   ,usecols="L:M",skiprows=[0])
        resData2.columns=['idCat','desCategoria']
        resData2=resData2[resData2.idCat.notnull()]
        resData2=resData2.astype({'idCat':'int64','desCategoria':'category'})
        resData3=self.dataXLS.parse('Plano_Contas',index_col=None,na_values=['NA']\
                                   ,usecols="S:T",skiprows=[0])
        resData3.columns=['idTipo','desMovimentacao']
        resData3=resData3[resData3.idTipo.notnull()]
        resData3=resData3.astype({'idTipo':'category','desMovimentacao':'category'})
        resData4=self.dataXLS.parse('Plano_Contas',index_col=None,na_values=['NA']\
                                   ,usecols="O:Q",skiprows=[0])
        resData4.columns=['idUN','desUnidade','conversao']
        resData4=resData4[resData4.idUN.notnull()]
        resData4=resData4.astype({'idUN':'category','desUnidade':'category','conversao':'float64'})
        resData5=self.dataXLS.parse('Empresas',index_col=None,na_values=['NA','',' ']\
                                   ,usecols="A:AB",skiprows=[0])
        resData5.columns=['idComp','desRasao','fantasia','matriz','idRamo','ramo','idGrp','grupo',\
                          'cnpj','ie','im','ddi','ddd','telefone','pabx','fax','email','website',\
                          'portal','smtp','cep','endereco','nr','complemento','notas','despesas',\
                          'entradas','situacao']
        resData5=resData5[resData5.idComp.notnull()]
        resData5.fantasia=resData5.fantasia.str.lower()
        resData5=resData5.astype({'idComp':'int64','desRasao':'category','fantasia':'category',\
                                  'matriz':'category','ramo':'category',\
                                  'grupo':'category','cep':'category','endereco':'category',\
                                  'complemento':'category','email':'category','situacao':'category',\
                                  'cnpj':'category','telefone':'category'})
        #'idRamo':'int64','idGrp':'int64','ddi':'int64','ddd':'int64',
        resData6=self.dataXLS.parse('Parametros',index_col=None,na_values=['NA']\
                                   ,usecols="A:C",skiprows=[0])
        resData6.columns=['idRamo','ramo','desRamo']
        resData6=resData6[resData6.idRamo.notnull()]
        resData6=resData6.astype({'idRamo':'int64','ramo':'category','desRamo':'category'})
        resData5=resData5.merge(resData6[['idRamo','desRamo']],on='idRamo',how='left')
        resData=DataFrame({'id':[0,1,2,3,4,5],'name':['Accounts Plan','Groups','Categories','Types',\
                           'Units','Companies']\
                          ,'df':[resData0,resData1,resData2,resData3,resData4,resData5]})
        return resData
    def loadCash(self,errAct='raise'):
        if self.isOK==False: #errAct='raise','coerce','ignore'
            raise Exception('Cash Flow file not available! ['+self.fileName+']')
        from pandas import DataFrame, to_datetime, concat, NaT #, to_numeric
        lstCash=list(filter(lambda x:x.startswith('Fluxo_de_Caixa'),self.dataXLS.sheet_names))
        if len(lstCash)<1:
            raise Exception('Cashflow data not available! [Fluxo_de_Caixa????]')
        dfCash=DataFrame(index=[], columns=['id','desConta','fantasia','dtPrevista','vlPrevisto','dtReal','vlReal'])
        for loop in lstCash:
            #print(loop) lData[lData.columns[-1]] #lData.iloc[:,-1:]
            tmpReadCols='A:AA' if (self.dataXLS.parse(loop,index_col=None,\
                na_values=['NA','∑'],nrows=1,skiprows=[0]).columns[27]=='Realizado') else "A:AY"
            lData=self.dataXLS.parse(loop,index_col=None,na_values=['NA','∑']\
                                   ,usecols=tmpReadCols,skiprows=[0])
            tmpYear=loop.replace('Fluxo_de_Caixa','').strip() #print(loop,tmpYear)
            if (len(lData.columns)<30): #[-2][:9]!='Realizado'):
                #lData=lData.iloc[:,0:27]
                lData.columns=['id','desConta','fantasia','dtReal1','vlReal1',\
                               'dtReal2','vlReal2','dtReal3','vlReal3',\
                               'dtReal4','vlReal4','dtReal5','vlReal5',\
                               'dtReal6','vlReal6','dtReal7','vlReal7',\
                               'dtReal8','vlReal8','dtReal9','vlReal9',\
                               'dtReal10','vlReal10','dtReal11','vlReal11',\
                               'dtReal12','vlReal12']
                lData[['dtPrev1','vlPrev1','dtPrev2','vlPrev2','dtPrev3','vlPrev3','dtPrev4','vlPrev4']]=None
                lData[['dtPrev5','vlPrev5','dtPrev6','vlPrev6','dtPrev7','vlPrev7','dtPrev8','vlPrev8']]=None
                lData[['dtPrev9','vlPrev9','dtPrev10','vlPrev10','dtPrev11','vlPrev11','dtPrev12','vlPrev12']]=None
            else:
                lData.columns=['id','desConta','fantasia','dtPrev1','vlPrev1','dtReal1','vlReal1',\
                               'dtPrev2','vlPrev2','dtReal2','vlReal2',\
                               'dtPrev3','vlPrev3','dtReal3','vlReal3',\
                               'dtPrev4','vlPrev4','dtReal4','vlReal4',\
                               'dtPrev5','vlPrev5','dtReal5','vlReal5',\
                               'dtPrev6','vlPrev6','dtReal6','vlReal6',\
                               'dtPrev7','vlPrev7','dtReal7','vlReal7',\
                               'dtPrev8','vlPrev8','dtReal8','vlReal8',\
                               'dtPrev9','vlPrev9','dtReal9','vlReal9',\
                               'dtPrev10','vlPrev10','dtReal10','vlReal10',\
                               'dtPrev11','vlPrev11','dtReal11','vlReal11',\
                               'dtPrev12','vlPrev12','dtReal12','vlReal12']
            lData=lData[lData.id.notnull()]
            for iMes in range(1,13):
                if (iMes>12):
                    break
                lData[['dtPeriodo'+str(iMes)]]=tmpYear+'-'+str(iMes)+'-1 00:00:00'
                #dfCash=dfCash.append(lData.rename(columns={'dtPeriodo'+str(iMes):'dtPeriodo',\
                #          'dtPrev'+str(iMes):'dtPrevista','vlPrev'+str(iMes):'vlPrevisto',
                #          'dtReal'+str(iMes):'dtReal','vlReal'+str(iMes):'vlReal'}\
                #          )[['id','desConta','fantasia'\
                #          ,'dtPeriodo','dtPrevista','vlPrevisto','dtReal','vlReal']],ignore_index=True)
                dfCash=concat([dfCash,lData.rename(columns={'dtPeriodo'+str(iMes):'dtPeriodo',\
                          'dtPrev'+str(iMes):'dtPrevista','vlPrev'+str(iMes):'vlPrevisto',
                          'dtReal'+str(iMes):'dtReal','vlReal'+str(iMes):'vlReal'}\
                          )[['id','desConta','fantasia'\
                          ,'dtPeriodo','dtPrevista','vlPrevisto','dtReal','vlReal']]],ignore_index=True)
                dfCash.loc[(dfCash['dtReal'].isnull()==True),'dtReal']=tmpYear+'-'+str(iMes)+'-1 00:00:00'
            tmpYear=''
        dfCash.loc[(dfCash['vlPrevisto'].isin([0,' ','0',0.0])),'vlPrevisto']=None
        dfCash.loc[(dfCash['vlReal'].isin([0,' ','0',0.0])),'vlReal']=None
        dfCash=dfCash.drop(dfCash[(dfCash['vlPrevisto'].isnull()) & (dfCash['vlReal'].isnull())].index)
        dfCash.fantasia=dfCash.fantasia.str.lower()
        dfCash=dfCash.astype({'id':'int64','desConta':'category','fantasia':'category',\
                              'vlPrevisto':'float64','vlReal':'float64'})
        dfCash.dtReal=to_datetime(dfCash['dtReal'].fillna(NaT), errors=errAct) #format='%Y-%m-%d %H:%M:%S', 
        dfCash.dtPrevista=to_datetime(dfCash['dtPrevista'].fillna(NaT), errors=errAct)
        dfCash.dtPeriodo=to_datetime(dfCash['dtPeriodo'], errors=errAct)
        dfCash.loc[(dfCash.vlPrevisto.isnull()==False),'perMetaVl']=\
            (dfCash.loc[(dfCash.vlPrevisto.isnull()==False),'vlReal'].fillna(0)/\
            dfCash.loc[(dfCash.vlPrevisto.isnull()==False),'vlPrevisto'])
        dfCash.loc[(dfCash.dtPrevista.isnull()==False),'qtDiasMeta']=\
            (dfCash.loc[(dfCash.dtPrevista.isnull()==False),'dtReal']-\
            dfCash.loc[(dfCash.dtPrevista.isnull()==False),'dtPrevista'])
        return dfCash
    def mergeCashParam(self,dfCash,dfParam,dfCotation=None,dfPresidents=None,dfClima=None):
        from pandas import DataFrame, get_dummies, concat, to_datetime
        import re
        from sklearn.cluster import KMeans
        assert isinstance(dfCash, DataFrame), 'mergeCashParam() parameter dfCash={} not a Pandas DataFrame'.format(dfCash)
        assert isinstance(dfParam, DataFrame), 'mergeCashParoam() parameter dfParam={} not a Pandas DataFrame'.format(dfParam)
        if (['id', 'desConta', 'fantasia', 'dtPrevista', 'vlPrevisto', 'dtReal', 'vlReal',\
             'dtPeriodo', 'perMetaVl', 'qtDiasMeta']!=dfCash.columns.to_list()):
            raise Exception('Cashflow dataframe do not match!!['+','.join(dfCash.columns.to_list())+']')
        if (['id', 'name', 'df']!=dfParam.columns.to_list()):
            raise Exception('Cashflow param dataframe do not match!['+','.join(dfParam.columns.to_list())+']')
        dfParu=dfParam.df[0].merge(dfParam.df[1]).merge(dfParam.df[2]).merge(dfParam.df[3])
        dfCashf=dfCash[['id','fantasia','dtPrevista','vlPrevisto','dtReal','vlReal','dtPeriodo','perMetaVl','qtDiasMeta']].\
                merge(dfParu,on='id',how='left') #, indicator=True
        dfCashf=dfCashf.merge(dfParam.df[5][['idComp','cnpj','desRasao','fantasia','matriz','idRamo','ramo','desRamo',\
                                             'idGrp','grupo','email','ddi','ddd','telefone']],on='fantasia',how='left')
        dfCashf.loc[dfCashf.email.notnull(),'email2']=dfCashf.loc[dfCashf.email.notnull(),'email']\
             .apply(lambda x:",".join(re.findall(r'[\w\.-]+@[\w\.-]+',str(x))))
        dfCashf.email = dfCashf.email2
        dfCashf.loc[dfCashf.cnpj.notnull(),'cnpj2']=dfCashf.loc[dfCashf.cnpj.notnull(),'cnpj']\
             .apply(lambda x:"".join(re.findall(r'\d+',str(x)))[0:14])
        dfCashf.cnpj = dfCashf.cnpj2
        del dfCashf['email2']
        del dfCashf['cnpj2']
        dfCashf=concat([dfCashf,get_dummies(dfCashf.idTipo)],axis=1)
        dfCashf['vlReceita']=dfCashf[dfCashf.idTipo=='E'][['vlReal']]
        dfCashf['vlDespesa']=dfCashf[dfCashf.idTipo.isin(['S','V','I'])][['vlReal']]
        dfCashf=dfCashf.astype({'fantasia':'category','email':'category','cnpj':'category',\
                                'idComp':'Int64','idGrp':'Int64','idRamo':'Int64','ddi':'Int64','ddd':'Int64'})
        kmeans = KMeans(n_clusters=7) #estudo elbow/silhouette apontam para 7
        dfCashf['clusterVl'] = kmeans.fit_predict(dfCashf[['id','vlPrevisto','vlReal','E','S','V','I','T']].fillna(0))
        #kmeans.cluster_centers_
        tmpConv={}
        if isinstance(dfCotation, DataFrame):
            lstCoinsName=dfCotation.siglaMoeda.unique()
            lstCoinsCod=dfCotation.codigoMoeda.unique()
            dfCashf['dtCotation']=dfCashf['dtReal'].fillna(dfCashf['dtPeriodo'])
            dictCoins = {lstCoinsName[i]: lstCoinsCod[i] for i in range(len(lstCoinsName))}
            for coinKey, coinValue in dictCoins.items():
                dfCashf['cotation'+coinKey]=str(coinValue)+dfCashf['dtCotation'].dt.strftime("%Y%m%d")
                dfCashf['cotation'+coinKey]=dfCashf['cotation'+coinKey].astype('Int64')
                dfCashf=dfCashf.merge(dfCotation[['id','taxaVenda']].rename({'id':'cotation'+coinKey}, axis=1),\
                    on='cotation'+coinKey).drop('cotation'+coinKey,axis=1).rename({'taxaVenda':'cotation'+coinKey},axis=1)
                tmpConv['cotation'+coinKey]='float64'
        if isinstance(dfPresidents, DataFrame):
            assert ('dtCotation' in dfCashf.columns),'Error: dfCotation is needed to define dtCotation! Add this dataframe.'
            dfPresidents.loc[dfPresidents.fim.isnull()==True,'fim']=to_datetime('today')
            dfPresidents=dfPresidents.loc[dfPresidents.fim>=min(dfCashf.dtCotation)]
            dfPresidents=dfPresidents.loc[dfPresidents.inicio.isnull()==False]
            dfCashf['idPresident']=None
            for i in dfCashf.index:
                dfCashf.at[i,'idPresident']=dfPresidents.loc[(dfPresidents.inicio<=dfCashf.at[i,'dtCotation']) & \
                (dfPresidents.fim>=dfCashf.at[i,'dtCotation']),'id'].values[0]
            tmpConv['idPresident']='int64'
        if isinstance(dfClima, DataFrame):
            assert ('dtCotation' in dfCashf.columns),'Error: dfCotation is needed to define dtCotation! Add this dataframe.'
            assert (len(str(dfClima.id[0]))==8),'Climate df need to be a daily indexed data! With retdetail=2.1dayavg.'
            dfClima=dfClima.loc[dfClima.id>=int(min(dfCashf.dtCotation).strftime("%Y%m%d"))]
            dfCashf['med_umidRelativa']=None
            dfCashf['med_precipitacao_mm']=None
            dfCashf['med_pressao']=None
            dfCashf['med_velVento_ms']=None
            dfCashf['med_temperatura_c']=None
            dfCashf['med_radiacao_kjm2']=None
            for c in dfCashf.index:
                if (dfCashf.at[c,'dtCotation']<min(dfClima.data)):
                    continue
                if (dfCashf.at[c,'dtCotation']>max(dfClima.data)):
                    continue
                dfCashf.at[c,'med_umidRelativa']=dfClima.loc[dfClima.id==\
                    int(dfCashf.at[c,'dtCotation'].strftime("%Y%m%d")),'umidrel_mean'].values[0]
                dfCashf.at[c,'med_precipitacao_mm']=dfClima.loc[dfClima.id==\
                    int(dfCashf.at[c,'dtCotation'].strftime("%Y%m%d")),'precTotal_mm_mean'].values[0]
                dfCashf.at[c,'med_pressao']=dfClima.loc[dfClima.id==\
                    int(dfCashf.at[c,'dtCotation'].strftime("%Y%m%d")),'pressao_mean'].values[0]
                dfCashf.at[c,'med_velVento_ms']=dfClima.loc[dfClima.id==\
                    int(dfCashf.at[c,'dtCotation'].strftime("%Y%m%d")),'vento_velmax_ms_mean'].values[0]
                dfCashf.at[c,'med_temperatura_c']=dfClima.loc[dfClima.id==\
                    int(dfCashf.at[c,'dtCotation'].strftime("%Y%m%d")),'tempseco_c_mean'].values[0]
                dfCashf.at[c,'med_radiacao_kjm2']=dfClima.loc[dfClima.id==\
                    int(dfCashf.at[c,'dtCotation'].strftime("%Y%m%d")),'radiacao_kjm2_mean'].values[0]
            tmpConv['med_umidRelativa']='float64'
            tmpConv['med_precipitacao_mm']='float64'
            tmpConv['med_pressao']='float64'
            tmpConv['med_velVento_ms']='float64'
            tmpConv['med_temperatura_c']='float64'
            tmpConv['med_radiacao_kjm2']='float64'
        if (len(tmpConv)>0):
            dfCashf=dfCashf.astype(tmpConv)
            #self.__limits__(bi[['med_radiacao_kjm2']])
        return dfCashf
    def writeXls(self,dfBI):
        from pandas import DataFrame
        assert isinstance(dfBI, DataFrame), 'writeXls: Parameter dfBI={} not a Pandas DataFrame'.format(dfBI)
        self.biFile=self.homeDir+"/cashFlowBI.xlsx"
        dfBI.to_excel(self.biFile)
        print('File:[',self.biFile,'] exported!')

#import Documents.cashFlowXLS as cf
#from Documents.cashFlowXLS import *
#cf=CashFlow('Documents/Fluxo_de_Caxa2020.xlsx') #cf=CashFlow()
#cashPar=cf.loadParam()
#cashPar.shape #cashPar['df'].iloc[0]
#cashPar.info() #cashPar.df[0].info()
#flowCash=cf.loadCash('coerce')
#flowCash.shape
#flowCash.info()
#bi=cf.mergeCashParam(flowCash,cashPar)
#bi.shape
#bi.info()
#bi.describe()
#cf.writeXls(bi)
#cs['dtPrevista']=pd.to_datetime(cs['dtPrevista'],format='%Y-%m-%d %H:%M:%S')
#https://stackoverflow.com/questions/17977540/pandas-looking-up-the-list-of-sheets-in-an-excel-file
#https://stackoverflow.com/questions/14916284/in-class-object-how-to-auto-update-attributes
#https://towardsdatascience.com/how-to-show-all-columns-rows-of-a-pandas-dataframe-c49d4507fcf
#all(df.colx.index == range(df.colx.shape[0]))# True
#df.index.duplicated().any()# False
#https://zapier.com/blog/extract-links-email-phone-regex/

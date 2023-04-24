import os
tmpOSDir=['C:\\Users\\Python','/Users/Python3/cashflow/']\
   if (os.name=='nt') else ['/home/usr','/home/usr/cashflow/']
os.chdir(tmpOSDir[0])
from cashflow.readBBCota import ReadBBCota
moedas=ReadBBCota(start_date='2023-04-18',cotationfile=tmpOSDir[1]+'cotacoesBB.xlsx')
moedas.writeXls()
from cashflow.readPresBRwiki import ReadPresBR
presidentes=ReadPresBR(hist_ignore=False, presidentsfile=tmpOSDir[1]+'presidentesBRw.xlsx')
presidentes.writeXls()
from cashflow.readInmetClima import ReadInmet
climaSP=ReadInmet(retdetail=2,startyear=2023,inmetfile=tmpOSDir[1]+'climaInmet2lvl.xlsx')
climaSP.writeXls()
from cashflow.cashFlowXLS import *
cf=CashFlow(tmpOSDir[1]+'Fluxo_de_Caixa2023.xlsx')
cashPar=cf.loadParam()
flowCash=cf.loadCash('coerce')
bi=cf.mergeCashParam(flowCash,cashPar,moedas.dataframe,presidentes.dataframe,climaSP.dataframe)
del(cf,cashPar,flowCash,moedas,climaSP)
print(bi)
cf.writeXls(bi)
#-----------------------------------
from cashflow.readComexStat import ReadCOMEX
comexData=ReadCOMEX(start_date='2023-01-01',end_date='2023-02-01',comexfile=tmpOSDir[1]+'COMEXStat.xlsx')
comexData.writeXls()
#-----------------------------------
from cashflow.dataDiscovery import Discover
from cashflow.readBBTaxcred import ReadCredTax
taxaCreditoBR=ReadCredTax(hist_ignore=True)
taxaCreditoBR.writeXls()
#-----------------------------------
from cashflow.readCepeaStream import ReadEsalq
cepeaEtanol=ReadEsalq(hist_ignore=True,indicator=111)
cepeaEtanol.writeXls()


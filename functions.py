# Librerías MIAS
import pandas as pd
import numpy as np
import re
from datetime import datetime
from os import listdir,path
import os
from pandas.core.frame import DataFrame
import yfinance as yf

class Acomodar_Data:
    
    def __init__(self, direccion: str):
        self.direccion= direccion
    
    def obtener_fechas(self):
        ##Ordenar las fechas de los archivos
        file_dates = []
        for f in os.listdir(self.direccion):
            file_dates.append(re.search(r'\d+',f).group(0))

        file_dates.sort(key = lambda date: datetime.strptime(date,'%Y%m%d'))
        return file_dates
    
    def correcciones_arch(self,file_dates:list):
        data_files = {}

        for i in file_dates:
            
            #Leer los archivos
            data = pd.read_csv(self.direccion +'/NAFTRAC_' + i + '.csv', skiprows=2)
            
            # Elimina NaN's
            data= data.dropna(how = 'any')
            
            #Cambiar precio a str para poder corregirlo
            data = data.astype({'Precio': str})
            
            # Quitar las comas a precio para poder hacerlo float
            data['Precio'] = [i.replace(',', '') for i in data['Precio']]
            
            #Quitar * para poder leerlo con Yahoo Finance
            data['Ticker'] = [i.replace('*', '') for i in data['Ticker']]
            
            #Corregir nombres de tickers
            data['Ticker'] = data['Ticker'].str.replace('GFREGIOO', 'RA', regex=True)
            data['Ticker'] = data['Ticker'].str.replace('MEXCHEM', 'ORBIA', regex=True)
            data['Ticker'] = data['Ticker'].str.replace('LIVEPOLC.1', 'LIVEPOLC-1', regex=True)
            data['Ticker'] = data['Ticker'].str.replace('SITESB.1', 'SITESB-1', regex=True)
            
            #Eliminar todas aquellas acciones que no tengan informacion o que son cash
            data.drop(data.index[data['Ticker'] == 'KOFL'], inplace = True)
            data.drop(data.index[data['Ticker'] == 'BSMXB'], inplace = True)
            data.drop(data.index[data['Ticker'] == 'NMKA.MX'], inplace = True)
            data.drop(data.index[data['Ticker'] == 'KOFUBL.MX'], inplace = True)
            data.drop(data.index[data['Ticker'] == 'MXN'], inplace = True)
            data.drop(data.index[data['Ticker'] == 'USD'], inplace = True)
            
            #Volver a hacer precio float para hacer operaciones
            data = data.astype({'Precio': float})
            #data['Ticker']= data['Ticker']+'.M'
            data['Ticker'] =  data['Ticker'].astype(str) + '.MX'
            
            # Sacar el peso en formato de porcentaje
            data['Peso (%)'] = data['Peso (%)']/ 100
            data_files[i] = data
            
        return data_files

    def tickers_unq(self,data_files:dict,file_dates:list):
        # Se obtiene una lista con los tickers unicos
        alltickers=[list(data_files[i]['Ticker']) for i in file_dates]
        alltickers = [item for sublist in alltickers for item in sublist]
        tickers= list(set(alltickers))
        tickers.sort()
        return tickers

    def formato_fechas(self, file_dates:list):
        dates=[(pd.to_datetime(file_dates[i]).date()).strftime('%Y-%m-%d') for i in range(len(file_dates))]
        return dates
    
    def info_yahoo(self, dates:list,tickers:list):
        # Se baja la información de YahooFinance
        start_d = dates[0]
        yahoo_data = yf.download(tickers, start= start_d, actions=False, group_by="close", interval='1d')
        return yahoo_data
    
    def obtener_cierres(self,tickers:list,yahoo_data:DataFrame):
        data_close = yahoo_data.loc[ : , ([0], ['Close'])] 

        for tick in tickers:
            closes=yahoo_data.loc[ : , (tick , ['Close'])] 
            closes= closes.droplevel(1, axis=1)
            data_close= pd.concat([data_close,closes], axis=1, join="inner")
        
        return data_close
    
    def closes_fnec(self, data_close: DataFrame, dates:list):
        data_close.reset_index(inplace=True)
        data_closeT = data_close[data_close['Date'].isin(dates)]
        data_closeT.set_index('Date', inplace=True)
        return data_closeT

class Inversion_pasiva:

    def __init__(self):
        pass

    def creacion_tabla(self, data_files:dict ,file_dates: list ,data_closeT: DataFrame, k:float,c:float):
        fir_data= data_files[file_dates[0]]
        fir_data= fir_data [['Ticker', 'Peso (%)']]
        fir_data = fir_data [~fir_data ['Ticker'].isin(['KOFL','BSMXB','NMKA.MX','KOFUBL.MX','MXN','USD' ])] 
        fir_data.reset_index(inplace=True, drop=True)

        prices_p= pd.DataFrame(data_closeT.T[file_dates[0]])
        prices_p.reset_index(inplace=True)
        prices_p= prices_p.rename(columns = { prices_p.columns[0]: 'Ticker', prices_p.columns[1]: 'Precios'}, inplace = False)

        fir_data = pd.merge(fir_data, prices_p,on=['Ticker'])
        fir_data = fir_data.sort_values('Ticker')
        fir_data. reset_index(drop=True)

        fir_data['Titulos'] = np.floor((k * fir_data['Peso (%)']) / (fir_data['Precios'] + (fir_data['Precios'] * c)))
        fir_data['Capital'] = np.round(fir_data['Titulos'] * (fir_data['Precios'] + (fir_data['Precios'] * c)), 2)
        fir_data['Postura'] = np.round(fir_data['Precios'] * fir_data['Titulos'], 2)
        fir_data['Comisiones'] = np.round(fir_data['Precios'] * c * fir_data['Titulos'], 2)
        fir_data. reset_index(drop=True)
        
        return fir_data

    def inv_fechas(self,dates,fir_data,data_closeT,file_dates,cash,k):

        passive_inv = {'Dates': [dates[0]], 'Capital': [k]}

        fir_data_Tickers= fir_data['Ticker'].tolist()

        for i in range(len(dates)):

            base= pd.DataFrame(data_closeT.T[file_dates[i]])
            base.reset_index(inplace=True)
            base = base[base[base.columns[0]].isin(fir_data_Tickers)]
            base.set_index('index', inplace=True)
            
            fir_data['Precios'] = np.array(base.iloc[:, 0])
            fir_data['Postura'] = np.round(fir_data['Precios'] * fir_data['Titulos'], 2)
            passive_inv['Capital'].append(np.round(fir_data['Postura'].sum(), 2) + cash)
            passive_inv['Dates'].append(dates[i])
            
        return passive_inv
    
    def base_inv_pasiva(self, passive_inv:dict):
        df_passive = pd.DataFrame()
        df_passive['Dates']= passive_inv['Dates']
        df_passive['Capital']= passive_inv['Capital']

        for i in range(1, len(df_passive)):
            df_passive.loc[i, 'Rendimiento'] = (df_passive.loc[i, 'Capital'] - df_passive.loc[i - 1, 'Capital']) / \
                                                df_passive.loc[i - 1, 'Capital']

            df_passive['Rendimiento Acumulado'] = df_passive['Rendimiento'].cumsum()

        return df_passive

################ INVERSION ACTIVA ############

class Inversion_activa:
    
    def __init__(self):
        pass
    
    def prices_pre(self,j_prices:DataFrame,fecha_act:list):

        prices_pre= np.log(j_prices).diff() 
        prices_pre.reset_index(inplace=True)
        prices_pre = prices_pre[prices_pre['Date'] >= fecha_act]
        prices_pre.set_index('Date', inplace=True)
        
        return prices_pre
    
    def portafolio_act(self,prices_pre:DataFrame, periodicidad,tasa_lr,mat_cov):
        
        data = {'Returns':[],'Volatility':[],'Sharpe':[]}
        num_assets = len(prices_pre.columns)
        num_portfolios = 100
        indv_returns = prices_pre.mean()
        p_weights = []

        for i in range(num_portfolios):
            weights = np.random.random(num_assets)
            weights = weights / np.sum(weights)
            p_weights.append(weights)
            returns = np.dot(weights, indv_returns * periodicidad) - tasa_lr

            data['Returns'].append(returns)

            variance = weights.T @ mat_cov @ weights
            stand_dev = np.sqrt(variance)

            data['Volatility'].append(stand_dev)

            sharpe_r = returns / stand_dev

            data['Sharpe'].append(sharpe_r)

        for j, s in enumerate(prices_pre.columns.tolist()):
                data[s + ' weight'] = [w[j] for w in p_weights]
        
        portafolios = pd.DataFrame(data)
        return portafolios
    
    def obtener_w_opt(self,portafolios):
        maximo_sharpe = (portafolios.iloc[portafolios['Sharpe'].idxmax()]).to_list()
        w_opt = maximo_sharpe[3:]
        return w_opt

    def pos_prices_f(self,j_prices,fecha_act):
        j_prices.reset_index(inplace=True)
        pos_prices_f = j_prices[j_prices['Date'] >= fecha_act]
        lista=  (pos_prices_f['Date']).tolist()
        pos_prices_f.set_index('Date', inplace=True)
        j_prices.set_index('Date', inplace=True)
        lista= [(pd.to_datetime(lista[i]).date()).strftime('%Y-%m-%d') for i in range(len(lista))]
        return pos_prices_f,lista

    def pos_prices(self,j_prices,fecha_act):
        pos_prices = j_prices.pct_change()
        pos_prices.reset_index(inplace=True)
        pos_prices = pos_prices[pos_prices['Date'] >= fecha_act]
        lista2 = (pos_prices['Date']).tolist()
        pos_prices.set_index('Date', inplace=True)
        lista2= [(pd.to_datetime(lista2[i]).date()).strftime('%Y-%m-%d') for i in range(len(lista2))]

        return pos_prices,lista2
        
    def obtener_portafolio_optimo(self,pos_prices_f,w_opt,net_capital,c):
        port_opt = pd.DataFrame({'Ticker':pos_prices_f.columns.tolist() , 'Precios':pos_prices_f.iloc[0,:].tolist()})
        port_opt['Peso']= w_opt
        port_opt['Postura'] = np.round(net_capital * port_opt['Peso'], 2)
        port_opt['Titulos'] = np.floor(port_opt['Postura'] / port_opt['Precios'])
        port_opt['Comisiones'] = np.round(port_opt['Precios'] * c * port_opt['Titulos'], 2)
        port_opt = port_opt.set_index('Ticker')

        return port_opt
    
    def updown(self,pos_prices):
        down=[]
        up=[]

        for i in range(len(pos_prices.iloc[0,:])):
            if pos_prices.iloc[0,:][i] <= -.05:
                down.append(pos_prices.iloc[0,:].index[i])
            else:
                up.append(pos_prices.iloc[0,:].index[i])
        
        return down,up
    
    def obtener_nuevo_port(self,j_prices,port_opt,down,c):
        titulos_ant = []

        for i in range(2):
            port_nuev = pd.DataFrame({'Ticker':j_prices.columns.tolist(),'Precio': j_prices.iloc[25+i,:].to_list()}) 
            #Periodo de pandemia 2020-02-28
            port_nuev = port_nuev.set_index("Ticker")

            if i == 0:
                port_nuev['Titulos anteriores'] = port_opt['Titulos'].to_list()
            else:
                port_nuev["Titulos anteriores"] = titulos_ant.to_list()


            new_titulos = []
            for ticker in (j_prices.columns.tolist()):
                if ticker in down:
                    n_titulos = port_nuev.loc[ticker, 'Titulos anteriores'] * .975
                    new_titulos.append(n_titulos)
                else:
                    n_titulos = port_nuev.loc[ticker, 'Titulos anteriores']
                    new_titulos.append(n_titulos)
            
            
            port_nuev['Nuevos Titulos'] = np.floor(new_titulos)
            titulos_ant = port_nuev['Nuevos Titulos']
            port_nuev['Nuevo Valor'] = port_nuev['Nuevos Titulos'] * port_nuev['Precio']
            port_nuev['Valor Venta'] = np.round(
                (port_nuev['Titulos anteriores'] - port_nuev['Nuevos Titulos']) * port_nuev['Precio'], 2)
            port_nuev['Comisiones venta'] = port_nuev['Valor Venta'] * c

            return port_nuev
        
    def valor_portafolio(self,cash,pos_prices_f,j_prices,port_opt,down,up,c):
        titulos_ant = []
        new_cash= cash
        acciones_compra = []
        comisiones_compra=[]
        acciones_venta = []
        comisiones_venta= []
        valor_portafolio = []

        for i in range(len(pos_prices_f)):
            port_nuev = pd.DataFrame({'Ticker':j_prices.columns.tolist(),'Precio': j_prices.iloc[25+i,:].to_list()}) #Periodo de pandemia 2020-02-28
            port_nuev = port_nuev.set_index("Ticker")

            if i == 0:
                port_nuev['Titulos anteriores'] = port_opt['Titulos'].to_list()
            else:
                port_nuev["Titulos anteriores"] = titulos_ant.to_list()


            new_titulos = []
            for ticker in (j_prices.columns.tolist()):
                if ticker in down:
                    n_titulos = port_nuev.loc[ticker, 'Titulos anteriores'] * .975
                    new_titulos.append(n_titulos)
                else:
                    n_titulos = port_nuev.loc[ticker, 'Titulos anteriores']
                    new_titulos.append(n_titulos)
            
            
            port_nuev['Nuevos Titulos'] = np.floor(new_titulos)
            titulos_ant = port_nuev['Nuevos Titulos']
            port_nuev['Nuevo Valor'] = port_nuev['Nuevos Titulos'] * port_nuev['Precio']
            port_nuev['Valor Venta'] = np.round(
                (port_nuev['Titulos anteriores'] - port_nuev['Nuevos Titulos']) * port_nuev['Precio'], 2)
            
            acciones_venta.append(port_nuev["Valor Venta"].sum())
            
            port_nuev['Comisiones venta'] = port_nuev['Valor Venta'] * c
            comisiones_venta.append(port_nuev["Comisiones venta"].sum())
            
            
            new_cash = new_cash + port_nuev['Valor Venta'].sum() - port_nuev['Comisiones venta'].sum()
            
            valor_compra = []
            
            cash_buy = new_cash
            for ticker in (j_prices.columns.tolist()):
                if cash_buy>=0 and ticker in up:
                    n_titulos = port_nuev.loc[ticker,'Titulos anteriores'] * 1.025
                    port_nuev.loc[ticker,'Nuevos Titulos'] = np.floor(n_titulos)
                    compra = np.round((port_nuev.loc[ticker,'Titulos anteriores'] - port_nuev.loc[ticker,'Nuevos Titulos']) * port_nuev.loc[ticker,'Precio'],2)*-1
                    if compra > cash_buy:
                        compra = 0
                else:
                    compra = 0
                valor_compra.append(compra)
                cash_buy = cash_buy - compra
                
            port_nuev["Valor Compra"] = valor_compra
            acciones_compra.append(port_nuev["Valor Compra"].sum())
            
            titulos_ant = port_nuev["Nuevos Titulos"]
            port_nuev["Comisiones Compra"] = port_nuev["Valor Compra"] * -c
            comisiones_compra.append(port_nuev["Comisiones Compra"].sum()*-1)
            port_nuev["Nuevo Valor"] = port_nuev["Nuevos Titulos"] * port_nuev["Precio"]
            
            #Manejo de efectivo despues de compra 
            new_cash = new_cash - port_nuev["Valor Compra"].sum() + port_nuev["Comisiones Compra"].sum()
            valor_portafolio.append(new_cash + port_nuev["Nuevo Valor"].sum())

        return valor_portafolio, acciones_compra, acciones_venta, comisiones_compra,comisiones_venta

    def df_activa(self,valor_portafolio, lista):
        valor_portafolio.insert(0, 1000000)
        lista.insert(0,lista[0])
        df_activa = pd.DataFrame({'Dates': lista, 'capital':valor_portafolio})
        df_activa['Rendimiento']= df_activa.capital.diff() / df_activa.capital
        df_activa['Rendimiento Acumulado']=  df_activa['Rendimiento'].cumsum()
        pd.set_option('display.float_format', lambda x: '%.3f' % x)

        return df_activa
    
    def df_operaciones(self,lista2,acciones_compra,acciones_venta,comisiones_compra,comisiones_venta):
        df_operaciones = pd.DataFrame({'timestamp':lista2 , 'titulos comprados / total operacion':acciones_compra, 
                              'titulos vendidos / total operacion': acciones_venta, 'comisiones compra':comisiones_compra,
                              'comisiones venta': comisiones_venta})

        df_operaciones['comisiones mes'] = df_operaciones['comisiones compra'] + df_operaciones['comisiones venta']
        df_operaciones['comisiones acumuladas'] = df_operaciones['comisiones mes'].cumsum()
        return df_operaciones
    
    def medidas_desempeno(self,prepa,df_activa,periodicidad):
        df_desempeno = pd.DataFrame()
        rend_1 = [((df_activa['Rendimiento']*periodicidad).mean()- .0429),((prepa['Rendimiento']*periodicidad).mean()- .0429)]
        rend_2 = [df_activa['Rendimiento Acumulado'].iloc[-1],prepa['Rendimiento Acumulado'].iloc[-1]]
        df_desempeno['Tipo de inversion'] = ['activa','pasiva']
        df_desempeno['Rend_1'] = rend_1
        df_desempeno['Rend_2'] = rend_2
        df_desempeno.set_index('Tipo de inversion')
        return df_desempeno
from functions import *
from os import path
import numpy as np

#Variable que guarda la direccion de los archivos "files"
abspath = path.abspath('files')

# Crear objeto
mis_datos = Acomodar_Data(abspath)

## Acomodar datos
file_dates = mis_datos.obtener_fechas()
data_files = mis_datos.correcciones_arch(file_dates)
tickers = mis_datos.tickers_unq(data_files,file_dates)
dates = mis_datos.formato_fechas(file_dates)
yahoo_data = mis_datos.info_yahoo(dates,tickers)
data_close = mis_datos.obtener_cierres(tickers,yahoo_data)
data_closeT= mis_datos.closes_fnec(data_close,dates)

### Inversion Pasiva

#Crear objeto
mi_inv_pasiva = Inversion_pasiva()

capital = 1000000
comision = 0.00125
fir_data= mi_inv_pasiva.creacion_tabla(data_files, file_dates,data_closeT,capital,comision)

# Efectivo 
cash = (1-fir_data['Peso (%)'].sum())* capital

# Sumatoria de las columnas de las comisiones
comision_sum = fir_data['Comisiones'].sum()

# Obteniendo inversion pasiva
passive_inv = mi_inv_pasiva.inv_fechas(dates,fir_data,data_closeT,file_dates,cash,capital)
inv_passive= mi_inv_pasiva.base_inv_pasiva(passive_inv)

# PRE PANDEMIC
prepa = inv_passive.loc[0:25]

# POS PANDEMIC
pospa= inv_passive.loc[25:].copy()
pospa.loc[25, 'Capital'] = capital
pospa.loc[25, 'Rendimiento']= 0
pospa['Rendimiento Acumulado'] = pospa['Rendimiento'].cumsum()


## Inversion Activa

activ_tick= fir_data['Ticker'].tolist()
j_prices= data_closeT[activ_tick]
fecha_act= '2020-02-28'
tasa_lr= 0.0425
periodicidad= 12

# Crear objeto
mi_inv_activa = Inversion_activa()

prices_pre = mi_inv_activa.prices_pre(j_prices,fecha_act)

# Seleccionar el portafolio con mayor sharpe
mat_cov = prices_pre.cov() * np.sqrt(periodicidad)
portafolios= mi_inv_activa.portafolio_act(prices_pre, periodicidad,tasa_lr,mat_cov)
w_opt = mi_inv_activa.obtener_w_opt(portafolios)

net_capital = 1000000 - cash

pos_prices_f,lista= mi_inv_activa.pos_prices_f(j_prices,fecha_act)
pos_prices,lista2= mi_inv_activa.pos_prices(j_prices,fecha_act)

port_opt= mi_inv_activa.obtener_portafolio_optimo(pos_prices_f,w_opt, net_capital,comision)

# Restar comisiones 
cash_act = cash - port_opt['Comisiones'].sum()

## Seleccionar up y down
down,up = mi_inv_activa.updown(pos_prices)

new_portfolio= mi_inv_activa.obtener_nuevo_port(j_prices,port_opt,down,comision)

valor_portafolio, acciones_compra, acciones_venta, comisiones_compra,comisiones_venta = mi_inv_activa.valor_portafolio(cash,pos_prices_f,j_prices,port_opt,down,up,comision)

df_activa= mi_inv_activa.df_activa(valor_portafolio, lista)

df_operaciones= mi_inv_activa.df_operaciones(lista2,acciones_compra,acciones_venta,comisiones_compra,comisiones_venta)


########### Medidas de desempeño
mis_medidas_desempeno= mi_inv_activa.medidas_desempeno(pospa,df_activa,periodicidad)
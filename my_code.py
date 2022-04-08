from select import select
from unicodedata import category
import pandas as pd
import mysql
import mysql.connector
from sqlalchemy import create_engine



#EXPLORO LA DATA de actor
df_sucio_actor = pd.read_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project-NOT-Mine\data\actor.csv", sep= ',')
#print(df_sucio_actor.describe())
df_sucio_actor= df_sucio_actor.fillna(0)# POR SI HAY UN VALOR NULO
df_sucio_actor['last_update']=pd.to_datetime(df_sucio_actor['last_update'])#CAMBIO A DATE TYPE 
#YA LIMPIO ESTE CSV LO PUEDO EXPORTAR
df_sucio_actor.to_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project\CSV limpios\actor_cleaned_df.csv")
df_sucio_actor=df_sucio_actor.dropna().reset_index(drop=True)
#EXPLORO LA DATA DE CATEGORY y limpio
df_sucio_category = pd.read_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project-NOT-Mine\data\category.csv")

#print(df_sucio_category.describe())
#print(df_sucio_category.dtypes)
df_sucio_category['name']= df_sucio_category['name'].convert_dtypes(infer_objects=True, convert_string=True)
df_sucio_category['last_update']=pd.to_datetime(df_sucio_category['last_update'])
df_sucio_category['name']= df_sucio_category['name'].str.strip()

#print(df_sucio_category.dtypes)
df_sucio_category.to_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project\CSV limpios\category_cleaned_df.csv")
df_sucio_category=df_sucio_category.dropna().reset_index(drop=True)
#EXPLORO LA DATA DE FILM Y LIMPIO
df_sucio_film = pd.read_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project-NOT-Mine\data\film.csv")
df_sucio_film.drop('original_language_id', axis=1, inplace=True)
df_sucio_film= df_sucio_film.fillna(0)
df_sucio_film.to_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project\CSV limpios\film_cleaned_df.csv")
df_sucio_film=df_sucio_film.dropna().reset_index(drop=True)

#EXPLORO LA DTA DE INVENTORY Y LIMPIO (pudiera quitar update YA QUE TODo SON IGUALES Y NO NOS APORTA NADA)
df_sucio_inventory = pd.read_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project-NOT-Mine\data\inventory.csv")
df_sucio_inventory.drop('last_update', axis=1, inplace=True)
df_sucio_inventory.to_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project\CSV limpios\inventory_cleaned_df.csv")
df_sucio_inventory=df_sucio_inventory.dropna().reset_index(drop=True)
#EXPLORO LA DATA DE LANGUAGE Y LIMPIO(igual que antes quito last update)
df_sucio_language = pd.read_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project-NOT-Mine\data\language.csv")
df_sucio_language.drop('last_update', axis=1, inplace=True)
df_sucio_language.to_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project\CSV limpios\inventory_cleaned_df.csv")
df_sucio_language=df_sucio_language.dropna().reset_index(drop=True)
#EXPLORO LA DATA DE RENTAL
df_sucio_rental = pd.read_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project-NOT-Mine\data\rental.csv", index_col='Unnamed: 0')
df_sucio_rental.drop(['Unnamed: 0.1'], axis=1, inplace=True)

#print(df_sucio_rental.columns)
#df_sucio_rental.to_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project-NOT-Mine\data\rental.csv")



#NOS ENVIARON UN NUEVO CSV LO IMPORTAMOS DE GIT COMO CSV, EXPLORAMOS Y AGREGAMOS AL DATABASE
df_old_HDD = pd.read_csv(r"C:\Users\penzi\projectos\projecto semana 3\w4-database-project-NOT-Mine\data\old_HDD.csv")


#creo una coneccion a mi servidor y la base de datos
db=mysql.connector.connect(host="localhost", user="root", password="password", database='proyect_semana_3')
cursor = db.cursor()

#cursor.execute("CREATE DATABASE proyect_semana_3") ACA CREO LA BASE DE DATOS
str_conn='mysql+pymysql://root:password@localhost:3306/proyect_semana_3'
motor=create_engine(str_conn)


#este bloque es para crear las tablas y asignar primary key
"""
df_sucio_films.to_sql(name='films', con=motor, if_exists='append', index=False)
df_sucio_rental.to_sql(name='rental', con=motor, if_exists='append', index=False)
df_sucio_actor.to_sql(name='actors', con=motor, if_exists='append', index=False)
df_sucio_category.to_sql(name='category', con=motor, if_exists='append', index=False)
df_sucio_inventory.to_sql(name='inventory', con=motor, if_exists='append', index=False)
df_sucio_language.to_sql(name='language', con=motor, if_exists='append', index=False)
df_old_HDD.to_sql(name='old_hdd', con=motor, if_exists='append', index=False)



#motor.execute("ALTER TABLE films ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY")
#motor.execute("ALTER TABLE rental ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY")
#motor.execute("ALTER TABLE actors ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY")
#motor.execute("ALTER TABLE category ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY")
#motor.execute("ALTER TABLE inventory ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY")
#motor.execute("ALTER TABLE language ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY")
#motor.execute("ALTER TABLE old_hdd ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY")

"""

#preguntas que hacer para los query
#cuales peliculas tienen mejores special features y es mas barato para alquilar para sacarle mas provecho
#que actores actuan mas en cada categoria
#que actor actua en peliculas donde el alquiler es el mas bajo
#que pelicula puedes alquiler por el maximo de tiempo
#que peliculas puedes alquilar en que idioma
#que pelicula se alquila mas
#que actor esta en la pelicula donde se alquila mas
#que pelicula le genera mas dinero a la tienda
#
#
#join hdd con category,   


cursor.execute('select * from rental;') #mysql method
#for x in cursor:
#    print(x)

#df_sql=pd.read_sql(x, motor)#sql alchemy donde por x pones la query que quieres

join_old_hdd_actors='''
select old_hdd.first_name , old_hdd.last_name, title, category_id, release_year, actor_id
from old_hdd
inner join actors
on actors.last_name = old_hdd.last_name and actors.first_name = old_hdd.first_name
group by old_hdd.first_name;
'''

df_old_actor= pd.read_sql(join_old_hdd_actors, motor)
#print(df_old_actor.head)

join_old_hdd_actors_category='''
select old_hdd.first_name , old_hdd.last_name, title, category.name
from old_hdd
inner join actors
on actors.last_name = old_hdd.last_name and actors.first_name = old_hdd.first_name
left join category
on category.category_id = old_hdd.category_id
group by category.name, old_hdd.first_name
order by old_hdd.title;
'''
df_old_actor_category= pd.read_sql(join_old_hdd_actors_category, motor)
print('las peliculas con cada actor y categoria de la pelicula ')
print(df_old_actor_category)













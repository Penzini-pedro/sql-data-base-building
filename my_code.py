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
#print(df_old_actor_category.head())

join_films_language= '''
select language.language_id, language.name, films.title, films.rental_rate, films.rental_duration
from language
inner join films
on language.language_id = films.language_id
group by title
order by films.rental_duration desc;
'''
df_films_language=  pd.read_sql(join_films_language, motor)
#print(df_films_language.head())

join_films_invenotry= '''
select store_id,count(inventory.store_id)
from films
join inventory
on films.film_id = inventory.film_id
join rental 
on rental.inventory_id= inventory.inventory_id
group by store_id;
'''
df_films_rental= pd.read_sql(join_films_invenotry,motor)
#print(df_films_rental.head())

join_films_inventroy_rental='''
SELECT rating, count(title)
FROM (select films.replacement_cost,rental.customer_id,films.title, films.rating  
from films
join inventory
on films.film_id = inventory.film_id
join rental 
on rental.inventory_id= inventory.inventory_id) as d
group by rating
having count(title)> 40
order by rating desc;
'''

df_films_rental_inventory=pd.read_sql(join_films_inventroy_rental, motor)
#print(df_films_rental_inventory)

cre_tbl='''
create temporary table new_tbl
select films.*,inventory.inventory_id,inventory.store_id, rental.staff_id, rental.customer_id, rental.rental_id
from films
join inventory
on films.film_id = inventory.film_id
join rental 
on rental.inventory_id= inventory.inventory_id;
'''

#cre_tbl2= pd.read_sql(cre_tbl, motor)
#print(cre_tbl2.head)

call_temp='''
select *
from new_tbl;
'''
#call_temp2= pd.read_sql(call_temp,motor)
#print(call_temp2) aunque la tabla se crea en mysql all salir del parametro se cae y no es llamable ojo que si hay metodos para llamrlo


mega_join='''
select category.name, films.title, films.rating, inventory.store_id, rental.return_date
from films
join old_hdd
on old_hdd.title= films.title
join category
on category.category_id= old_hdd.category_id
join inventory
on films.film_id= inventory_id
join rental
on inventory.inventory_id= rental.inventory_id 
group by title
order by name;'''

mega_join2= pd.read_sql(mega_join, motor)
print(mega_join2.head())

price_per_day='''
select films.title, films.rental_rate, films.rental_duration, rental.inventory_id, sum(films.rental_rate/films.rental_duration)as price_per_day
from films
join inventory
on films.film_id= inventory.film_id
join rental
on rental.inventory_id= inventory.inventory_id
group by inventory.inventory_id
order by price_per_day desc;'''

df_price_per_day= pd.read_sql(price_per_day, motor)
print(df_price_per_day.head())


all_join='''
select actors.first_name, actors.last_name, inventory.store_id, films.title, films.release_year 
from films
join old_hdd
on old_hdd.title= films.title
join category
on category.category_id= old_hdd.category_id
join inventory
on films.film_id= inventory_id
join rental
on inventory.inventory_id= rental.inventory_id 
join actors
on actors.last_name = old_hdd.last_name and actors.first_name = old_hdd.first_name
group by title
order by name;'''

df_all= pd.read_sql(all_join, motor)
print(df_all.head())








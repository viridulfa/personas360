create table if not exists sis_db_data_raw.rc_defunciones (
PK_DEFUNCIONES STRING,
FK_FUENTE_INFORMACION STRING,
ANIO STRING,
JUZGADO STRING,
NU_ACTA STRING,
LIBRO STRING,
INGRESO STRING,
FECHA_REGISTRO STRING,
CLASE STRING,
FK_CLAVE_ESTADO STRING,
FK_CLAVE_DELEGACION STRING,
DIFUNTO_NOMBRE STRING,
DIFUNTO_AP_PATERNO STRING,
DIFUNTO_AP_MATERNO STRING,
FECHA_DEFUNCION STRING,
HORA_DEFUNCION STRING,
DIFUNTO_ESTADO STRING,
DIFUNTO_DOMICILIO STRING,
DIFUNTO_ESCOLARIDAD STRING,
DIFUNTO_ESTADO_CIVIL STRING,
DIFUNTO_EDAD STRING,
DIFUNTO_ESTADO_DE_NACIMIENTO STRING,
DIFUNTO_NACIONALIDAD STRING,
DIFUNTO_OCUPACION STRING,
DIFUNTO_SEXO STRING,
DIFUNTO_TRABAJABA STRING,
DIFUNTO_TRABAJO STRING,
PAREJA_DIFUNTO STRING,
CAUSA_MUERTE STRING,
PANTEON_NUMERO_ORDEN STRING,
DESTINO_DEL_CUERPO STRING,
DONDE_FALLECIO STRING,
ANOTACION STRING,
PANTEON STRING,
PANTEON_DOMICILIO STRING,
DEFUNCION_LUGAR STRING,
DOCTOR_NOMBRE STRING,
DOCTOR_AP_PATERNO STRING,
DOCTOR_AP_MATERNO STRING,
DOCTOR_CEDULA_PROFESIONAL STRING,
DOCTOR_ESTADO STRING,
DOCTOR_DELEGACION STRING,
DOCTOR_CALLE STRING,
DOCTOR_COLONIA STRING,
CERTIFICADO_MEDICO STRING,
ASISTENCIA_MEDICA STRING,
DECLARANTE_NOMBRE STRING,
DECLARANTE_AP_PATERNO STRING,
DECLARANTE_AP_MATERNO STRING,
DECLARANTE_PARENTESCO STRING,
DECLARANTE_EDAD STRING,
DECLARANTE_ESTADO STRING,
DECLARANTE_DELEGACION STRING,
DECLARANTE_COLONIA STRING,
DECLARANTE_CALLE STRING,
DECLARANTE_NACIONALIDAD STRING,
PADRE_DIFUNTO_NOMBRE STRING,
PADRE_DIFUNTO_AP_MATERNO STRING,
PADRE_DIFUNTO_AP_PATERNO STRING,
MADRE_DIFUNTO_NOMBRE STRING,
MADRE_DIFUNTO_AP_PATERNO STRING,
MADRE_DIFUNTO_AP_MATERNO STRING,
TESTIGO1_NOMBRE STRING,
TESTIGO1_AP_PATERNO STRING,
TESTIGO1_AP_MATERNO STRING,
TESTIGO1_ESTADO STRING,
TESTIGO1_DELEGACION STRING,
TESTIGO1_COLONIA STRING,
TESTIGO1_CALLE STRING,
TESTIGO1_NACIONALIDAD STRING,
TESTIGO1_PARENTESCO STRING,
TESTIGO1_EDAD STRING,
TESTIGO1_OCUPACION STRING,
TESTIGO2_NOMBRE STRING,
TESTIGO2_AP_PATERNO STRING,
TESTIGO2_AP_MATERNO STRING,
TESTIGO2_ESTADO STRING,
TESTIGO2_DELEGACION STRING,
TESTIGO2_COLONIA STRING,
TESTIGO2_CALLE STRING,
TESTIGO2_NACIONALIDAD STRING,
TESTIGO2_PARENTESCO STRING,
TESTIGO2_EDAD STRING,
TESTIGO2_OCUPACION STRING,
TIPO_DE_JUEZ_INSEXT STRING,
NOMBRE_JUEZ STRING,
ID_REGISTRADOR STRING,
NOMBRE_DE_IMAGEN STRING,
DOC_ADJUNTO_DE_IMAGEN STRING,
BORRADO_LOGICO STRING,
CON_SIN_TESTIGOS STRING,
FORMATO_VARIOS STRING,
CAPTURISTA_MECA STRING,
ID_CAPTURA STRING,
LOTE_CAPTURA STRING,
ERROR_ORIGEN STRING,
ART_6BIS STRING,
TIPO_ACTO STRING,
DIFUNTO_NACIONALIDAD_RENAPO STRING,
REGISTRO_MUNICIPIO_RENAPO STRING,
REGISTRO_ENTIDAD_RENAPO STRING,
ANOTACION_RENAPO STRING,
FECHA_NACIMIENTO STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
'field.delim'='|',
'serialization.encoding'='ISO-8859-1')
STORED AS TEXTFILE LOCATION '/data/siadipsbx/raw/rc/rw_defunciones/data'
TBLPROPERTIES ("skip.header.line.count"="1");

drop table sis_db_data_raw.rc_defunciones purge

--LUEGO
--hadoop fs -copyFromLocal /home/siadip/csv/defunciones/defunciones_arregladas/* /data/siadipsbx/raw/rc/rw_defunciones/data/
---LUEGO
--hdfs dfs -chmod 777 /data/siadipsbx/raw/rc/rw_defunciones/data/*

select count(*) from sis_db_data_raw.rc_defunciones

select * from sis_db_data_raw.rc_defunciones




invalidate metadata

select anio from sis_db_data_raw.rc_defunciones
group by anio order by anio 

select id_captura from sis_db_data_raw.rc_defunciones
group by id_captura order by id_captura

select id_captura from sis_db_data_raw.rc_defunciones
group by id_captura order by id_captura

select id_captura from sis_db_data_raw.rc_defunciones
where id_captura rlike '^[0-9]+$'
group by id_captura order by id_captura

select anio from sis_db_data_raw.rc_defunciones
where anio not rlike '^[0-9]+$'
group by anio order by anio


select doctor_colonia from sis_db_data_raw.rc_defunciones where PK_DEFUNCIONES ='200333055'
select substr(fecha_nacimiento,1,4) as fecha_nacimiento from sis_db_data_raw.rc_defunciones group by fecha_nacimiento order by fecha_nacimiento
select * from  sis_db_data_raw.rc_defunciones where fecha_nacimiento like '%SE LE%'
select * from  sis_db_data_raw.rc_defunciones where pk_defunciones='200333055'


select count(distinct(pk_defunciones)) from sis_db_data_raw.rc_defunciones

select pk_defunciones, anio, ingreso, fecha_registro,fecha_nacimiento, difunto_nombre, difunto_ap_paterno, difunto_ap_materno,
difunto_sexo
from sis_db_data_raw.rc_defunciones 

select count(*)
from sis_db_data_raw.rc_defunciones
where difunto_nombre ='' and difunto_ap_paterno='' and difunto_ap_materno=''

select fecha_registro
from sis_db_data_raw.rc_defunciones
group by fecha_registro order by fecha_registro


select pk_defunciones,anio,ingreso,fecha_registro,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,
fecha_defuncion,hora_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,
registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
from sis_db_data_raw.rc_defunciones

------ TABLA 1 ---------
select * from sis_db_data_raw.rc_defunciones

select count(*) from sis_db_data_raw.rc_defunciones
where difunto_nombre='' and difunto_ap_paterno='' and difunto_ap_materno='' and anotacion_renapo='' --4,337,412

--negacion
select count(*) from sis_db_data_raw.rc_defunciones
where difunto_nombre!='' or difunto_ap_paterno!='' or difunto_ap_materno!='' or anotacion_renapo!='' --6,014,056

create table sis_db_data_cln.rc_stg_def_1 as select * from sis_db_data_raw.rc_defunciones
where difunto_nombre!='' or difunto_ap_paterno!='' or difunto_ap_materno!='' or anotacion_renapo!='' 

INSERT INTO sis_db_data_cln.rc_stg_def_1 select * from sis_db_data_raw.rc_defunciones
where difunto_nombre!='' or difunto_ap_paterno!='' or difunto_ap_materno!='' or anotacion_renapo!='' 

invalidate metadata

------ TABLA 2 ---------
select * from sis_db_data_cln.rc_stg_def_1
select count(*) from sis_db_data_cln.rc_stg_def_1

SELECT COUNT(*) FROM sis_db_data_cln.rc_stg_def_1
WHERE UPPER(DIFUNTO_NOMBRE) LIKE 'QUE %'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%DESCO%' 
OR UPPER(DIFUNTO_NOMBRE) LIKE '%FETO%' 
OR UPPER(DIFUNTO_NOMBRE) LIKE '%INDIVIDUO%' 
OR UPPER(DIFUNTO_NOMBRE) LIKE '%MUJER%' 
OR UPPER(DIFUNTO_NOMBRE) LIKE '%MENOR%' 
OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑA%NOMBRE%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑA%S%N%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑO%NOMBRE%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑO%S%N%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%HOMBRE%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%MASCULINO%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%FEMENINO%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%ABORTO%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%RECIEN NACIDO%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%RECIEN NACIDA%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%NACIDO%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%NACIDA%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%S-N%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%RECIEN%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%IGNORA%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%ILEGIBLE%'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%NOMBRE%' --556,233
OR UPPER(DIFUNTO_NOMBRE) LIKE '% O %'
OR UPPER(DIFUNTO_NOMBRE) LIKE '%INDETERMINABLE%'



SELECT COUNT(*) FROM (
    SELECT DIFUNTO_NOMBRE FROM sis_db_data_cln.rc_stg_def_1
        WHERE UPPER(DIFUNTO_NOMBRE) LIKE 'QUE %'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%DESCO%' 
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%FETO%' 
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%INDIVIDUO%' 
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%MUJER%' 
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%MENOR%' 
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑA%' 
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑO%' 
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑA%S%N%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NIÑO%S%N%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%HOMBRE%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%MASCULINO%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%FEMENINO%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%ABORTO%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%RECIEN NACIDO%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%RECIEN NACIDA%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NACIDO%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NACIDA%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%S-N%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%RECIEN%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%IGNORA%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%ILEGIBLE%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%NOMBRE%' --556,233
        OR UPPER(DIFUNTO_NOMBRE) LIKE '% O %'
        OR UPPER(DIFUNTO_NOMBRE) LIKE '%INDETERMINABLE%'
        OR UPPER(DIFUNTO_NOMBRE) LIKE 'DEL %'
        --GROUP BY DIFUNTO_NOMBRE ORDER BY LENGTH(UPPER(DIFUNTO_NOMBRE)) DESC
) T --557,716
 

SELECT PK_DEFUNCIONES FROM sis_db_data_cln.rc_stg_def_1 WHERE DIFUNTO_NOMBRE LIKE '%SIN NOMBRE%GONZALEZ%GONZALEZ%'
SELECT * FROM sis_db_data_cln.rc_stg_def_1 d WHERE PK_DEFUNCIONES ='5238182'

select difunto_nombre from (
    select pk_defunciones,anio, ingreso, fecha_registro,
    upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    difunto_nombre, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź')
    , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')) as difunto_nombre,
    upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    difunto_ap_paterno, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź')
    , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')) as difunto_ap_paterno,
    upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    difunto_ap_materno, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź')
    , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')) as difunto_ap_materno,
    fecha_defuncion,hora_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
    from sis_db_data_cln.rc_stg_def_1
) t 
--where difunto_nombre not rlike '[A-Z ]+'
where difunto_nombre like '%NIÑA%' Or difunto_nombre like '%NIÑO%' or difunto_nombre like '%MUJER%' or difunto_nombre like '%HOMBRE%' or difunto_nombre like '%FETO%' OR difunto_nombre like '%DESCONOCIDO%' or difunto_nombre like '%MUERTO%' or difunto_nombre like '%MUERTA%'
group by difunto_nombre order by difunto_nombre 




select substr(fecha_nacimiento,1,4) fecha_nacimiento, count(*) from(

    select pk_defunciones,anio, ingreso, fecha_registro,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_nombre,'NIÑO SIN NOMBRE','DESCONOCIDO'),'FETO FEMENINO','DESCONOCIDO'),'ABORTO MASCULINO','DESCONOCIDO'),'ABORTO FEMENINO','DESCONOCIDO'),'SIN NOMBRE','DESCONOCIDO')
        ,'NIÑA','DESCONOCIDO'),'NIÑO','DESCONOCIDO'),'MUJER','DESCONOCIDO'),'HOMBRE','DESCONOCIDO'),'MUERTO','DESCONOCIDO'),'MUERTA','DESCONOCIDO'),'FETO','DESCONOCIDO'),' O DESCONOCIDO','')
        ,' O DESCONOCIDA',''),'DESCONOCIDA','DESCONOCIDO')
        , "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", '')) as difunto_nombre,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_ap_paterno,'NIÑO SIN NOMBRE',''),'FETO FEMENINO',''),'ABORTO MASCULINO',''),'ABORTO FEMENINO',''),'SIN NOMBRE',''),'NIÑA',''),'NIÑO',''),'MUJER',''),'HOMBRE',''),'MUERTO',''),'MUERTA',''),'FETO',''),' O DESCONOCIDO',''),' O DESCONOCIDA',''),'DESCONOCIDO',''),'DESCONOCIDA',''), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", '')
        )) as difunto_ap_paterno,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_ap_materno,'NIÑO SIN NOMBRE',''),'FETO FEMENINO',''),'ABORTO MASCULINO',''),'ABORTO FEMENINO',''),'SIN NOMBRE',''),'NIÑA',''),'NIÑO',''),'MUJER',''),'HOMBRE',''),'MUERTO',''),'MUERTA',''),'FETO',''),' O DESCONOCIDO',''),' O DESCONOCIDA',''),'DESCONOCIDO',''),'DESCONOCIDA',''), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", '')
        )) as difunto_ap_materno,
        fecha_defuncion,
        --regexp_replace(hora_defuncion,'[^0-9: ]+','') as hora_defuncion,
        difunto_edad,
        CASE 
            WHEN difunto_sexo='1.0' THEN 'H'
            WHEN difunto_sexo='2.0' THEN 'M'
            ELSE ''
        END as difunto_sexo,
        regexp_replace(difunto_nacionalidad_renapo,'\.0','') as difunto_nacionalidad_renapo,
        regexp_replace(registro_municipio_renapo,'\.0','') as registro_municipio_renapo,
        regexp_replace(registro_entidad_renapo,'\.0','') as registro_entidad_renapo,
        trim(UPPER(anotacion_renapo)) as anotacion_renapo,fecha_nacimiento
        FROM(
            select * from (
                select pk_defunciones,anio, ingreso, fecha_registro,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_nombre, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź')
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_nombre,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_ap_paterno, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź')
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_ap_paterno,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_ap_materno, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź')
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_ap_materno,
                fecha_defuncion,hora_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
                from sis_db_data_cln.rc_stg_def_1
            ) t 
        ) t2

) t3
group by fecha_nacimiento order by fecha_nacimiento 



/*create table sis_db_data_cln.rc_stg_def_2 as     select pk_defunciones,anio, ingreso, fecha_registro,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_nombre,'QUE TUVO LUGAR UN ABORTO DE SEXO MASCULINO','DESCONOCIDO'),'QUE NACIO MUERTO EL FETO DE SEXO INDEFINIDO','DESCONOCIDO'),'QUE NACIO MUERTO UN FETO MASCULINO','DESCONOCIDO'),'QUE NACIO MUERTO UN IDIVIDUO','DESCONOCIDO'),'QUE NACIO MUERTO UN FETO FEMENINO','DESCONOCIDO'),'QUE NACIO MUERTO FETO MASCULINO','DESCONOCIDO'),'QUE NACIO MUERTO EL FETO MASCULINO','DESCONOCIDO'),'QUE NACIO MUERTO EL FETO MACULINO','DESCONOCIDO'),'QUE NACIO MUERTO EL FETO INDEFINIDO','DESCONOCIDO'),'QUE NACIO MUERTO EL FETO IN DEFINIDO','DESCONOCIDO'),'QUE NACIO MUERTO EL FETO FEMENINO','DESCONOCIDO'),'QUE NACIO MUERTO EL FETO','DESCONOCIDO'),'QUE NACIO MUERTO','DESCONOCIDO'),'QUE MUERTO EL FETO','DESCONOCIDO'),'NIÑO SIN NOMBRE','DESCONOCIDO'),'FETO FEMENINO','DESCONOCIDO'),'ABORTO MASCULINO','DESCONOCIDO'),'ABORTO FEMENINO','DESCONOCIDO'),'SIN NOMBRE','DESCONOCIDO'),'NIÑA','DESCONOCIDO'),'NIÑO','DESCONOCIDO'),'MUJER','DESCONOCIDO'),'HOMBRE','DESCONOCIDO'),
        'MUERTO','DESCONOCIDO'),'MUERTA','DESCONOCIDO'),'FETO','DESCONOCIDO'),' O DESCONOCIDO',''),' O DESCONOCIDA',''),'DESCONOCIDA','DESCONOCIDO'),"[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''),'DESCONCOCIDO', 'DESCONOCIDO'),'DESCONICIDO', 'DESCONOCIDO'),'FEMENINO', 'DESCONOCIDO'),'MASCULINO', 'DESCONOCIDO'),
        'RECIEN NACIDO', 'DESCONOCIDO'),' O ', '')
        )) as difunto_nombre,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_ap_paterno,'NIÑO SIN NOMBRE','DESCONOCIDO'),'FETO FEMENINO','DESCONOCIDO'),'ABORTO MASCULINO','DESCONOCIDO'),'ABORTO FEMENINO','DESCONOCIDO'),'SIN NOMBRE','DESCONOCIDO'),'NIÑA','DESCONOCIDO'),'NIÑO','DESCONOCIDO'),'MUJER','DESCONOCIDO'),'HOMBRE','DESCONOCIDO'),'MUERTO','DESCONOCIDO'),'MUERTA','DESCONOCIDO'),'FETO','DESCONOCIDO'),' O DESCONOCIDO',''),' O DESCONOCIDA',''),
        'DESCONOCIDA','DESCONOCIDO'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''), 'DESCONCOCIDO', 'DESCONOCIDO'), 'DESCONICIDO', 'DESCONOCIDO'), 'FEMENINO', 'DESCONOCIDO'), 'MASCULINO', 'DESCONOCIDO'), 'RECIEN NACIDO', 'DESCONOCIDO'), ' O ', '')
        )) as difunto_ap_paterno,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_ap_materno,'NIÑO SIN NOMBRE','DESCONOCIDO'),'FETO FEMENINO','DESCONOCIDO'),'ABORTO MASCULINO','DESCONOCIDO'),'ABORTO FEMENINO','DESCONOCIDO'),'SIN NOMBRE','DESCONOCIDO'),'NIÑA','DESCONOCIDO'),'NIÑO','DESCONOCIDO'),'MUJER','DESCONOCIDO'),'HOMBRE','DESCONOCIDO'),'MUERTO','DESCONOCIDO'),'MUERTA','DESCONOCIDO'),'FETO','DESCONOCIDO'),' O DESCONOCIDO',''),' O DESCONOCIDA',''),
        'DESCONOCIDA','DESCONOCIDO'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''), 'DESCONCOCIDO', 'DESCONOCIDO'), 'DESCONICIDO', 'DESCONOCIDO'), 'FEMENINO', 'DESCONOCIDO'), 'MASCULINO', 'DESCONOCIDO'), 'RECIEN NACIDO', 'DESCONOCIDO'), ' O ', '')
        )) as difunto_ap_materno,
        fecha_defuncion,
        --regexp_replace(hora_defuncion,'[^0-9: ]+','') as hora_defuncion,
        difunto_edad,
        CASE 
            WHEN difunto_sexo='1.0' THEN 'H'
            WHEN difunto_sexo='2.0' THEN 'M'
            ELSE ''
        END as difunto_sexo,
        regexp_replace(difunto_nacionalidad_renapo,'\.0','') as difunto_nacionalidad_renapo,
        regexp_replace(registro_municipio_renapo,'\.0','') as registro_municipio_renapo,
        regexp_replace(registro_entidad_renapo,'\.0','') as registro_entidad_renapo,
        trim(UPPER(anotacion_renapo)) as anotacion_renapo,fecha_nacimiento
        FROM(
            select * from (
                select pk_defunciones,anio, ingreso, fecha_registro,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_nombre, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_nombre,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_ap_paterno, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_ap_paterno,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_ap_materno, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_ap_materno,
                fecha_defuncion,hora_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
                from sis_db_data_cln.rc_stg_def_1
            ) t 
        ) t2*/




/*INSERT INTO sis_db_data_cln.rc_stg_def_2   select pk_defunciones,anio, ingreso, fecha_registro,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_nombre,'NIÑO SIN NOMBRE',''),'FETO FEMENINO',''),'ABORTO MASCULINO',''),'ABORTO FEMENINO',''),'SIN NOMBRE',''),'NIÑA',''),'NIÑO',''),'MUJER',''),'HOMBRE',''),'MUERTO',''),'MUERTA',''),'FETO',''),' O DESCONOCIDO',''),' O DESCONOCIDA',''),'DESCONOCIDO',''),'DESCONOCIDA',''), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''), 'DESCONCOCIDO', ''), 'DESCONICIDO', ''), 'FEMENINO', ''), 'MASCULINO', ''), 'RECIEN NACIDO', ''), ' O ', '')
        )) as difunto_nombre,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_ap_paterno,'NIÑO SIN NOMBRE',''),'FETO FEMENINO',''),'ABORTO MASCULINO',''),'ABORTO FEMENINO',''),'SIN NOMBRE',''),'NIÑA',''),'NIÑO',''),'MUJER',''),'HOMBRE',''),'MUERTO',''),'MUERTA',''),'FETO',''),' O DESCONOCIDO',''),' O DESCONOCIDA',''),'DESCONOCIDO',''),'DESCONOCIDA',''), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''), 'DESCONCOCIDO', ''), 'DESCONICIDO', ''), 'FEMENINO', ''), 'MASCULINO', ''), 'RECIEN NACIDO', ''), ' O ', '')
        )) as difunto_ap_paterno,
        trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        difunto_ap_materno,'NIÑO SIN NOMBRE',''),'FETO FEMENINO',''),'ABORTO MASCULINO',''),'ABORTO FEMENINO',''),'SIN NOMBRE',''),'NIÑA',''),'NIÑO',''),'MUJER',''),'HOMBRE',''),'MUERTO',''),'MUERTA',''),'FETO',''),' O DESCONOCIDO',''),' O DESCONOCIDA',''),'DESCONOCIDO',''),'DESCONOCIDA',''), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''), 'DESCONCOCIDO', ''), 'DESCONICIDO', ''), 'FEMENINO', ''), 'MASCULINO', ''), 'RECIEN NACIDO', ''), ' O ', '')
        )) as difunto_ap_materno,
        fecha_defuncion,
        --regexp_replace(hora_defuncion,'[^0-9: ]+','') as hora_defuncion,
        difunto_edad,
        CASE 
            WHEN difunto_sexo='1.0' THEN 'H'
            WHEN difunto_sexo='2.0' THEN 'M'
            ELSE ''
        END as difunto_sexo,
        regexp_replace(difunto_nacionalidad_renapo,'\.0','') as difunto_nacionalidad_renapo,
        regexp_replace(registro_municipio_renapo,'\.0','') as registro_municipio_renapo,
        regexp_replace(registro_entidad_renapo,'\.0','') as registro_entidad_renapo,
        trim(UPPER(anotacion_renapo)) as anotacion_renapo,fecha_nacimiento
        FROM(
            select * from (
                select pk_defunciones,anio, ingreso, fecha_registro,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_nombre, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_nombre,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_ap_paterno, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_ap_paterno,
                trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                difunto_ap_materno, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'))) as difunto_ap_materno,
                fecha_defuncion,hora_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
                from sis_db_data_cln.rc_stg_def_1
            ) t 
        ) t2*/



---- a parir de aqui se limpia

SELECT difunto_nombre from (    
    SELECT pk_defunciones,anio, ingreso, fecha_registro,
    trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    CASE 
        WHEN UPPER(difunto_nombre) LIKE 'QUE %'
        OR UPPER(difunto_nombre) LIKE '%DESCO%' 
        OR UPPER(difunto_nombre) LIKE '%FETO%' 
        OR UPPER(difunto_nombre) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_nombre) LIKE '%MUJER%' 
        OR UPPER(difunto_nombre) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_nombre) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_nombre) LIKE '%HOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%MASCULINO%'
        OR UPPER(difunto_nombre) LIKE '%FEMENINO%'
        OR UPPER(difunto_nombre) LIKE '%ABORTO%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_nombre) LIKE '%NACIDO%'
        OR UPPER(difunto_nombre) LIKE '%NACIDA%'
        OR UPPER(difunto_nombre) LIKE '%S-N%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN%'
        OR UPPER(difunto_nombre) LIKE '%IGNORA%'
        OR UPPER(difunto_nombre) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_nombre) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_nombre) LIKE '% O %'
        OR UPPER(difunto_nombre) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_nombre) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_nombre END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), 'Ñ', 'N'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_nombre,
trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(                 
    CASE 
        WHEN UPPER(difunto_ap_paterno) LIKE 'QUE %'
        OR UPPER(difunto_ap_paterno) LIKE '%DESCO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%FETO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%MUJER%' 
        OR UPPER(difunto_ap_paterno) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_ap_paterno) LIKE '%HOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%MASCULINO%'
        OR UPPER(difunto_ap_paterno) LIKE '%FEMENINO%'
        OR UPPER(difunto_ap_paterno) LIKE '%ABORTO%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_ap_paterno) LIKE '%NACIDO%'
        OR UPPER(difunto_ap_paterno) LIKE '%NACIDA%'
        OR UPPER(difunto_ap_paterno) LIKE '%S-N%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN%'
        OR UPPER(difunto_ap_paterno) LIKE '%IGNORA%'
        OR UPPER(difunto_ap_paterno) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_ap_paterno) LIKE '% O %'
        OR UPPER(difunto_ap_paterno) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_ap_paterno) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_ap_paterno END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), 'Ñ', 'N'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_ap_paterno,
trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(                
    CASE 
        WHEN UPPER(difunto_ap_materno) LIKE 'QUE %'
        OR UPPER(difunto_ap_materno) LIKE '%DESCO%' 
        OR UPPER(difunto_ap_materno) LIKE '%FETO%' 
        OR UPPER(difunto_ap_materno) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_ap_materno) LIKE '%MUJER%' 
        OR UPPER(difunto_ap_materno) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_ap_materno) LIKE '%HOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%MASCULINO%'
        OR UPPER(difunto_ap_materno) LIKE '%FEMENINO%'
        OR UPPER(difunto_ap_materno) LIKE '%ABORTO%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_ap_materno) LIKE '%NACIDO%'
        OR UPPER(difunto_ap_materno) LIKE '%NACIDA%'
        OR UPPER(difunto_ap_materno) LIKE '%S-N%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN%'
        OR UPPER(difunto_ap_materno) LIKE '%IGNORA%'
        OR UPPER(difunto_ap_materno) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_ap_materno) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_ap_materno) LIKE '% O %'
        OR UPPER(difunto_ap_materno) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_ap_materno) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_ap_materno END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), 'Ñ', 'N'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_ap_materno,
    fecha_defuncion,difunto_edad,
    CASE 
        WHEN difunto_sexo='1.0' THEN 'H'
        WHEN difunto_sexo='2.0' THEN 'M'
        ELSE ''
    END as difunto_sexo,
    regexp_replace(difunto_nacionalidad_renapo,'\.0','') as difunto_nacionalidad_renapo,
    regexp_replace(registro_municipio_renapo,'\.0','') as registro_municipio_renapo,
    regexp_replace(registro_entidad_renapo,'\.0','') as registro_entidad_renapo,
    trim(UPPER(anotacion_renapo)) as anotacion_renapo,fecha_nacimiento        
    FROM sis_db_data_cln.rc_stg_def_1
    ORDER BY pk_defunciones,anio, ingreso, fecha_registro,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
) t group by difunto_nombre order by difunto_nombre



create table if not exists sis_db_data_cln.rc_stg_def_2 as
SELECT pk_defunciones,anio, ingreso, fecha_registro,
    trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    CASE 
        WHEN UPPER(difunto_nombre) LIKE 'QUE %'
        OR UPPER(difunto_nombre) LIKE '%DESCO%' 
        OR UPPER(difunto_nombre) LIKE '%FETO%' 
        OR UPPER(difunto_nombre) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_nombre) LIKE '%MUJER%' 
        OR UPPER(difunto_nombre) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_nombre) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_nombre) LIKE '%HOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%MASCULINO%'
        OR UPPER(difunto_nombre) LIKE '%FEMENINO%'
        OR UPPER(difunto_nombre) LIKE '%ABORTO%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_nombre) LIKE '%NACIDO%'
        OR UPPER(difunto_nombre) LIKE '%NACIDA%'
        OR UPPER(difunto_nombre) LIKE '%S-N%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN%'
        OR UPPER(difunto_nombre) LIKE '%IGNORA%'
        OR UPPER(difunto_nombre) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_nombre) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_nombre) LIKE '% O %'
        OR UPPER(difunto_nombre) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_nombre) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_nombre END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_nombre,
trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(                 
    CASE 
        WHEN UPPER(difunto_ap_paterno) LIKE 'QUE %'
        OR UPPER(difunto_ap_paterno) LIKE '%DESCO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%FETO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%MUJER%' 
        OR UPPER(difunto_ap_paterno) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_ap_paterno) LIKE '%HOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%MASCULINO%'
        OR UPPER(difunto_ap_paterno) LIKE '%FEMENINO%'
        OR UPPER(difunto_ap_paterno) LIKE '%ABORTO%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_ap_paterno) LIKE '%NACIDO%'
        OR UPPER(difunto_ap_paterno) LIKE '%NACIDA%'
        OR UPPER(difunto_ap_paterno) LIKE '%S-N%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN%'
        OR UPPER(difunto_ap_paterno) LIKE '%IGNORA%'
        OR UPPER(difunto_ap_paterno) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_ap_paterno) LIKE '% O %'
        OR UPPER(difunto_ap_paterno) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_ap_paterno) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_ap_paterno END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_ap_paterno,
trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(                
    CASE 
        WHEN UPPER(difunto_ap_materno) LIKE 'QUE %'
        OR UPPER(difunto_ap_materno) LIKE '%DESCO%' 
        OR UPPER(difunto_ap_materno) LIKE '%FETO%' 
        OR UPPER(difunto_ap_materno) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_ap_materno) LIKE '%MUJER%' 
        OR UPPER(difunto_ap_materno) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_ap_materno) LIKE '%HOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%MASCULINO%'
        OR UPPER(difunto_ap_materno) LIKE '%FEMENINO%'
        OR UPPER(difunto_ap_materno) LIKE '%ABORTO%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_ap_materno) LIKE '%NACIDO%'
        OR UPPER(difunto_ap_materno) LIKE '%NACIDA%'
        OR UPPER(difunto_ap_materno) LIKE '%S-N%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN%'
        OR UPPER(difunto_ap_materno) LIKE '%IGNORA%'
        OR UPPER(difunto_ap_materno) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_ap_materno) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_ap_materno) LIKE '% O %'
        OR UPPER(difunto_ap_materno) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_ap_materno) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_ap_materno END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_ap_materno,
    fecha_defuncion,difunto_edad,
    CASE 
        WHEN difunto_sexo='1.0' THEN 'H'
        WHEN difunto_sexo='2.0' THEN 'M'
        ELSE ''
    END as difunto_sexo,
    regexp_replace(difunto_nacionalidad_renapo,'\.0','') as difunto_nacionalidad_renapo,
    regexp_replace(registro_municipio_renapo,'\.0','') as registro_municipio_renapo,
    regexp_replace(registro_entidad_renapo,'\.0','') as registro_entidad_renapo,
    trim(UPPER(anotacion_renapo)) as anotacion_renapo,fecha_nacimiento        
    FROM sis_db_data_cln.rc_stg_def_1
    ORDER BY pk_defunciones,anio, ingreso, fecha_registro,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento



INSERT INTO sis_db_data_cln.rc_stg_def_2 SELECT pk_defunciones,anio, ingreso, fecha_registro,
    trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    CASE 
        WHEN UPPER(difunto_nombre) LIKE 'QUE %'
        OR UPPER(difunto_nombre) LIKE '%DESCO%' 
        OR UPPER(difunto_nombre) LIKE '%FETO%' 
        OR UPPER(difunto_nombre) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_nombre) LIKE '%MUJER%' 
        OR UPPER(difunto_nombre) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_nombre) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_nombre) LIKE '%HOMBRE%'
        OR UPPER(difunto_nombre) LIKE '%MASCULINO%'
        OR UPPER(difunto_nombre) LIKE '%FEMENINO%'
        OR UPPER(difunto_nombre) LIKE '%ABORTO%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_nombre) LIKE '%NACIDO%'
        OR UPPER(difunto_nombre) LIKE '%NACIDA%'
        OR UPPER(difunto_nombre) LIKE '%S-N%'
        OR UPPER(difunto_nombre) LIKE '%RECIEN%'
        OR UPPER(difunto_nombre) LIKE '%IGNORA%'
        OR UPPER(difunto_nombre) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_nombre) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_nombre) LIKE '% O %'
        OR UPPER(difunto_nombre) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_nombre) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_nombre END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_nombre,
trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(                 
    CASE 
        WHEN UPPER(difunto_ap_paterno) LIKE 'QUE %'
        OR UPPER(difunto_ap_paterno) LIKE '%DESCO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%FETO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_ap_paterno) LIKE '%MUJER%' 
        OR UPPER(difunto_ap_paterno) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_ap_paterno) LIKE '%HOMBRE%'
        OR UPPER(difunto_ap_paterno) LIKE '%MASCULINO%'
        OR UPPER(difunto_ap_paterno) LIKE '%FEMENINO%'
        OR UPPER(difunto_ap_paterno) LIKE '%ABORTO%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_ap_paterno) LIKE '%NACIDO%'
        OR UPPER(difunto_ap_paterno) LIKE '%NACIDA%'
        OR UPPER(difunto_ap_paterno) LIKE '%S-N%'
        OR UPPER(difunto_ap_paterno) LIKE '%RECIEN%'
        OR UPPER(difunto_ap_paterno) LIKE '%IGNORA%'
        OR UPPER(difunto_ap_paterno) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_ap_paterno) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_ap_paterno) LIKE '% O %'
        OR UPPER(difunto_ap_paterno) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_ap_paterno) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_ap_paterno END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_ap_paterno,
trim(upper(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(                
    CASE 
        WHEN UPPER(difunto_ap_materno) LIKE 'QUE %'
        OR UPPER(difunto_ap_materno) LIKE '%DESCO%' 
        OR UPPER(difunto_ap_materno) LIKE '%FETO%' 
        OR UPPER(difunto_ap_materno) LIKE '%INDIVIDUO%' 
        OR UPPER(difunto_ap_materno) LIKE '%MUJER%' 
        OR UPPER(difunto_ap_materno) LIKE '%MENOR%' 
        OR UPPER(difunto_nombre) LIKE '%NIÑA%'
        OR UPPER(difunto_nombre) LIKE '%NIÑO%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑA%NOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑA%S%N%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑO%NOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%NIÑO%S%N%'
        OR UPPER(difunto_ap_materno) LIKE '%HOMBRE%'
        OR UPPER(difunto_ap_materno) LIKE '%MASCULINO%'
        OR UPPER(difunto_ap_materno) LIKE '%FEMENINO%'
        OR UPPER(difunto_ap_materno) LIKE '%ABORTO%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN NACIDO%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN NACIDA%'
        OR UPPER(difunto_ap_materno) LIKE '%NACIDO%'
        OR UPPER(difunto_ap_materno) LIKE '%NACIDA%'
        OR UPPER(difunto_ap_materno) LIKE '%S-N%'
        OR UPPER(difunto_ap_materno) LIKE '%RECIEN%'
        OR UPPER(difunto_ap_materno) LIKE '%IGNORA%'
        OR UPPER(difunto_ap_materno) LIKE '%ILEGIBLE%'
        OR UPPER(difunto_ap_materno) LIKE '%NOMBRE%' --556,233
        OR UPPER(difunto_ap_materno) LIKE '% O %'
        OR UPPER(difunto_ap_materno) LIKE '%INDETERMINABLE%'
        OR UPPER(difunto_ap_materno) LIKE 'DEL %' THEN 'DESCONOCIDO' ELSE difunto_ap_materno END, 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'), 'Û', 'U'), 'Ä', 'A'), 'Ö', 'O'), 'Ï', 'I'), 'Ç', 'C'), 'NIÃA', ''), 'NIÃO', ''), 'NIñA', '') 
                , 'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ'), "[^A-ZÁ-ŹÀ-ỲÜÑ ]+", ''))) as difunto_ap_materno,
    fecha_defuncion,difunto_edad,
    CASE 
        WHEN difunto_sexo='1.0' THEN 'H'
        WHEN difunto_sexo='2.0' THEN 'M'
        ELSE ''
    END as difunto_sexo,
    regexp_replace(difunto_nacionalidad_renapo,'\.0','') as difunto_nacionalidad_renapo,
    regexp_replace(registro_municipio_renapo,'\.0','') as registro_municipio_renapo,
    regexp_replace(registro_entidad_renapo,'\.0','') as registro_entidad_renapo,
    trim(UPPER(anotacion_renapo)) as anotacion_renapo,fecha_nacimiento        
    FROM sis_db_data_cln.rc_stg_def_1
    ORDER BY pk_defunciones,anio, ingreso, fecha_registro,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento







invalidate metadata


select difunto_nombre,difunto_sexo
from sis_db_data_dev.rc_stg_def_2
group by difunto_sexo order by difunto_sexo


select count(*) from sis_db_data_cln.rc_stg_def_1
select count(*) from sis_db_data_cln.rc_stg_def_2--6,014,056

select * from sis_db_data_cln.rc_stg_def_2

drop table sis_db_data_cln.rc_stg_def_2 purge

----- TABLA 3 -----

select count(*) from sis_db_data_cln.rc_stg_def_2 where fecha_nacimiento='' --6,010,414

select count(*) from (select pk_defunciones,anio, ingreso, fecha_registro,
        trim(regexp_replace(regexp_replace(difunto_nombre,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_nombre,
        trim(regexp_replace(regexp_replace(difunto_ap_paterno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_paterno,
        trim(regexp_replace(regexp_replace(difunto_ap_materno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_materno,
        fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
        from sis_db_data_cln.rc_stg_def_2
    )t0
where (fecha_nacimiento ='001-01-01' and difunto_nombre rlike '[^A-Z ]+' and difunto_ap_paterno rlike '[^A-Z ]+' and difunto_ap_materno rlike '[^A-Z ]+')
or ((difunto_nombre='DESCONOCIDO' or length(difunto_nombre)=1 or difunto_nombre='') and (difunto_ap_paterno='DESCONOCIDO' or length(difunto_ap_paterno)=1 or difunto_ap_paterno='') and (difunto_ap_materno='DESCONOCIDO' or length(difunto_ap_materno)=1 or difunto_ap_materno=''))
--145,979

--- prueba para enseñarle a carina
select fecha_nacimiento,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,count(*)
from (select pk_defunciones,anio, ingreso, fecha_registro,
        trim(regexp_replace(regexp_replace(difunto_nombre,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_nombre,
        trim(regexp_replace(regexp_replace(difunto_ap_paterno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_paterno,
        trim(regexp_replace(regexp_replace(difunto_ap_materno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_materno,
        fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
        from sis_db_data_cln.rc_stg_def_2
    )t0
where (fecha_nacimiento ='001-01-01' and difunto_nombre rlike '[^A-Z ]+' and difunto_ap_paterno rlike '[^A-Z ]+' and difunto_ap_materno rlike '[^A-Z ]+')
or ((difunto_nombre='DESCONOCIDO' or length(difunto_nombre)=1 or difunto_nombre='') and (difunto_ap_paterno='DESCONOCIDO' or length(difunto_ap_paterno)=1 or difunto_ap_paterno='') and (difunto_ap_materno='DESCONOCIDO' or length(difunto_ap_materno)=1 or difunto_ap_materno=''))
group by fecha_nacimiento,difunto_nombre,difunto_ap_paterno,difunto_ap_materno
order by fecha_nacimiento,difunto_nombre,difunto_ap_paterno,difunto_ap_materno


select count(*) from sis_db_data_cln.rc_stg_def_2 where difunto_ap_paterno rlike '^[A-Z ]+$'
select customer_id,regexp_replace(trim(first_name),' +',' ') from customer
select regexp_replace(trim(difunto_nombre),' +',' ') from (
    select difunto_nombre from sis_db_data_cln.rc_stg_def_2 where difunto_nombre like '%   %')t

select difunto_nombre,difunto_ap_paterno,difunto_ap_materno
from sis_db_data_cln.rc_stg_def_2
group by difunto_nombre,difunto_ap_paterno,difunto_ap_materno

--NEGACION
SELECT Count(*) FROM (select pk_defunciones,anio, ingreso, fecha_registro,
        trim(regexp_replace(regexp_replace(difunto_nombre,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_nombre,
        trim(regexp_replace(regexp_replace(difunto_ap_paterno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_paterno,
        trim(regexp_replace(regexp_replace(difunto_ap_materno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_materno,
        fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
        from sis_db_data_cln.rc_stg_def_2
    )t0
WHERE (fecha_nacimiento!='001-01-01' OR difunto_nombre NOT rlike '[^A-Z ]+' OR difunto_ap_paterno NOT rlike '[^A-Z ]+' OR difunto_ap_materno NOT rlike '[^A-Z ]+') --5,867,932
AND ((difunto_nombre!='DESCONOCIDO' AND length(difunto_nombre)!=1 AND difunto_nombre!='') OR (difunto_ap_paterno!='DESCONOCIDO' AND length(difunto_ap_paterno)!=1 AND difunto_ap_paterno!='') OR (difunto_ap_materno!='DESCONOCIDO' AND length(difunto_ap_materno)!=1 AND difunto_ap_materno!=''))



CREATE TABLE sis_db_data_cln.rc_stg_def_3 as (SELECT * FROM (select pk_defunciones,anio, ingreso, fecha_registro,
        trim(regexp_replace(regexp_replace(difunto_nombre,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_nombre,
        trim(regexp_replace(regexp_replace(difunto_ap_paterno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_paterno,
        trim(regexp_replace(regexp_replace(difunto_ap_materno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_materno,
        fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
        from sis_db_data_cln.rc_stg_def_2
    )t0
WHERE (fecha_nacimiento!='001-01-01' OR difunto_nombre NOT rlike '[^A-Z ]+' OR difunto_ap_paterno NOT rlike '[^A-Z ]+' OR difunto_ap_materno NOT rlike '[^A-Z ]+')
AND ((difunto_nombre!='DESCONOCIDO' AND length(difunto_nombre)!=1 AND difunto_nombre!='') OR (difunto_ap_paterno!='DESCONOCIDO' AND length(difunto_ap_paterno)!=1 AND difunto_ap_paterno!='') OR (difunto_ap_materno!='DESCONOCIDO' AND length(difunto_ap_materno)!=1 AND difunto_ap_materno!=''))
ORDER BY pk_defunciones,anio, ingreso, fecha_registro,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento) 


INSERT INTO sis_db_data_cln.rc_stg_def_3 SELECT * FROM (select pk_defunciones,anio, ingreso, fecha_registro,
        trim(regexp_replace(regexp_replace(difunto_nombre,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_nombre,
        trim(regexp_replace(regexp_replace(difunto_ap_paterno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_paterno,
        trim(regexp_replace(regexp_replace(difunto_ap_materno,'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''),' +',' ')) as difunto_ap_materno,
        fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento
        from sis_db_data_cln.rc_stg_def_2
    )t0
WHERE (fecha_nacimiento!='001-01-01' OR difunto_nombre NOT rlike '[^A-Z ]+' OR difunto_ap_paterno NOT rlike '[^A-Z ]+' OR difunto_ap_materno NOT rlike '[^A-Z ]+') --5,868,077
AND ((difunto_nombre!='DESCONOCIDO' AND length(difunto_nombre)!=1 AND difunto_nombre!='') OR (difunto_ap_paterno!='DESCONOCIDO' AND length(difunto_ap_paterno)!=1 AND difunto_ap_paterno!='') OR (difunto_ap_materno!='DESCONOCIDO' AND length(difunto_ap_materno)!=1 AND difunto_ap_materno!=''))
ORDER BY pk_defunciones,anio, ingreso, fecha_registro,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,anotacion_renapo,fecha_nacimiento

drop table sis_db_data_cln.rc_stg_def_3 purge


select count(*) from sis_db_data_cln.rc_stg_def_3--5756247
select * from sis_db_data_cln.rc_stg_def_3

invalidate metadata

--- TABLA 4 ----

select * from sis_db_data_cln.rc_stg_def_3
order by difunto_ap_paterno,difunto_nombre,difunto_ap_materno

select ingreso,fecha_registro,fecha_defuncion,fecha_nacimiento from sis_db_data_cln.rc_stg_def_3
group by ingreso,fecha_registro,fecha_defuncion,fecha_nacimiento
order by ingreso,fecha_registro,fecha_defuncion,fecha_nacimiento 

--
select pk_defunciones,difunto_nombre
from sis_db_data_cln.rc_stg_def_3
where difunto_nombre not rlike '^[A-Z ]+$'
AND difunto_nombre!=''
group by pk_defunciones,difunto_nombre order by difunto_nombre

select difunto_nombre
from sis_db_data_cln.rc_stg_def_3
where difunto_nombre not rlike '^[A-Z ]+$'
AND difunto_nombre!=''
group by difunto_nombre order by difunto_nombre


select difunto_ap_paterno
from sis_db_data_dev.rc_stg_def_3
where difunto_ap_paterno not rlike '^[A-Z ]+$'
group by difunto_ap_paterno order by difunto_ap_paterno

select difunto_ap_materno
from sis_db_data_cln.rc_stg_def_3
where difunto_ap_materno not rlike '^[A-Z ]+$'
group by difunto_ap_materno order by difunto_ap_materno


select pk_defunciones from sis_db_data_cln.rc_stg_def_3 where difunto_nombre like '%MARIA CONCEPÇION%'
select DIFUNTO_NOMBRE from sis_db_data_raw.rc_defunciones where pk_defunciones='2068329'
select difunto_nombre from sis_db_data_raw.rc_defunciones where difunto_nombre like '%Á%'

--- Veo duplicados en defunciones
select count(*) from(
    SELECT difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,fecha_nacimiento,count(*)
    FROM sis_db_data_cln.rc_stg_def_3
    GROUP BY difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,fecha_nacimiento
    HAVING count(*) >1 ORDER BY count(*) DESC
)t --21,977

select count(*) from (
    select max(pk_defunciones) as pk_defunciones,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,
    fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,
    registro_entidad_renapo,fecha_nacimiento
    from sis_db_data_cln.rc_stg_def_3
    group by difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,fecha_nacimiento
    order by pk_defunciones,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,fecha_nacimiento
) t



CREATE TABLE IF NOT EXISTS sis_db_data_cln.rc_stg_def_4 AS (select max(pk_defunciones) as pk_defunciones,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,
    fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,
    registro_entidad_renapo,fecha_nacimiento
    from sis_db_data_cln.rc_stg_def_3
    group by difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,fecha_nacimiento
    order by pk_defunciones,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,fecha_nacimiento
)


INSERT INTO sis_db_data_cln.rc_stg_def_4 select max(pk_defunciones) as pk_defunciones,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,
    fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,
    registro_entidad_renapo,fecha_nacimiento
    from sis_db_data_cln.rc_stg_def_3
    group by difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,fecha_nacimiento
    order by pk_defunciones,difunto_nombre,difunto_ap_paterno,difunto_ap_materno,fecha_defuncion,difunto_edad,difunto_sexo,difunto_nacionalidad_renapo,registro_municipio_renapo,registro_entidad_renapo,fecha_nacimiento



invalidate metadata

select count(*) from sis_db_data_cln.rc_stg_def_4
select * from sis_db_data_cln.rc_stg_def_4


------ TABLA 5 --------


select count(*) from (

    SELECT t1.pk_defunciones pkDef,
    t1.difunto_nombre difuntoNom,
    t1.difunto_ap_paterno difuntoApPat,
    t1.difunto_ap_materno difuntoApMat,
    t1.fecha_defuncion fcDef,
    t1.difunto_edad difuntoEdad,
    t1.difunto_sexo difuntoSex,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.pareja_difunto),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) parejaDif,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.declarante_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.declarante_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.declarante_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraApMat,
    t2.declarante_parentesco declaraPar,
    trim(regexp_replace(regexp_replace(t2.declarante_edad,'\.0',''),' +',' ')) declaraEdad,
    t2.declarante_estado declaraEdo,
    t2.declarante_delegacion declaraDel,
    t2.declarante_colonia declaraCol,
    t2.declarante_calle declaraCalle,
    t2.declarante_nacionalidad declaraNaci,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.padre_difunto_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.padre_difunto_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.padre_difunto_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreApMat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.madre_difunto_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.madre_difunto_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.madre_difunto_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreApMat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.testigo1_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo1Nom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.testigo1_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo1ApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.testigo1_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testtigo1ApMat,   
    t2.testigo1_estado testigo1Edo,
    t2.TESTIGO1_DELEGACION testigo1Del,
    t2.TESTIGO1_COLONIA testigo1Col,
    t2.TESTIGO1_CALLE testigo1Calle,
    t2.TESTIGO1_NACIONALIDAD testigo1Naci,
    t2.TESTIGO1_PARENTESCO testigo1Paren,
    trim(regexp_replace(regexp_replace(t2.TESTIGO1_EDAD,'\.0',''),' +',' ')) testigo1Edad,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.testigo2_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2Nom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.testigo2_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2ApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper(t2.testigo2_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2ApMat,   
    t2.testigo2_estado testigo2Edo,
    t2.testigo2_DELEGACION testigo2Del,
    t2.testigo2_COLONIA testigo2Col,
    t2.testigo2_CALLE testigo2Calle,
    t2.testigo2_NACIONALIDAD testigo2Naci,
    t2.testigo2_PARENTESCO testigo2Paren,
    trim(regexp_replace(regexp_replace(t2.testigo2_EDAD,'\.0',''),' +',' ')) testigo2Edad,
    t1.difunto_nacionalidad_renapo difuntoNaciRenapo,
    t1.registro_municipio_renapo munRenapo,
    t1.registro_entidad_renapo entRenapo,
    t2.anotacion_renapo anotRenapo,
    t1.fecha_nacimiento difuntoFcNac,
    t2.difunto_estado difuntoEdo,
    t2.difunto_domicilio difuntoDom
    FROM sis_db_data_cln.rc_stg_def_4 t1
        LEFT JOIN(SELECT * FROM sis_db_data_raw.rc_defunciones) t2
    ON t1.pk_defunciones = t2.pk_defunciones
    ORDER BY t1.pk_defunciones,t1.difunto_nombre,t1.difunto_ap_paterno,t1.difunto_ap_materno,
    t1.fecha_defuncion
    
    
) t3 


CREATE TABLE IF NOT EXISTS sis_db_data_cln.rc_stg_def_5 AS
    SELECT t1.pk_defunciones pkDef,
    t1.difunto_nombre difuntoNom,
    t1.difunto_ap_paterno difuntoApPat,
    t1.difunto_ap_materno difuntoApMat,
    t1.fecha_defuncion fcDef,
    t1.difunto_edad difuntoEdad,
    t1.difunto_sexo difuntoSex,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.pareja_difunto),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) parejaDif,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.declarante_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.declarante_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.declarante_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraApMat,
    t2.declarante_parentesco declaraPar,
    trim(regexp_replace(regexp_replace(t2.declarante_edad,'\.0',''),' +',' ')) declaraEdad,
    t2.declarante_estado declaraEdo,
    t2.declarante_delegacion declaraDel,
    t2.declarante_colonia declaraCol,
    t2.declarante_calle declaraCalle,
    t2.declarante_nacionalidad declaraNaci,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.padre_difunto_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.padre_difunto_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.padre_difunto_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreApMat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.madre_difunto_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.madre_difunto_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.madre_difunto_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreApMat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo1_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo1Nom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo1_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo1ApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo1_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testtigo1ApMat,   
    t2.testigo1_estado testigo1Edo,
    t2.TESTIGO1_DELEGACION testigo1Del,
    t2.TESTIGO1_COLONIA testigo1Col,
    t2.TESTIGO1_CALLE testigo1Calle,
    t2.TESTIGO1_NACIONALIDAD testigo1Naci,
    t2.TESTIGO1_PARENTESCO testigo1Paren,
    trim(regexp_replace(regexp_replace(t2.TESTIGO1_EDAD,'\.0',''),' +',' ')) testigo1Edad,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo2_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2Nom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo2_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2ApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo2_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2ApMat,   
    t2.testigo2_estado testigo2Edo,
    t2.testigo2_DELEGACION testigo2Del,
    t2.testigo2_COLONIA testigo2Col,
    t2.testigo2_CALLE testigo2Calle,
    t2.testigo2_NACIONALIDAD testigo2Naci,
    t2.testigo2_PARENTESCO testigo2Paren,
    trim(regexp_replace(regexp_replace(t2.testigo2_EDAD,'\.0',''),' +',' ')) testigo2Edad,
    t1.difunto_nacionalidad_renapo difuntoNaciRenapo,
    t1.registro_municipio_renapo munRenapo,
    t1.registro_entidad_renapo entRenapo,
    t2.anotacion_renapo anotRenapo,
    t1.fecha_nacimiento difuntoFcNac,
    t2.difunto_estado difuntoEdo,
    t2.difunto_domicilio difuntoDom
    FROM sis_db_data_cln.rc_stg_def_4 t1
        LEFT JOIN(SELECT * FROM sis_db_data_raw.rc_defunciones) t2
    ON t1.pk_defunciones = t2.pk_defunciones
    ORDER BY t1.pk_defunciones,t1.difunto_nombre,t1.difunto_ap_paterno,t1.difunto_ap_materno,
    t1.fecha_defuncion
    
    
INSERT INTO sis_db_data_cln.rc_stg_def_5
    SELECT t1.pk_defunciones pkDef,
    t1.difunto_nombre difuntoNom,
    t1.difunto_ap_paterno difuntoApPat,
    t1.difunto_ap_materno difuntoApMat,
    t1.fecha_defuncion fcDef,
    t1.difunto_edad difuntoEdad,
    t1.difunto_sexo difuntoSex,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.pareja_difunto),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) parejaDif,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.declarante_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.declarante_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.declarante_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) declaraApMat,
    t2.declarante_parentesco declaraPar,
    trim(regexp_replace(regexp_replace(t2.declarante_edad,'\.0',''),' +',' ')) declaraEdad,
    t2.declarante_estado declaraEdo,
    t2.declarante_delegacion declaraDel,
    t2.declarante_colonia declaraCol,
    t2.declarante_calle declaraCalle,
    t2.declarante_nacionalidad declaraNaci,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.padre_difunto_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.padre_difunto_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.padre_difunto_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) padreApMat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.madre_difunto_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreNom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.madre_difunto_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.madre_difunto_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) madreApMat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo1_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo1Nom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo1_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo1ApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo1_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testtigo1ApMat,   
    t2.testigo1_estado testigo1Edo,
    t2.TESTIGO1_DELEGACION testigo1Del,
    t2.TESTIGO1_COLONIA testigo1Col,
    t2.TESTIGO1_CALLE testigo1Calle,
    t2.TESTIGO1_NACIONALIDAD testigo1Naci,
    t2.TESTIGO1_PARENTESCO testigo1Paren,
    trim(regexp_replace(regexp_replace(t2.TESTIGO1_EDAD,'\.0',''),' +',' ')) testigo1Edad,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo2_nombre),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2Nom,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo2_ap_paterno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2ApPat,
    trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(upper(t2.testigo2_ap_materno),'[^A-ZÁ-ŹÀ-ỲÜÑ ]+',''),' +',' '),'^[A]+$|^[B]+$|^[C]+$|^[D]+$|^[E]+$|^[F]+$|^[G]+$|^[H]+$|^[I]+$|^[J]+$|^[K]+$|^[L]+$|^[M]+$|^[N]+$|^[Ñ]+$|^[O]+$|^[P]+$|^[Q]+$|^[R]+$|^[S]+$|^[T]+$|^[U]+$|^[V]+$|^[W]+$|^[X]+$|^[Y]+$|^[Z]+$',''), 'Á', 'A'), 'Ć', 'C'),'É', 'E'),'Ǵ', 'G'),'Í', 'I'),'Ĺ', 'L'),'Ń', 'N'),'Ó', 'O'),'Ŕ', 'R'),'Ś', 'S'),'Ú', 'U'),'Ǘ', 'U'),'Ẃ', 'W'), 'Ý', 'Y'), 'Ź', 'Ź'),'À', 'A'), 'È', 'E'), 'Ì', 'I'), 'Ǹ', 'N'), 'Ò', 'O'), 'Ù', 'U'), 'Ü', 'U'), 'Ẁ', 'W'), 'Ć', 'Ỳ')
    ) testigo2ApMat,   
    t2.testigo2_estado testigo2Edo,
    t2.testigo2_DELEGACION testigo2Del,
    t2.testigo2_COLONIA testigo2Col,
    t2.testigo2_CALLE testigo2Calle,
    t2.testigo2_NACIONALIDAD testigo2Naci,
    t2.testigo2_PARENTESCO testigo2Paren,
    trim(regexp_replace(regexp_replace(t2.testigo2_EDAD,'\.0',''),' +',' ')) testigo2Edad,
    t1.difunto_nacionalidad_renapo difuntoNaciRenapo,
    t1.registro_municipio_renapo munRenapo,
    t1.registro_entidad_renapo entRenapo,
    t2.anotacion_renapo anotRenapo,
    t1.fecha_nacimiento difuntoFcNac,
    t2.difunto_estado difuntoEdo,
    t2.difunto_domicilio difuntoDom
    FROM sis_db_data_cln.rc_stg_def_4 t1
        LEFT JOIN(SELECT * FROM sis_db_data_raw.rc_defunciones) t2
    ON t1.pk_defunciones = t2.pk_defunciones
    ORDER BY t1.pk_defunciones,t1.difunto_nombre,t1.difunto_ap_paterno,t1.difunto_ap_materno,
    t1.fecha_defuncion

select * from sis_db_data_cln.rc_stg_def_5
select count(*) from sis_db_data_cln.rc_stg_def_5--5,734,093

select * from sis_db_data_cln.rc_stg_def_4
where difunto_ap_paterno like '%PENA%'

select * from sis_db_data_cln.rc_stg_nac_5
where titular_ap_paterno like '%PENA%'

select * from sis_db_data_cln.rc_stg_def_4
where difunto_nombre like '%Ñ%'


select PAREJA_DIFUNTO from sis_db_data_raw.rc_defunciones





invalidate metadata


drop table sis_db_data_dev.rc_stg_def_1 purge
drop table sis_db_data_dev.rc_stg_def_2 purge
drop table sis_db_data_dev.rc_stg_def_3 purge


drop table sis_db_data_cln.rc_stg_def_1 purge
drop table sis_db_data_cln.rc_stg_def_2 purge
drop table sis_db_data_cln.rc_stg_def_3 purge
drop table sis_db_data_cln.rc_stg_def_4 purge
drop table sis_db_data_cln.rc_stg_def_5 purge


select count(*) from sis_db_data_cln.rc_stg_nac_1

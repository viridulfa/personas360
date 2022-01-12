CREATE TABLE IF NOT EXISTS sis_db_data_raw.pv(
llave string,
vin string,
clave_vehicular string,
placa string,
estatus_registro string,
cveentida string,
rfv string,
marca string,
linea string,
clase_vehiculo string,
tipo_vehiculo string,
modelo string,
version string,
fecha_alta string,
numero_motor string,
cilindros string,
combustible string,
tipo_servicio string,
procedencia string,
fecha_factura string,
num_factura string,
importe_factura string,
placa_anterior string,
rfc string,
nombre_propietario string,
fecnacimiento string,
calle string,
numero_exterior string,
numero_interior string,
colonia string,
codigo_postal string,
entidad_federativa_propietario string,
municipio_propietario string,
localidad_propietario string,
telefono string,
capacidad_carga string,
repuve string,
tipo_cv string,
vigencia string,
litros string,
numero_de_la_tarjeta string,
personas string,
no_calc_egalizacion string,
fecha_legalizacion string,
oficina_expedidora string,
estatus_tramite string,
operador string,
folio_logico_tc string,
folio_fisico_tc string,
expedicion_tc string,
expiracion_tc string,
operacion string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
'field.delim'='|')
STORED AS TEXTFILE LOCATION '/data/siadipsbx/raw/padron_vehicular/data'
TBLPROPERTIES ('skip.header.line.count'='1');


invalidate metadata;

create table if not exists sis_db_data_raw.pv_padron as select * from sis_db_data_raw.pv;

select count(*) from sis_db_data_raw.pv; --6,144,678
select * from sis_db_data_raw.pv;

select estatus_registro from sis_db_data_raw.pv group by estatus_registro ;


drop table sis_db_data_raw.pv purge;
drop table sis_db_data_raw.pv_padron purge;
















select count(*) from sis_db_data_dev.stg_dom_veh; --6,102,138



select rfc,max(nombre_completo) nom_com,group_concat(concat_ws(' ',placa,cast(id_veh as string))) placa
from sis_db_data_dev.stg_dom_veh
group by rfc;

select rfc,count(distinct nombre_completo),group_concat(nombre_completo)
from  sis_db_data_dev.stg_dom_veh
group by rfc
having count(distinct nombre_completo)>1;

select rfc,group_concat(nombre_completo)
from  sis_db_data_dev.stg_dom_veh
group by  rfc,nombre_completo
having count(*)>1;

select cve_unica, count(distinct curp)
from comerciantes
group by cve_unica
having count(distinct curp) >= 1;



select * from sis_db_data_dev.stg_dom_veh where id_veh=406826;

--- join con join_stg_nac_def_vac_llv_direc_telef 

select count(distinct(id_veh)) from sis_db_data_dev.stg_dom_veh;--6,102,138

select nombre_completo,fc_nacimiento from sis_db_data_dev.stg_dom_veh
group by nombre_completo,fc_nacimiento
having count(*) >1;

select substr(fc_nacimiento,1,4)
from sis_db_data_dev.stg_dom_veh
group by substr(fc_nacimiento,1,4) order by substr(fc_nacimiento,1,4); --hay fechas nulas

select count(*) from sis_db_data_dev.stg_dom_veh
where nombre_completo is null or nombre_completo=''; --3,190 nulos

select * from join_stg_nac_def_vac_llv_direc_telef ;

select count(*) from (

    select pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
    direcciones,telefonos,
    group_concat(vehiculos) vehiculos
    from (
        select t2.pknac,t2.pkvac,t2.pkllv,t2.fcnacnac,t2.curpnac,t2.nomnac,t2.apnac,t2.amnac,t2.sexonac,t2.fcnacvac,t2.curpvac,t2.nomvac,t2.apvac,t2.amvac,t2.fcnacllv,t2.curpllv,t2.nomllv,t2.apllv,t2.amllv,t2.sexollv,
        t2.direcciones,t2.telefonos,
        concat_ws('','VEHICULO:',
                'idveh:',ifnull(cast(t4.id_veh as string),''),'placa:',ifnull(cast(t4.placa as string),''),'anio:',ifnull(cast(t4.ano as string),''),
                'factura:',ifnull(cast(t4.importe_factura as string),''),'marca:',ifnull(cast(t4.id_marca as string),''),'motor:',ifnull(cast(t4.clv_motor as string),''),
                'vin:',ifnull(cast(t4.vin as string),''),'linea:',ifnull(cast(t4.linea as string),''),'version:',ifnull(cast(t4.ver as string),''))
         vehiculos
        from(
            select t1.*,case
                when (t1.nac='' and t1.vac='') then t1.llv 
                when (t1.nac='' and t1.vac!='') then t1.vac 
                else t1.nac end colmatch1
                from (
                    select t0.*,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacnac,''),coalesce(nomnac,''),coalesce(apnac,''),coalesce(amnac,'')),'-',''),'[ ]+','') nac,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') vac,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                    from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef t0
                )t1
        ) t2
        LEFT JOIN (select t3.*,regexp_replace(regexp_replace(concat_ws('',ifnull(fc_nacimiento,''),ifnull(nombre_completo,'')),'-',''),'[ ]+','') colmatch2
                    from sis_db_data_dev.stg_dom_veh t3
                    where t3.nombre_completo is not null) t4
        ON t2.colmatch1 = t4.colmatch2
    ) final
    group by pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,direcciones,telefonos
    
    
)final; --19,295,796





SELECT entidad,mun,col,calle,cp,num_ext,num_int from sis_db_data_dev.stg_dom_veh
where entidad like '%|%' or mun like '%|%' or calle like '%|%' or cp like '%|%' or num_ext like '%|%' or num_int like '%|%'; -- sí hay

select regexp_replace('hola|','\\|','');

CREATE TABLE IF NOT EXISTS sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
AS (
    --SELECT COUNT(*) FROM(
    SELECT id,pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,telefonos,direcciones,
    case when vehiculos='VEHICULO:idveh:placa:anio:factura:marca:motor:vin:linea:version:' then '' else vehiculos end vehiculos
    FROM(
        SELECT row_number() over (order by cast(pknac as int)) id,
        pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
        concat_ws(' | ',ifnull(telefonos,''),telefonospadron) telefonos, concat_ws(' | ',ifnull(direcciones,''),direccionespadron) direcciones, vehiculos
        FROM (
            select 
            pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
            direcciones,
            group_concat(direccionespadron,' |DIREC VEHICULOS: ') direccionespadron,
            telefonos,
            ifnull(group_concat(telefonospadron,' | '),'') telefonospadron,
            group_concat(vehiculos,' | ') vehiculos
            from (
                select t2.pknac,t2.pkvac,t2.pkllv,t2.fcnacnac,t2.curpnac,t2.nomnac,t2.apnac,t2.amnac,t2.sexonac,t2.fcnacvac,t2.curpvac,t2.nomvac,t2.apvac,t2.amvac,t2.fcnacllv,t2.curpllv,t2.nomllv,t2.apllv,t2.amllv,t2.sexollv,
                concat_ws(' ',
                'ent:',regexp_replace(ifnull(cast(t4.entidad as string),''),'\\|',' '),'mun:',regexp_replace(ifnull(cast(t4.mun as string),''),'\\|',' '),'col:',regexp_replace(ifnull(cast(t4.col as string),''),'\\|',' '),
                'calle:',regexp_replace(ifnull(cast(t4.calle as string),''),'\\|',' '),'cp:',regexp_replace(ifnull(cast(t4.cp as string),''),'\\|',' '),'numext:',regexp_replace(ifnull(cast(t4.num_ext as string),''),'\\|',' '),
                'numint:',regexp_replace(ifnull(cast(t4.num_int as string),''),'\\|',' ')) direccionespadron,            
                t2.direcciones,
                t2.telefonos,
                trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                ifnull(cast(t4.tel as string),''),'\\|',' '),'[^0-9]+',''),'^0$',''),'[ ]+','')) telefonospadron,              
                concat_ws('','VEHICULO:',
                        'idveh:',ifnull(cast(t4.id_veh as string),''),'placa:',ifnull(cast(t4.placa as string),''),'anio:',ifnull(cast(t4.ano as string),''),
                        'factura:',ifnull(cast(t4.importe_factura as string),''),'marca:',ifnull(cast(t4.id_marca as string),''),'motor:',ifnull(cast(t4.clv_motor as string),''),
                        'vin:',ifnull(cast(t4.vin as string),''),'linea:',ifnull(cast(t4.linea as string),''),'version:',ifnull(cast(t4.ver as string),''))
                 vehiculos
                from(
                    select t1.*,case
                        when (t1.nac='' and t1.vac='') then t1.llv 
                        when (t1.nac='' and t1.vac!='') then t1.vac 
                        else t1.nac end colmatch1
                        from (
                            select t0.*,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacnac,''),coalesce(nomnac,''),coalesce(apnac,''),coalesce(amnac,'')),'-',''),'[ ]+','') nac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') vac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                            from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef t0
                        )t1
                ) t2
                LEFT JOIN (select t3.*,regexp_replace(regexp_replace(concat_ws('',ifnull(fc_nacimiento,''),ifnull(nombre_completo,'')),'-',''),'[ ]+','') colmatch2
                            from sis_db_data_dev.stg_dom_veh t3
                            where t3.nombre_completo is not null) t4
                ON t2.colmatch1 = t4.colmatch2
            ) tabla1
            group by pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,direcciones,telefonos
            --order by pknac
        ) tabla2
    )tabla3    
    --where vehiculos!='VEHICULO:idveh:placa:anio:factura:marca:motor:vin:linea:version:'
    order by cast(id as int)
    --) conteo --19,295,796
);


INSERT INTO sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
    SELECT id,pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,telefonos,direcciones,
    case when vehiculos='VEHICULO:idveh:placa:anio:factura:marca:motor:vin:linea:version:' then '' else vehiculos end vehiculos
    FROM(
        SELECT row_number() over (order by cast(pknac as int)) id,
        pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
        concat_ws(' | ',ifnull(telefonos,''),telefonospadron) telefonos, concat_ws(' | ',ifnull(direcciones,''),direccionespadron) direcciones, vehiculos
        FROM (
            select 
            pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
            direcciones,
            group_concat(direccionespadron,' |DIREC VEHICULOS: ') direccionespadron,
            telefonos,
            ifnull(group_concat(telefonospadron,' | '),'') telefonospadron,
            group_concat(vehiculos,' | ') vehiculos
            from (
                select t2.pknac,t2.pkvac,t2.pkllv,t2.fcnacnac,t2.curpnac,t2.nomnac,t2.apnac,t2.amnac,t2.sexonac,t2.fcnacvac,t2.curpvac,t2.nomvac,t2.apvac,t2.amvac,t2.fcnacllv,t2.curpllv,t2.nomllv,t2.apllv,t2.amllv,t2.sexollv,
                concat_ws(' ',
                'ent:',regexp_replace(ifnull(cast(t4.entidad as string),''),'\\|',' '),'mun:',regexp_replace(ifnull(cast(t4.mun as string),''),'\\|',' '),'col:',regexp_replace(ifnull(cast(t4.col as string),''),'\\|',' '),
                'calle:',regexp_replace(ifnull(cast(t4.calle as string),''),'\\|',' '),'cp:',regexp_replace(ifnull(cast(t4.cp as string),''),'\\|',' '),'numext:',regexp_replace(ifnull(cast(t4.num_ext as string),''),'\\|',' '),
                'numint:',regexp_replace(ifnull(cast(t4.num_int as string),''),'\\|',' ')) direccionespadron,            
                t2.direcciones,
                t2.telefonos,
                trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                ifnull(cast(t4.tel as string),''),'\\|',' '),'[^0-9]+',''),'^0$',''),'[ ]+','')) telefonospadron,              
                concat_ws('','VEHICULO:',
                        'idveh:',ifnull(cast(t4.id_veh as string),''),'placa:',ifnull(cast(t4.placa as string),''),'anio:',ifnull(cast(t4.ano as string),''),
                        'factura:',ifnull(cast(t4.importe_factura as string),''),'marca:',ifnull(cast(t4.id_marca as string),''),'motor:',ifnull(cast(t4.clv_motor as string),''),
                        'vin:',ifnull(cast(t4.vin as string),''),'linea:',ifnull(cast(t4.linea as string),''),'version:',ifnull(cast(t4.ver as string),''))
                 vehiculos
                from(
                    select t1.*,case
                        when (t1.nac='' and t1.vac='') then t1.llv 
                        when (t1.nac='' and t1.vac!='') then t1.vac 
                        else t1.nac end colmatch1
                        from (
                            select t0.*,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacnac,''),coalesce(nomnac,''),coalesce(apnac,''),coalesce(amnac,'')),'-',''),'[ ]+','') nac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') vac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                            from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef t0
                        )t1
                ) t2
                LEFT JOIN (select t3.*,regexp_replace(regexp_replace(concat_ws('',ifnull(fc_nacimiento,''),ifnull(nombre_completo,'')),'-',''),'[ ]+','') colmatch2
                            from sis_db_data_dev.stg_dom_veh t3
                            where t3.nombre_completo is not null) t4
                ON t2.colmatch1 = t4.colmatch2
            ) tabla1
            group by pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,direcciones,telefonos
        ) tabla2
    )tabla3    
    order by cast(id as int);

-------------------- PEGAR IDS MONGO
CREATE TABLE IF NOT EXISTS sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh_idmongo as (
    select t2.idmongo,t1.*
    from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh t1
    left join (select * from sis_db_data_cln.objectidmongo) t2
    on cast(t1.id as int) = cast(t2.id as int)
    order by cast(t1.id as int)
);

select count(*) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh; --19,295,796
select count(*) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh_idmongo; --19,295,796
SELECT count(pknac) FROM sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh_idmongo;

select count(*) from sis_db_data_cln.rc_stg_nac_7;


select * from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh_idmongo;

INVALIDATE METADATA;

--drop table sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh purge ;

select count(*) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh; --19,295,796
select * from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh order by id;
select direcciones from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh;
select * from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef where pknac='40213407'; --nac
select * from sis_db_data_dev.stg_dom_veh where id_veh=5646853; --veh



select nac,vac,llv,colmatch
--select count(*)
--select *
from(
    select t2.*,case
                when (nac='' and vac='') then llv 
                when (nac='' and vac!='') then vac 
                else nac end colmatch
    from (
            select t.*,
            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacnac,''),coalesce(nomnac,''),coalesce(apnac,''),coalesce(amnac,'')),'-',''),'[ ]+','') nac,
            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') vac,
            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
            from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef t
    ) t2
) t3
where t3.nac='' and t3.vac='' and llv=''; -- hay 11 vacíos en nombres de nacimientos, vacunas, y llave pero tienen curp




select *
from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
where nomnac='' and apnac='' and amnac='' and nomvac='' and apvac='' and amvac='' and nomllv='' and apllv='' and amllv='';


select *
from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
where nomnac is null	and apnac is null	and amnac is null
and nomvac is null	and apvac is null	and amvac is null 
and nomllv is null and apllv is null	and amllv is null; --0 



select count(*)
from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
where pknac is null and pkvac is null and pkllv is null;

--19,295,796


select count(*)
from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef
where pknac is null	and pkvac is null	and pkllv is null	and fcnacnac is null	and curpnac is null
and nomnac is null	and apnac is null	and amnac is null	and sexonac is null	and fcnacvac is null	and curpvac	is null
and nomvac is null	and apvac is null	and amvac is null	and fcnacllv is null	and curpllv is null	and nomllv is null	and apllv is null	and amllv is null; --0 

select count(*)
from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef
where pknac =''     and fcnacnac =''    and curpnac =''
and nomnac =''  and apnac =''   and amnac =''   and sexonac ='' and fcnacvac =''    and curpvac =''
and nomvac =''  and apvac =''   and amvac =''   and fcnacllv =''    and curpllv ='' and nomllv =''  and apllv =''   and amllv =''; --0 







select count(*) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
where nomnac is null ;



select count(*) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh ; --19,295,796

select count(*) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh where vehiculos !=''; --2,589,030

select count(distinct vehiculos) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh; --1,840,495

select count(distinct vehiculos) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh where vehiculos !=''; --1,840,495


select vehiculos,count(*) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
group by vehiculos
having count(*) >1;



select * from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
where vehiculos ='VEHICULO:idveh:3238594placa:294XKCanio:2011factura:216800marca:4motor:MR18598667Hvin:3N1BC1CDXBL394992linea:38version:3';


-------------------------------------- SE CAMBIA EL PADRON Y SE VUELVEN A HACER LOS JOIN

    SELECT id,pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,telefonos,direcciones,
    case when vehiculos='VEHICULO:idveh:estatus_registro:placa:anio:factura:marca:motor:vin:linea:version:' then '' else vehiculos end vehiculos
    FROM(
        SELECT row_number() over (order by cast(pknac as int)) id,
        pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
        concat_ws(' | ',ifnull(telefonos,''),telefonospadron) telefonos, concat_ws(' | ',ifnull(direcciones,''),direccionespadron) direcciones, vehiculos
        FROM (
            select 
            pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
            direcciones,
            group_concat(direccionespadron,' |DIREC VEHICULOS: ') direccionespadron,
            telefonos,
            ifnull(group_concat(telefonospadron,' | '),'') telefonospadron,
            group_concat(vehiculos,' | ') vehiculos
            from (
                select t2.pknac,t2.pkvac,t2.pkllv,t2.fcnacnac,t2.curpnac,t2.nomnac,t2.apnac,t2.amnac,t2.sexonac,t2.fcnacvac,t2.curpvac,t2.nomvac,t2.apvac,t2.amvac,t2.fcnacllv,t2.curpllv,t2.nomllv,t2.apllv,t2.amllv,t2.sexollv,
                concat_ws(' ',
                'ent:',regexp_replace(ifnull(cast(t4.entidad_federativa_propietario as string),''),'\\|',' '),'mun:',regexp_replace(ifnull(cast(t4.municipio_propietario as string),''),'\\|',' '),'col:',regexp_replace(ifnull(cast(t4.colonia as string),''),'\\|',' '),
                'calle:',regexp_replace(ifnull(cast(t4.calle as string),''),'\\|',' '),'cp:',regexp_replace(ifnull(cast(t4.codigo_postal as string),''),'\\|',' '),'numext:',regexp_replace(ifnull(cast(t4.numero_exterior as string),''),'\\|',' '),
                'numint:',regexp_replace(ifnull(cast(t4.numero_interior as string),''),'\\|',' ')) direccionespadron,            
                t2.direcciones,
                t2.telefonos,
                trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                ifnull(cast(t4.telefono as string),''),'\\|',' '),'[^0-9]+',''),'^0$',''),'[ ]+','')) telefonospadron,              
                concat_ws('','VEHICULO:',
                        'idveh:',ifnull(cast(t4.id as string),''),'estatus_registro:',ifnull(cast(t4.estatus_registro as string),''),'placa:',ifnull(cast(t4.placa as string),''),'anio:',ifnull(cast(t4.modelo as string),''),
                        'factura:',ifnull(cast(t4.importe_factura as string),''),'marca:',ifnull(cast(t4.marca as string),''),'motor:',ifnull(cast(t4.numero_motor as string),''),
                        'vin:',ifnull(cast(t4.vin as string),''),'linea:',ifnull(cast(t4.linea as string),''),'version:',ifnull(cast(t4.version as string),''))
                 vehiculos
                from(
                    select t1.*,case
                        when (t1.nac='' and t1.vac='') then t1.llv 
                        when (t1.nac='' and t1.vac!='') then t1.vac 
                        else t1.nac end colmatch1
                        from (
                            select t0.*,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacnac,''),coalesce(nomnac,''),coalesce(apnac,''),coalesce(amnac,'')),'-',''),'[ ]+','') nac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') vac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                            from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef t0
                        )t1
                ) t2
                LEFT JOIN (select t3.*,regexp_replace(regexp_replace(concat_ws('',ifnull(fecnacimiento,''),ifnull(nombre_propietario,'')),'-',''),'[ ]+','') colmatch2
                            from sis_db_data_cln.pv_personas_fisicas_id t3
                            where t3.nombre_propietario is not null and t3.nombre_propietario!='' and t3.fecnacimiento is not null and t3.fecnacimiento !=''
                            ) t4
                ON t2.colmatch1 = t4.colmatch2
            ) tabla1
            group by pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,direcciones,telefonos
        ) tabla2
    )tabla3    
    order by cast(id as int);


-- borro la tabla anterior
drop table sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh purge;

-- creo la tabla de nuevo
CREATE TABLE IF NOT EXISTS sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
AS (
    SELECT id,pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,telefonos,direcciones,
    case when vehiculos='VEHICULO:idveh:estatus_registro:placa:anio:factura:marca:motor:vin:linea:version:' then '' else vehiculos end vehiculos
    FROM(
        SELECT row_number() over (order by cast(pknac as int)) id,
        pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
        concat_ws(' | ',ifnull(telefonos,''),telefonospadron) telefonos, concat_ws(' | ',ifnull(direcciones,''),direccionespadron) direcciones, vehiculos
        FROM (
            select 
            pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
            direcciones,
            group_concat(direccionespadron,' |DIREC VEHICULOS: ') direccionespadron,
            telefonos,
            ifnull(group_concat(telefonospadron,' | '),'') telefonospadron,
            group_concat(vehiculos,' | ') vehiculos
            from (
                select t2.pknac,t2.pkvac,t2.pkllv,t2.fcnacnac,t2.curpnac,t2.nomnac,t2.apnac,t2.amnac,t2.sexonac,t2.fcnacvac,t2.curpvac,t2.nomvac,t2.apvac,t2.amvac,t2.fcnacllv,t2.curpllv,t2.nomllv,t2.apllv,t2.amllv,t2.sexollv,
                concat_ws(' ',
                'ent:',regexp_replace(ifnull(cast(t4.entidad_federativa_propietario as string),''),'\\|',' '),'mun:',regexp_replace(ifnull(cast(t4.municipio_propietario as string),''),'\\|',' '),'col:',regexp_replace(ifnull(cast(t4.colonia as string),''),'\\|',' '),
                'calle:',regexp_replace(ifnull(cast(t4.calle as string),''),'\\|',' '),'cp:',regexp_replace(ifnull(cast(t4.codigo_postal as string),''),'\\|',' '),'numext:',regexp_replace(ifnull(cast(t4.numero_exterior as string),''),'\\|',' '),
                'numint:',regexp_replace(ifnull(cast(t4.numero_interior as string),''),'\\|',' ')) direccionespadron,            
                t2.direcciones,
                t2.telefonos,
                trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                ifnull(cast(t4.telefono as string),''),'\\|',' '),'[^0-9]+',''),'^0$',''),'[ ]+','')) telefonospadron,              
                concat_ws('','VEHICULO:',
                        'idveh:',ifnull(cast(t4.id as string),''),'estatus_registro:',ifnull(cast(t4.estatus_registro as string),''),'placa:',ifnull(cast(t4.placa as string),''),'anio:',ifnull(cast(t4.modelo as string),''),
                        'factura:',ifnull(cast(t4.importe_factura as string),''),'marca:',ifnull(cast(t4.marca as string),''),'motor:',ifnull(cast(t4.numero_motor as string),''),
                        'vin:',ifnull(cast(t4.vin as string),''),'linea:',ifnull(cast(t4.linea as string),''),'version:',ifnull(cast(t4.version as string),''))
                 vehiculos
                from(
                    select t1.*,case
                        when (t1.nac='' and t1.vac='') then t1.llv 
                        when (t1.nac='' and t1.vac!='') then t1.vac 
                        else t1.nac end colmatch1
                        from (
                            select t0.*,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacnac,''),coalesce(nomnac,''),coalesce(apnac,''),coalesce(amnac,'')),'-',''),'[ ]+','') nac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') vac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                            from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef t0
                        )t1
                ) t2
                LEFT JOIN (select t3.*,regexp_replace(regexp_replace(concat_ws('',ifnull(fecnacimiento,''),ifnull(nombre_propietario,'')),'-',''),'[ ]+','') colmatch2
                            from sis_db_data_cln.pv_personas_fisicas_id t3
                            where t3.nombre_propietario is not null and t3.nombre_propietario!='' and t3.fecnacimiento is not null and t3.fecnacimiento !=''
                            ) t4
                ON t2.colmatch1 = t4.colmatch2
            ) tabla1
            group by pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,direcciones,telefonos
        ) tabla2
    )tabla3    
    order by cast(id as int)
);

invalidate metadata;


INSERT INTO sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh
    SELECT id,pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,telefonos,direcciones,
    case when vehiculos='VEHICULO:idveh:estatus_registro:placa:anio:factura:marca:motor:vin:linea:version:' then '' else vehiculos end vehiculos
    FROM(
        SELECT row_number() over (order by cast(pknac as int)) id,
        pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
        concat_ws(' | ',ifnull(telefonos,''),telefonospadron) telefonos, concat_ws(' | ',ifnull(direcciones,''),direccionespadron) direcciones, vehiculos
        FROM (
            select 
            pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,
            direcciones,
            group_concat(direccionespadron,' |DIREC VEHICULOS: ') direccionespadron,
            telefonos,
            ifnull(group_concat(telefonospadron,' | '),'') telefonospadron,
            group_concat(vehiculos,' | ') vehiculos
            from (
                select t2.pknac,t2.pkvac,t2.pkllv,t2.fcnacnac,t2.curpnac,t2.nomnac,t2.apnac,t2.amnac,t2.sexonac,t2.fcnacvac,t2.curpvac,t2.nomvac,t2.apvac,t2.amvac,t2.fcnacllv,t2.curpllv,t2.nomllv,t2.apllv,t2.amllv,t2.sexollv,
                concat_ws(' ',
                'ent:',regexp_replace(ifnull(cast(t4.entidad_federativa_propietario as string),''),'\\|',' '),'mun:',regexp_replace(ifnull(cast(t4.municipio_propietario as string),''),'\\|',' '),'col:',regexp_replace(ifnull(cast(t4.colonia as string),''),'\\|',' '),
                'calle:',regexp_replace(ifnull(cast(t4.calle as string),''),'\\|',' '),'cp:',regexp_replace(ifnull(cast(t4.codigo_postal as string),''),'\\|',' '),'numext:',regexp_replace(ifnull(cast(t4.numero_exterior as string),''),'\\|',' '),
                'numint:',regexp_replace(ifnull(cast(t4.numero_interior as string),''),'\\|',' ')) direccionespadron,            
                t2.direcciones,
                t2.telefonos,
                trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
                ifnull(cast(t4.telefono as string),''),'\\|',' '),'[^0-9]+',''),'^0$',''),'[ ]+','')) telefonospadron,              
                concat_ws('','VEHICULO:',
                        'idveh:',ifnull(cast(t4.id as string),''),'estatus_registro:',ifnull(cast(t4.estatus_registro as string),''),'placa:',ifnull(cast(t4.placa as string),''),'anio:',ifnull(cast(t4.modelo as string),''),
                        'factura:',ifnull(cast(t4.importe_factura as string),''),'marca:',ifnull(cast(t4.marca as string),''),'motor:',ifnull(cast(t4.numero_motor as string),''),
                        'vin:',ifnull(cast(t4.vin as string),''),'linea:',ifnull(cast(t4.linea as string),''),'version:',ifnull(cast(t4.version as string),''))
                 vehiculos
                from(
                    select t1.*,case
                        when (t1.nac='' and t1.vac='') then t1.llv 
                        when (t1.nac='' and t1.vac!='') then t1.vac 
                        else t1.nac end colmatch1
                        from (
                            select t0.*,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacnac,''),coalesce(nomnac,''),coalesce(apnac,''),coalesce(amnac,'')),'-',''),'[ ]+','') nac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') vac,
                            regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                            from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef t0
                        )t1
                ) t2
                LEFT JOIN (select t3.*,regexp_replace(regexp_replace(concat_ws('',ifnull(fecnacimiento,''),ifnull(nombre_propietario,'')),'-',''),'[ ]+','') colmatch2
                            from sis_db_data_cln.pv_personas_fisicas_id t3
                            where t3.nombre_propietario is not null and t3.nombre_propietario!='' and t3.fecnacimiento is not null and t3.fecnacimiento !=''
                            ) t4
                ON t2.colmatch1 = t4.colmatch2
            ) tabla1
            group by pknac,pkvac,pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,direcciones,telefonos
        ) tabla2
    )tabla3    
    order by cast(id as int);




select * from sis_db_data_cln.pv_personas_fisicas_id;
select * from sis_db_data_dev.stg_dom_veh;
select * from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh ;


select count(*) from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh ; --19,295,796



-------------------- PEGAR IDS MONGO


-- borro la tabla anterior
drop table sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh_idmongo purge;


CREATE TABLE IF NOT EXISTS sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh_idmongo as (
    select t2.idmongo,t1.*
    from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh t1
    left join (select * from sis_db_data_cln.objectidmongo) t2
    on cast(t1.id as int) = cast(t2.id as int)
    order by cast(t1.id as int)
);

INVALIDATE METADATA;

SELECT  * FROM sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh_idmongo ;
SELECT count(*) FROM sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef_veh_idmongo ; --19.295.796

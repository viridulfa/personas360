------------------------------------ ELIMINAR REGISTROS
-- ID PAPA CARINA 12012300
select * from rc_stg_def_5 where pkdef='12012300';
-- ID papas carina
select * from rc_stg_mat_4 where pkmat='7095829'; --carina
select * from rc_stg_mat_4 where pkmat='2913652'; --papas
-- ID MIO NACIMIENTOS y papás
select * from rc_stg_nac_7 where pknac='3606624'; --MÍO
select * from rc_stg_nac_7 where pknac='40749332'; --MAMÁ
select * from rc_stg_nac_7 where pknac='45552961';--PAPÁ
select * from rc_stg_nac_7 where pknac in ('6175223','1871361'); --HERMANO;

select * from rc_stg_nac_7 where titularnom='JOSE' and titularappat='ANGELES' and titularapmat='PEREZ';
select * from rc_stg_nac_7 where padrenom='ARMANDO' and padreappat='ANGELES' and padreapmat='CRUZ';
select * from rc_stg_nac_7 where padrenom='JOSE' and padreappat='ANGELES' and padreapmat='CRUZ';
select * from rc_stg_mat_4 where conyugue1nom='ARMANDO' and conyugue1appaterno='ANGELES' and conyugue1apmaterno='CRUZ';

SELECT * from sis_db_data_raw.vac_vacunas2 where nombre='GABRIELA VIRIDIANA' and paterno='ANGELES' and materno='PEREZ';



create table if not exists sis_db_data_cln.rc_stg_nac_7_aux as 
SELECT * FROM sis_db_data_cln.rc_stg_nac_7
WHERE pknac not in ('3606624','40749332','45552961','6175223','1871361');

INSERT INTO sis_db_data_cln.rc_stg_nac_7_aux SELECT * FROM sis_db_data_cln.rc_stg_nac_7
WHERE pknac not in ('3606624','40749332','45552961','6175223','1871361');

create table if not exists sis_db_data_cln.rc_stg_mat_4_aux as 
SELECT * FROM sis_db_data_cln.rc_stg_mat_4
WHERE pkmat not in ('7095829','2913652');

create table if not exists sis_db_data_cln.rc_stg_def_5_aux as 
SELECT * FROM sis_db_data_cln.rc_stg_def_5
WHERE pkdef not in ('12012300');

INSERT INTO sis_db_data_cln.rc_stg_def_5_aux SELECT * FROM sis_db_data_cln.rc_stg_def_5
WHERE pkdef not in ('12012300');

INVALIDATE METADATA;

SELECT count(*) FROM sis_db_data_cln.rc_stg_nac_7_aux; --16,138,612
SELECT count(*) FROM sis_db_data_cln.rc_stg_nac_7; --16,138,612

SELECT count(*) FROM sis_db_data_cln.rc_stg_def_5_aux; --5,734,092
SELECT count(*) FROM sis_db_data_cln.rc_stg_def_5; --5,734,092

SELECT count(*) FROM sis_db_data_cln.rc_stg_mat_4_aux; --3,748,387
SELECT count(*) FROM sis_db_data_cln.rc_stg_mat_4; --3,748,387

drop table sis_db_data_cln.rc_stg_mat_4 purge;
create table if not exists sis_db_data_cln.rc_stg_mat_4 as select * from sis_db_data_cln.rc_stg_mat_4_aux ;
insert into sis_db_data_cln.rc_stg_mat_4 select * from sis_db_data_cln.rc_stg_mat_4_aux ;
--drop table sis_db_data_cln.rc_stg_mat_4_aux purge;

drop table sis_db_data_cln.rc_stg_def_5 purge;
create table if not exists sis_db_data_cln.rc_stg_def_5 as select * from sis_db_data_cln.rc_stg_def_5_aux ;
insert into sis_db_data_cln.rc_stg_def_5 select * from sis_db_data_cln.rc_stg_def_5_aux ;
--drop table sis_db_data_cln.rc_stg_def_5_aux purge;

drop table sis_db_data_cln.rc_stg_nac_7 purge;
create table if not exists sis_db_data_cln.rc_stg_nac_7 as select * from sis_db_data_cln.rc_stg_nac_7_aux ;
insert into sis_db_data_cln.rc_stg_nac_7 select * from sis_db_data_cln.rc_stg_nac_7_aux ;
--drop table sis_db_data_cln.rc_stg_def_5_aux purge;



select count(*) from sis_db_data_cln.rc_stg_mat_4;--3,748,387




select sum(tot) from (
    select titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat,cast(tot as int) tot  from(    
        select titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat,count(*) tot from rc_stg_nac_7
        where padrenom!='' and padreappat!='' and padreapmat!='' and madrenom!='' and madreappat!='' and madreapmat!=''
        group by titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat
    ) t
)t2
where tot>1; --1,033,739

select tot from (
    select titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat,cast(tot as int) tot  from(    
        select titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat,count(*) tot from rc_stg_nac_7
        where padrenom!='' and padreappat!='' and padreapmat!='' and madrenom!='' and madreappat!='' and madreapmat!=''
        group by titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat
    ) t
)t2
where tot>1 order by tot desc;

select tot from (
    select titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat,tot from (
        select titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat,cast(tot as int) tot  from(    
            select titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat,count(*) tot from rc_stg_nac_7
            where padrenom!='' and padreappat!='' and padreapmat!='' and madrenom!='' and madreappat!='' and madreapmat!=''
            group by titularnom,titularappat,titularapmat,padrenom,padreappat,padreapmat,madrenom,madreappat,madreapmat
        ) t
    )t2
    where tot>1
)t3
order by tot desc;


select * from rc_stg_nac_7
where titularnom='IRVING DANIEL' and titularappat='AMADOR' and titularapmat = 'MENDEZ'
and padrenom='' and padreappat='' and padreapmat='' and madrenom='' and madreappat='' and madreapmat='';



------------ 1. JOIN VACUNAS LLAVE
select count(*)
from vc_stg_vac_2_7 t1
join llv_stg_usr_3 t2
on t2.curp=t1.curp; --933,286

select count(*)
from vc_stg_vac_2_7 t1
full join llv_stg_usr_3 t2
on t2.curp=t1.curp; --5,706,142


select t1.pkvac id_vac,t2.id id_llv,
COALESCE(t1.fcnac,t2.fcnac) fcnac,
COALESCE(t1.curp ,t2.curp) curp,COALESCE(t1.nom,t2.nom) nom,COALESCE(t1.appaterno,t2.appat) ap,COALESCE(t2.apmat,t1.apmaterno) am,t2.sexo
from vc_stg_vac_2_7 t1
full join llv_stg_usr_3 t2
on t2.curp=t1.curp
order by t1.pkvac;

select t1.pkvac id_vac,t2.id id_llv,t1.fcnac fcnacvac,t2.fcnac fcnaclvv,
t1.curp curpvac,t2.curp curpllv,t1.nom nomvac,t2.nom nomllv,t1.appaterno apvac,t2.appat apllv,t2.apmat amvac,t1.apmaterno amllv,t2.sexo sexollv
from vc_stg_vac_2_7 t1
full join llv_stg_usr_3 t2
on t2.curp=t1.curp
order by t1.pkvac;

create table if not exists sis_db_data_cln.join_stg_vac_llv as 
    select t1.pkvac id_vac,t2.id id_llv,
    COALESCE(t1.fcnac,t2.fcnac) fcnac,
    COALESCE(t1.curp ,t2.curp) curp,COALESCE(t1.nom,t2.nom) nom,COALESCE(t1.appaterno,t2.appat) ap,COALESCE(t2.apmat,t1.apmaterno) am,t2.sexo
    from vc_stg_vac_2_7 t1
    full join llv_stg_usr_3 t2
    on t2.curp = t1.curp
    order by t1.pkvac,t2.id;
    
create table if not exists sis_db_data_cln.join_stg_vac_llv as 
    select t1.pkvac id_vac,t2.id id_llv,t1.fcnac fcnacvac,t1.curp curpvac,t1.nom nomvac,t1.appaterno apvac,t1.apmaterno amvac,
                                        t2.fcnac fcnacllv,t2.curp curpllv,t2.nom nomllv,t2.appat apllv,t2.apmat amllv,t2.sexo sexollv
    from vc_stg_vac_2_7 t1
    full join llv_stg_usr_3 t2
    on t2.curp = t1.curp
    order by t1.pkvac;

select * from sis_db_data_cln.join_stg_vac_llv;
select count(*) from sis_db_data_cln.join_stg_vac_llv;

drop table sis_db_data_cln.join_stg_vac_llv purge;

INVALIDATE METADATA;

------------ 2. JOIN DEFUNCIONES NACIMIENTOS 
select count(*) from (
    select t1.pknac,t1.titularcurp curp_nac,t1.fcnac,t1.titularnom nom_nac,t1.titularappat ap_nac,t1.titularapmat am_nac,t1.titularsexo sexo_nac,t1.match_nac
    from (select pknac,titularcurp,fcnac,titularnom,titularappat,titularapmat,titularsexo,
            regexp_replace(concat_ws('',titularsexo,titularnom,titularappat,titularapmat),'[ ]+','') as match_nac
            from rc_stg_nac_7 ) t1
    where match_nac not in (select regexp_replace(concat_ws('',difuntosex,difuntonom,difuntoappat,difuntoapmat),'[ ]+','') as match_def
                from rc_stg_def_5
                group by match_def)
) t3; --13,879,747


select count(*) from (
    select t1.pknac,t1.fcnac,t1.titularcurp curp_nac,t1.titularnom nom_nac,t1.titularappat ap_nac,t1.titularapmat am_nac,t1.titularsexo sexo_nac,t1.match_nac
    from (select pknac,titularcurp,fcnac,titularnom,titularappat,titularapmat,titularsexo,
            regexp_replace(concat_ws('',titularsexo,titularnom,titularappat,titularapmat),'[ ]+','') as match_nac
            from rc_stg_nac_7 ) t1
    where match_nac in (select regexp_replace(concat_ws('',difuntosex,difuntonom,difuntoappat,difuntoapmat),'[ ]+','') as match_def
                from rc_stg_def_5
                group by match_def)
) t3; --2,258,865


create table if not exists sis_db_data_cln.join_stg_nac_def as 
    select t1.pknac,t1.fcnac fc_nac,t1.titularcurp curp_nac,t1.titularnom nom_nac,t1.titularappat ap_nac,t1.titularapmat am_nac,t1.titularsexo sexo_nac,t1.match_nac
    from (select pknac,titularcurp,fcnac,titularnom,titularappat,titularapmat,titularsexo,
            regexp_replace(concat_ws('',titularsexo,titularnom,titularappat,titularapmat),'[ ]+','') as match_nac
            from rc_stg_nac_7 ) t1
    where match_nac not in (select regexp_replace(concat_ws('',difuntosex,difuntonom,difuntoappat,difuntoapmat),'[ ]+','') as match_def
                from rc_stg_def_5
                group by match_def)
    order by t1.pknac;

select count(pknac) from sis_db_data_cln.join_stg_nac_def;--13,879,747
    
create table if not exists sis_db_data_cln.join_stg_nac_def as 
    select t1.pknac,t1.fcnac fcnacnac,t1.titularcurp curpnac,t1.titularnom nomnac,t1.titularappat apnac,t1.titularapmat amnac,t1.titularsexo sexonac
    from    (select pknac,titularcurp,fcnac,titularnom,titularappat,titularapmat,titularsexo,
            regexp_replace(concat_ws('',titularsexo,titularnom,titularappat,titularapmat),'[ ]+','') as match_nac
            from rc_stg_nac_7 ) t1
    where match_nac not in (select regexp_replace(concat_ws('',difuntosex,difuntonom,difuntoappat,difuntoapmat),'[ ]+','') as match_def
                            from rc_stg_def_5
                            group by match_def)
    order by t1.pknac;
    

select * from sis_db_data_cln.join_stg_nac_def;
select count(*) from sis_db_data_cln.join_stg_nac_def;--13,879,747

drop table sis_db_data_cln.join_stg_nac_def purge;


INVALIDATE METADATA;

------------ 3. JOIN (DEFUNCIONES NACIMIENTOS) vs (VACUNAS LLAVE) - CURP
select * from join_stg_nac_def;
select * from join_stg_vac_llv;

select count(*) from join_stg_nac_def; --13,879,747
select count(*) from join_stg_vac_llv; --5,706,142

select match_nac,count(*) from(
    select pknac,curpnac,fcnacnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as match_nac
    from join_stg_nac_def
) t
group by match_nac
having count(*) >1 order by count(*) desc;

select * from(
    select pknac,curp_nac,fc_nac,nom_nac,ap_nac,am_nac,sexo_nac,regexp_replace(regexp_replace(concat_ws('',fc_nac,curp_nac,nom_nac,ap_nac,am_nac),'[ ]+',''),'-','') as match_nac
    from join_stg_nac_def
) t
where match_nac='20070417AARE070417MDFLDMA4EMILYIXUALVAREZRODRIGUEZ';

select * from rc_stg_nac_7 where titularnom='EMILY IXU' and titularappat='ALVAREZ';
select * from rc_stg_nac_7 where titularcurp='AARE070417MDFLDMA4';
select * from join_stg_nac_def where nom='EMILY IXU' and ap='ALVAREZ';
select * from join_stg_vac_llv where nom='EMILY IXU' and ap='ALVAREZ';

-- CON CURP

-- con info vacunas 
-- JOIN
select count(*)
from (
    select pknac,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as match_nac
    from join_stg_nac_def) t1
join (select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,sexollv,
        regexp_replace(regexp_replace(concat_ws('',fcnacvac,curpvac,nomvac,apvac,amvac),'[ ]+',''),'-','') as match_llv
        from join_stg_vac_llv) t2
on t1.match_nac = t2.match_llv; --224,242

--FULL JOIN
select count(*)
from (
    select pknac,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as match_nac
    from join_stg_nac_def) t1
full join (select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,sexollv,
            regexp_replace(regexp_replace(concat_ws('',fcnacvac,curpvac,nomvac,apvac,amvac),'[ ]+',''),'-','') as match_llv
            from join_stg_vac_llv) t2
on t1.match_nac = t2.match_llv; --19,361,647

-- con info llve
-- JOIN
select count(*)
from (
    select pknac,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as match_nac
    from join_stg_nac_def) t1
join (select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,sexollv,
        regexp_replace(regexp_replace(concat_ws('',fcnacllv,curpllv,nomllv,apllv,amllv),'[ ]+',''),'-','') as match_llv
        from join_stg_vac_llv) t2
on t1.match_nac = t2.match_llv; --116,349

-- FULL JOIN
select count(*)
from (
    select pknac,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as match_nac
    from join_stg_nac_def) t1
full join (select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,sexollv,
            regexp_replace(regexp_replace(concat_ws('',fcnacllv,curpllv,nomllv,apllv,amllv),'[ ]+',''),'-','') as match_llv
            from join_stg_vac_llv) t2
on t1.match_nac = t2.match_llv; --19,469,543



-- Para armar la llave con datos de vacunas y si no tiene con llave
select case
        when t.nac='' then llv
        else t.nac end col
,llv,nac
from (
    select fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,
    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(curpvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') nac,
    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(curpllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
    from join_stg_vac_llv
) t
where t.llv='';

-- verifico el conteo
select count(*) from (
    select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,case when nac='' then llv else nac end colmatch
    from (
        select t.*,
        regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(curpvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') nac,
        regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(curpllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
        from join_stg_vac_llv t
    ) t2
)t3; --5,706,142


-- hago el join con nacimientos

SELECT COUNT(*) FROM(
    SELECT join1.*, join2.*
    FROM (
        select pknac,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as colmatch1
        from join_stg_nac_def) join1
    JOIN 
        (
            select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,case when nac='' then llv else nac end colmatch2
            from (
                    select t.*,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(curpvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') nac,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(curpllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                    from join_stg_vac_llv t
            ) t2
        ) join2
    ON join2.colmatch2 = join1.colmatch1
) final; --290,093


SELECT COUNT(*) FROM(
    SELECT pknac,id_vac pkvac,id_llv pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv
    FROM (
        select pknac,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as colmatch1
        from join_stg_nac_def) join1
    FULL OUTER JOIN 
        (
            select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,case when nac='' then llv else nac end colmatch2
            from (
                    select t.*,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(curpvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') nac,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(curpllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                    from join_stg_vac_llv t
            ) t2
        ) join2
    ON join2.colmatch2 = join1.colmatch1
) final; --19,295,796


CREATE TABLE IF NOT EXISTS sis_db_data_cln.join_stg_nac_def_vac_llv AS
    SELECT pknac,id_vac pkvac,id_llv pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv
    FROM (
        select pknac,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as colmatch1
        from join_stg_nac_def) join1
    FULL OUTER JOIN 
        (
            select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,case when nac='' then llv else nac end colmatch2
            from (
                    select t.*,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(curpvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') nac,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(curpllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                    from join_stg_vac_llv t
            ) t2
        ) join2
    ON join2.colmatch2 = join1.colmatch1;
    

select count(*) from join_stg_nac_def_vac_llv; --19,295,796
drop table sis_db_data_cln.join_stg_nac_def_vac_llv purge;
    
INVALIDATE METADATA;


--PRUEBAS

SELECT COUNT(*) FROM (
    SELECT pknac,id_vac pkvac,id_llv pkllv,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv
    FROM (
        select pknac,fcnacnac,curpnac,nomnac,apnac,amnac,sexonac,regexp_replace(regexp_replace(concat_ws('',fcnacnac,curpnac,nomnac,apnac,amnac),'[ ]+',''),'-','') as colmatch1
        from join_stg_nac_def) join1
    FULL OUTER JOIN 
        (
            select id_vac,id_llv,fcnacvac,curpvac,nomvac,apvac,amvac,fcnacllv,curpllv,nomllv,apllv,amllv,sexollv,case when nac='' then llv else nac end colmatch2
            from (
                    select t.*,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacvac,''),coalesce(curpvac,''),coalesce(nomvac,''),coalesce(apvac,''),coalesce(amvac,'')),'-',''),'[ ]+','') nac,
                    regexp_replace(regexp_replace(concat_ws('',coalesce(fcnacllv,''),coalesce(curpllv,''),coalesce(nomllv,''),coalesce(apllv,''),coalesce(amllv,'')),'-',''),'[ ]+','') llv
                    from join_stg_vac_llv t
            ) t2
        ) join2
    ON join2.colmatch2 = join1.colmatch1
) fin
where pknac is not null and pkvac is not null and pkllv is not null; --50,785 resultaron que están en nacimientos-vacunas-llave




------ PEGAR DOMICILIOS, TELEFONOS ETC
SELECT * FROM (
    SELECT concat_ws(' | ', concat_ws(' ','col:',isnull(t2.naccol,''), ' edo:',isnull(t2.nacedo,'')),
                          concat_ws(' ','ent:',isnull(t3.entidadmv,''),'mun:',isnull(t3.municipiomv,''),'cp:',isnull(t3.cpmv,'')),
                          concat_ws(' ','ent:',isnull(t3.entidad,''),'mun:',isnull(t3.municipio,''),'loc:',isnull(t3.localidad,''),'col:',isnull(t3.col,''),'cp:',isnull(t3.cp,''),'numext:',isnull(t3.numext,''),'numint:',isnull(t3.numint,''))
                    ) direcciones,t3.numext,t1.*
    FROM 
        (SELECT * FROM join_stg_nac_def_vac_llv
        WHERE pknac is not null and pkvac is not null and pkllv is not  null
        ) t1
    LEFT JOIN rc_stg_nac_7 t2
    on t1.pknac = t2.pknac
    LEFT JOIN vc_stg_vac_2_7 t3
    on t1.pkvac = t3.pkvac
    ORDER BY t1.pknac
    ) fin
where fin.numext is not null;

-- para enseñar a Carina
SELECT * FROM (
    --SELECT COUNT(*) FROM (
        SELECT t1.*,t3.numint,concat_ws(' | ',
                            concat_ws(' ','col:',isnull(t2.naccol,''), ' edo:',isnull(t2.nacedo,'')),
                            concat_ws(' ','ent:',isnull(t3.entidadmv,''),'mun:',isnull(t3.municipiomv,''),'cp:',isnull(t3.cpmv,'')),
                            concat_ws(' ','ent:',isnull(t3.entidad,''),'mun:',isnull(t3.municipio,''),'loc:',isnull(t3.localidad,''),'col:',isnull(t3.col,''),'cp:',isnull(t3.cp,''),'numext:',isnull(t3.numext,''),'numint:',isnull(t3.numint,''))
                            ) direcciones,
                        concat_ws(' | ',t3.tel1,t3.tel1mv,t3.tel2,t3.tel2mv) telefonos
        FROM 
            (SELECT * FROM join_stg_nac_def_vac_llv
            --WHERE pknac is not null and pkvac is not null and pkllv is not  null
            ) t1
        LEFT JOIN rc_stg_nac_7 t2
        ON t1.pknac = t2.pknac
        LEFT JOIN vc_stg_vac_2_7 t3
        ON t1.pkvac = t3.pkvac
        ORDER BY t1.pknac
    --) fin --19,295,796
) fin2
where fin2.numint is not null;


CREATE TABLE IF NOT EXISTS join_stg_nac_def_vac_llv_direc_telef AS
        SELECT t1.*,concat_ws('|',
                            concat_ws('','DIREC NACIMIENTOS: ','edo:',isnull(t2.nacedo,''),'col:',isnull(t2.naccol,'')),-- DIRECCIONES NACIMIENTOS
                            concat_ws('','DIREC1 VACUNAS: ','ent:',regexp_replace(isnull(t3.entidadmv,''),'\\|',''),'mun:',regexp_replace(isnull(t3.municipiomv,''),'\\|',''),'cp:',regexp_replace(isnull(t3.cpmv,''),'\\|','')),--DIRECCIONES VACUNAS
                            concat_ws('','DIREC2 VACUNAS: ','ent:',regexp_replace(isnull(t3.entidad,''),'\\|',''),'mun:',regexp_replace(isnull(t3.municipio,''),'\\|',''),'loc:',regexp_replace(isnull(t3.localidad,''),'\\|',''),'col:',regexp_replace(isnull(t3.col,''),'\\|',''),'cp:',regexp_replace(isnull(t3.cp,''),'\\|',''),'numext:',regexp_replace(isnull(t3.numext,''),'\\|',''),'numint:',regexp_replace(isnull(t3.numint,''),'\\|','')),
                            concat_ws('','DIREC LLVE: ','edo:',regexp_replace(isnull(t4.edo,''),'\\|',''),'mun:',regexp_replace(isnull(t4.mun,''),'\\|',''),'col:',regexp_replace(isnull(t4.col,''),'\\|',''),'cp:',regexp_replace(isnull(t4.cp,''),'\\|',''),'calle:',regexp_replace(isnull(t4.calle,''),'\\|',''),'numext:',isnull(t4.numext,''),'numint:',regexp_replace(isnull(t4.numint,''),'\\|',''))--DIRECCIONES LLAVE
                        ) direcciones,
                    concat_ws('|',t3.tel1,t3.tel1mv,t3.tel2,t3.tel2mv,regexp_replace(regexp_replace(regexp_replace(isnull(t4.tel,''),'\\|',''),'[^0-9]+',''),'[ ]+','')) telefonos
        FROM 
            (SELECT * FROM join_stg_nac_def_vac_llv
            --WHERE pknac is not null and pkvac is not null and pkllv is not  null
            ) t1
        LEFT JOIN rc_stg_nac_7 t2
        ON t1.pknac = t2.pknac
        LEFT JOIN vc_stg_vac_2_7 t3
        ON t1.pkvac = t3.pkvac
        LEFT JOIN llv_stg_usr_3 t4
        ON t1.pkllv = t4.id
        ORDER BY t1.pknac;
        
--select count(pknac) from sis_db_data_cln.join_stg_nac_def_vac_llv;     

SELECT * FROM (
    --SELECT COUNT(*) FROM (
            SELECT t1.*,concat_ws('|',
                                concat_ws('','DIREC NACIMIENTOS: ','edo:',isnull(t2.nacedo,''),'col:',isnull(t2.naccol,'')),-- DIRECCIONES NACIMIENTOS
                                concat_ws('','DIREC1 VACUNAS: ','ent:',regexp_replace(isnull(t3.entidadmv,''),'\\|',''),'mun:',regexp_replace(isnull(t3.municipiomv,''),'\\|',''),'cp:',regexp_replace(isnull(t3.cpmv,''),'\\|','')),--DIRECCIONES VACUNAS
                                concat_ws('','DIREC2 VACUNAS: ','ent:',regexp_replace(isnull(t3.entidad,''),'\\|',''),'mun:',regexp_replace(isnull(t3.municipio,''),'\\|',''),'loc:',regexp_replace(isnull(t3.localidad,''),'\\|',''),'col:',regexp_replace(isnull(t3.col,''),'\\|',''),'cp:',regexp_replace(isnull(t3.cp,''),'\\|',''),'numext:',regexp_replace(isnull(t3.numext,''),'\\|',''),'numint:',regexp_replace(isnull(t3.numint,''),'\\|','')),
                                concat_ws('','DIREC LLVE: ','edo:',regexp_replace(isnull(t4.edo,''),'\\|',''),'mun:',regexp_replace(isnull(t4.mun,''),'\\|',''),'col:',regexp_replace(isnull(t4.col,''),'\\|',''),'cp:',regexp_replace(isnull(t4.cp,''),'\\|',''),'calle:',regexp_replace(isnull(t4.calle,''),'\\|',''),'numext:',isnull(t4.numext,''),'numint:',regexp_replace(isnull(t4.numint,''),'\\|',''))--DIRECCIONES LLAVE
                            ) direcciones,
                        concat_ws('|',t3.tel1,t3.tel1mv,t3.tel2,t3.tel2mv,regexp_replace(regexp_replace(regexp_replace(isnull(t4.tel,''),'\\|',''),'[^0-9]+',''),'[ ]+','')) telefonos
            FROM 
                (SELECT * FROM join_stg_nac_def_vac_llv
                --WHERE pknac is not null and pkvac is not null and pkllv is not  null
                ) t1
            LEFT JOIN rc_stg_nac_7 t2
            ON t1.pknac = t2.pknac
            LEFT JOIN vc_stg_vac_2_7 t3
            ON t1.pkvac = t3.pkvac
            LEFT JOIN llv_stg_usr_3 t4
            ON t1.pkllv = t4.id
            ORDER BY t1.pknac
    --) conteo
where telefonos is not null 

--drop table join_stg_nac_def_vac_llv_direc_telef purge;
SELECT * from join_stg_nac_def_vac_llv_direc_telef where pkvac is not null and pkllv is not null and telefonos is not null;
SELECT COUNT(*) from join_stg_nac_def_vac_llv_direc_telef; --19,295,796


-- hacer mapeo de mongo
-- terminar el join (tercera fase) y luego unir con vehiculos con left join (cuarta fase). quinta fase meter la familia



invalidate metadata;









-- EJEMPLOS DEFUNCIONES

SELECT * FROM join_stg_nac_def;

SELECT match_def from (
    SELECT regexp_replace(concat_ws('',difuntosex,difuntonom,difuntoappat,difuntoapmat),'[ ]+','') as match_def
    FROM rc_stg_def_5
) t
WHERE match_def in (select regexp_replace(concat_ws('',titularsexo,titularnom,titularappat,titularapmat),'[ ]+','') as match_nac from rc_stg_nac_7);

select * from rc_stg_nac_7 where titularnom='PAULA' and titularappat='AQUILES' and titularapmat='OCHOA'; --2010-01-05
select * from rc_stg_def_5 where difuntonom like '%PAULA%' and difuntoappat='AQUILES' and difuntoapmat='OCHOA'; --2010-01-28
select * from rc_stg_nac_7 where titularnom='MARTIN CRISTOBAL' and titularappat='GARCIA' and titularapmat='LOPEZ'; --1987-04-18
select * from rc_stg_def_5 where difuntonom= 'MARTIN CRISTOBAL' and difuntoappat='GARCIA' and difuntoapmat='LOPEZ'; --2018-03-26
select * from rc_stg_nac_7 where titularnom='LUCIA' and titularappat='MARTINEZ' and titularapmat=''; --1947-09-03
select * from rc_stg_def_5 where difuntonom= 'LUCIA' and difuntoappat='MARTINEZ' and difuntoapmat=''; --2018-03-26
SELECT * from sis_db_data_raw.rc_defunciones where pk_defunciones='8642641';


INVALIDATE METADATA;


-- EJEMPLOS LLAVE VACUNAS
select * from llv_stg_usr_3;

SELECT t1.curp,t1.nom,t1.appat,t1.apmat,t1.fcnac,
t2.nom nomvac,t2.appaterno appatvac, t2.apmaterno apmatvac
FROM llv_stg_usr_3 t1
JOIN vc_stg_vac_2_7 t2
on t1.curp = t2.curp;




SELECT * FROM rc_matrimonios
where el_nombre='ERIC VAN' and el_ap_paterno='TREJO' and el_ap_materno='BECERRIL';


SELECT * FROM rc_matrimonios
where ella_nombre='LAURA' and ella_ap_paterno='VALDES' and ella_ap_materno='ALONSO';

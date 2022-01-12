

INVALIDATE METADATA;


SELECT * FROM join_stg_nac_def_vac_llv_direc_telef_veh_idmongo_fam;
SELECT count(*) FROM tmp_join_pers;--19,295,796

SELECT * FROM tmp_join_pers;--19,295,796


--array_rem_pol (ya)
--nombre_completo
select * from sis_db_data_dev.stg_dom_rem_pol;
select count(*) from sis_db_data_dev.stg_dom_rem_pol ; --82,104
select count(distinct id_pol) from sis_db_data_dev.stg_dom_rem_pol ; --82,104
select folio_iph,clave,nombre_completo,count(*)
from sis_db_data_dev.stg_dom_rem_pol
group by folio_iph,nombre_completo,clave
having count(*) >1; --0

--array_inv_nc (ya)
--nombre_completo + fc_nacimiento
select * from sis_db_data_dev.stg_dom_fgj_inv;

--array_ci_imp (ver como agregar exordio)
--nombre_completo + fc_nacimiento
select * from sis_db_data_dev.stg_dom_ci_imp;

--array_rem_det
--nombre_completo
SELECT * FROM sis_db_data_dev.stg_dom_rem_det;
SELECT COUNT(*) FROM sis_db_data_dev.stg_dom_rem_det; --60,274
SELECT COUNT(DISTINCT id_det) FROM sis_db_data_dev.stg_dom_rem_det; --60,274

--array_inv_placa
--placa
select * from sis_db_data_dev.stg_dom_fgj_inv; 






select nombre_completo from sis_db_data_dev.stg_dom_rem_pol
where nombre_completo rlike '[^A-Z ]+';

select * from (
    select nombre_completo,regexp_replace(regexp_replace(ifnull(upper(nombre_completo),''),'[^A-ZÑ ]+',''),'[ ]+','')
    from sis_db_data_dev.stg_dom_rem_pol
)t 
where t.nombre_completo rlike '[^A-ZÑ ]+';


------------ JOIN array_rem_pol
    
SELECT count(*) from (    
    SELECT * FROM sis_db_data_cln.tmp_join_pers t1
    LEFT JOIN (select regexp_replace(regexp_replace(ifnull(upper(nombre_completo),''),'[^A-ZÑ ]+',''),'[ ]+','') nombre_completo,
                group_concat(concat_ws('','idpol:',cast(id_pol as string),'folioiph:',regexp_replace(folio_iph,'\\|',''),'clave:',regexp_replace(clave,'\\|','')),'|') pol
                from sis_db_data_dev.stg_dom_rem_pol
                group by nombre_completo
                ) t2
    on t1.nc = t2.nombre_completo
) t; --19,295,796

SELECT * from (    
    SELECT t1.*,t2.remisiones_policias
    FROM sis_db_data_cln.tmp_join_pers t1
    LEFT JOIN (select regexp_replace(regexp_replace(ifnull(upper(nombre_completo),''),'[^A-ZÑ ]+',''),'[ ]+','') nombre_completo,
                group_concat(concat_ws('','idpol:',cast(id_pol as string),
                                        'folioiph:',regexp_replace(ifnull(folio_iph,''),'\\|',''),
                                        'clave:',regexp_replace(ifnull(clave,''),'\\|',''),
                                        'nombre:',regexp_replace(ifnull(nombre_completo,''),'\\|','')),'|') remisiones_policias
                from sis_db_data_dev.stg_dom_rem_pol
                group by nombre_completo
                ) t2
    on t1.nc = t2.nombre_completo
) t
where remisiones_policias is not null ;


select nombre_completo, count(*)
from sis_db_data_dev.stg_dom_rem_pol 
group by nombre_completo
HAVING count(*) >1;


------------ JOIN array_inv_nc

select * from sis_db_data_dev.stg_dom_fgj_inv;
    
SELECT * from ( 

    select pol from (
        SELECT t1.*,t2.pol
        FROM (select concat_ws('',nc,regexp_replace(ifnull(fcnacnac,''),'-','')) match1 from sis_db_data_cln.tmp_join_pers) t1
        LEFT JOIN (select concat_ws('',regexp_replace(regexp_replace(ifnull(upper(nombre_completo),''),'[^A-ZÑ ]+',''),'[ ]+',''),regexp_replace(ifnull(fc_nacimiento,''),'-','')) match2,
                    group_concat(concat_ws('','clv_involucrado:',regexp_replace(clv_involucrado,'\\|',''),
                                                'fc_captura:',regexp_replace(fc_captura,'\\|',''),
                                                'status_persona:',regexp_replace(cast(status_persona as string),'\\|',''),
                                                'calidad_involucrado:',regexp_replace(calidad_involucrado,'\\|',''),
                                                'clase_involucrado:',regexp_replace(clase_involucrado,'\\|',''),
                                                'nombre_completo:',regexp_replace(nombre_completo,'\\|',''),
                                                'edad:',regexp_replace(cast(edad as string),'\\|',''),
                                                'sexo:',regexp_replace(sexo,'\\|','')
                                            ),'|') pol
                    from sis_db_data_dev.stg_dom_fgj_inv
                    group by match2
                    ) t2
        on match1 = match2
    )t3
    WHERE t3.pol is not null;

) t; --19,295,796

select count(*) from sis_db_data_dev.stg_dom_fgj_inv;--5,124,913
select count(distinct clv_involucrado) from sis_db_data_dev.stg_dom_fgj_inv;--5,124,913



------------ JOIN array_ci_imp

select * from sis_db_data_dev.stg_dom_ci_imp_hist;
select * from sis_db_data_dev.stg_dom_ci_imp;
select * from sis_db_data_dev.stg_dom_ci;

---- imputados con historico
    /*    dn = { 'id_imp_hist': dj.get('id_imp_hist'),
               #'id_ci': dj.get('id_ci'),
               #'id_imp': dj.get('id_imp'),
               'fc_actualizacion': dj.get('fc_actualizacion'),
               'nivel_binario': dj.get('nivel_binario'),
               'status_binario': dj.get('status_binario'),
               'libertad': dj.get('libertad'),
               'incompetencia': dj.get('incompetencia'),
               'mecanismo_alternativo': dj.get('mecanismo_alternativo'),
               'judicializacion': dj.get('judicializacion'),
               'orden_aprehension': dj.get('orden_aprehension'),
               'legal_detencion': dj.get('legal_detencion'),
               'vincula_proceso': dj.get('vincula_proceso'),
               'pp_oficiosa': dj.get('pp_oficiosa'),
               'pp_justificada': dj.get('pp_justificada'),
               'otra_medida_cautelar': dj.get('otra_medida_cautelar'),
               'sin_medida_cautelar': dj.get('sin_medida_cautelar'),
               'suspension_condicional': dj.get('suspension_condicional'),
               'acuerdo_reparatorio': dj.get('acuerdo_reparatorio') }*/

select count(distinct id_imp) from(
    select id_imp, group_concat(concat_ws('','id_imp_hist:',cast(id_imp_hist as string),
                                            'id_ci:',cast(id_ci as string),
                                            'fc_actualizacion:',cast(fc_actualizacion as string),
                                            'nivel_binario:',cast(nivel_binario as string),
                                            'status_binario',cast(status_binario as string),
                                            'libertad:',cast(libertad as string),
                                            'incompetencia:',cast(incompetencia as string),
                                            'mecanismo_alternativo',cast(mecanismo_alternativo as string),
                                            'judicializacion:',cast(judicializacion as string),
                                            'orden_aprehension:',cast(orden_aprehension as string),
                                            'legal_detencion:', cast(legal_detencion as string),
                                            'vincula_proceso:',cast(vincula_proceso as string),
                                            'pp_oficiosa:',cast(pp_oficiosa as string),
                                            'pp_justificada:',cast(pp_justificada as string),
                                            'otra_medida_cautelar:',cast(otra_medida_cautelar as string),
                                            'sin_medida_cautelar:',cast(sin_medida_cautelar as string),
                                            'suspension_condicional:',cast(suspension_condicional as string),
                                            'acuerdo_reparatorio:',cast(acuerdo_reparatorio as string)),'|')
    from sis_db_data_dev.stg_dom_ci_imp_hist
    where id_imp is not null
    group by id_imp
) t
; --110222

---- imputados
        /*dn = { 'id_imp': dj.get('id_imp'),
               #'id_ci': dj.get('id_ci'),
               'id_seg': dj.get('id_seg'),
               'id_persona': dj.get('id_persona'),
               'nombre_completo': dj.get('nombre_completo'),
               'nombre_elems': {
                   'nom': dj.get('nom'),
                   'ap': dj.get('ap'),
                   'am': dj.get('am') },
               'fc_nacimiento': dj.get('fc_nacimiento'),
               'edad': dj.get('edad'),
               'sexo': dj.get('sexo'),
               'nacionalidad': dj.get('nacionalidad'),
               'corporacion_remite': dj.get('corporacion_remite'),
               'nivel_binario': dj.get('nivel_binario'),
               'status_binario': dj.get('status_binario'),
               'orden_aprehension': dj.get('orden_aprehension'),
               'motivo_libertad': dj.get('motivo_libertad'),
               'observaciones_dgpec_1': dj.get('observaciones_dgpec_1'),
               'observaciones_dgpec_2': dj.get('observaciones_dgpec_2') }*/
               
select count(distinct id_imp) from (               
    select id_ci,id_imp, group_concat(concat_ws('',
                                            'id_seg:',ifnull(cast(id_seg as string),''),
                                            'id_persona:',ifnull(cast(id_persona as string),''),
                                            'nombre_completo:',ifnull(cast(nombre_completo as string),''),
                                            'nom',ifnull(cast(nom as string),''),
                                            'ap:',ifnull(cast(ap as string),''),
                                            'am:',ifnull(cast(am as string),''),
                                            'fc_nacimiento',ifnull(cast(fc_nacimiento as string),''),
                                            'edad:',ifnull(cast(edad as string),''),
                                            'sexo:',ifnull(cast(sexo as string),''),
                                            'nacionalidad:', ifnull(cast(nacionalidad as string),''),
                                            'corporacion_remite:',ifnull(cast(corporacion_remite as string),''),
                                            'nivel_binario:',ifnull(cast(nivel_binario as string),''),
                                            'status_binario:',ifnull(cast(status_binario as string),''),
                                            'orden_aprehension:',ifnull(cast(orden_aprehension as string),''),
                                            'motivo_libertad:',ifnull(cast(motivo_libertad as string),''),
                                            'observaciones_dgpec_1:',ifnull(cast(observaciones_dgpec_1 as string),''),
                                            'observaciones_dgpec_2:',ifnull(cast(observaciones_dgpec_2 as string),'')),'|')
    from sis_db_data_dev.stg_dom_ci_imp
    where id_ci is not null and id_imp is not null
    group by id_ci, id_imp
) t; --110222


--- join imputados e imputados con historico


select t1.id_ci,t1.id_imp,t1.status,t2.status_historico
from (select id_ci,id_imp, group_concat(concat_ws('',
                                            'id_seg:',ifnull(cast(id_seg as string),''),
                                            'id_persona:',ifnull(cast(id_persona as string),''),
                                            'nombre_completo:',ifnull(cast(nombre_completo as string),''),
                                            'nom',ifnull(cast(nom as string),''),
                                            'ap:',ifnull(cast(ap as string),''),
                                            'am:',ifnull(cast(am as string),''),
                                            'fc_nacimiento',ifnull(cast(fc_nacimiento as string),''),
                                            'edad:',ifnull(cast(edad as string),''),
                                            'sexo:',ifnull(cast(sexo as string),''),
                                            'nacionalidad:', ifnull(cast(nacionalidad as string),''),
                                            'corporacion_remite:',ifnull(cast(corporacion_remite as string),''),
                                            'nivel_binario:',ifnull(cast(nivel_binario as string),''),
                                            'status_binario:',ifnull(cast(status_binario as string),''),
                                            'orden_aprehension:',ifnull(cast(orden_aprehension as string),''),
                                            'motivo_libertad:',ifnull(cast(motivo_libertad as string),''),
                                            'observaciones_dgpec_1:',ifnull(cast(observaciones_dgpec_1 as string),''),
                                            'observaciones_dgpec_2:',ifnull(cast(observaciones_dgpec_2 as string),'')),'|') status
    from sis_db_data_dev.stg_dom_ci_imp
    where id_ci is not null and id_imp is not null
    group by id_ci, id_imp) t1
left join 
    (select id_imp, group_concat(concat_ws('','id_imp_hist:',cast(id_imp_hist as string),
                                            'id_ci:',cast(id_ci as string),
                                            'fc_actualizacion:',cast(fc_actualizacion as string),
                                            'nivel_binario:',cast(nivel_binario as string),
                                            'status_binario',cast(status_binario as string),
                                            'libertad:',cast(libertad as string),
                                            'incompetencia:',cast(incompetencia as string),
                                            'mecanismo_alternativo',cast(mecanismo_alternativo as string),
                                            'judicializacion:',cast(judicializacion as string),
                                            'orden_aprehension:',cast(orden_aprehension as string),
                                            'legal_detencion:', cast(legal_detencion as string),
                                            'vincula_proceso:',cast(vincula_proceso as string),
                                            'pp_oficiosa:',cast(pp_oficiosa as string),
                                            'pp_justificada:',cast(pp_justificada as string),
                                            'otra_medida_cautelar:',cast(otra_medida_cautelar as string),
                                            'sin_medida_cautelar:',cast(sin_medida_cautelar as string),
                                            'suspension_condicional:',cast(suspension_condicional as string),
                                            'acuerdo_reparatorio:',cast(acuerdo_reparatorio as string)),'|') status_historico
    from sis_db_data_dev.stg_dom_ci_imp_hist
    where id_imp is not null
    group by id_imp) t2
on t1.id_imp=t2.id_imp;



select * from sis_db_data_dev.stg_dom_ci_imp_hist where id_imp=76995; --CI-FIVC/VC3/UI1CD/001778/102020
select * from sis_db_data_dev.stg_dom_ci_imp where id_imp=76995;


select * from sis_db_data_dev.stg_dom_ci;


     /*   dn = { 'id_ci': dj.get('id_ci'),
               'id_dgpec': int(dj.get('id_dgpec')) if dj.get('id_dgpec') is not None else None,
               'id_dgtsi': dj.get('id_dgtsi'),
               'ci': dj.get('ci'),
               'ci_corto': dj.get('ci_corto'),
               'ci_corto_elems': {
                   'fsc': dj.get('fsc'),
                   'agn': dj.get('agn') },
               'fc_inicio': dj.get('fc_inicio'),
               'fc_elems': {
                   'year_num': dj.get('year_num'),
                   'month_num': dj.get('month_num'),
                   'day_num': dj.get('day_num'),
                   'week_num': dj.get('week_num'),
                   'weekday_num': dj.get('weekday_num') },
               'tm_inicio': dj.get('tm_inicio'),
               'fc_hechos': dj.get('fc_hechos'),
               'tm_hechos': dj.get('tm_hechos'),
               'fc_creacion': dj.get('fc_creacion'),
               'delito': [{
                   'id_delito': dj.get('id_delito'),
                   'delito': dj.get('delito'),
                   'delito_2': dj.get('delito_2'),
                   'modalidad': dj.get('modalidad'),
                   'delito_categoria': dj.get('delito_categoria'),
                   'id_delito_tablero': dj.get('id_delito_tablero'),
                   'delito_tablero': dj.get('delito_tablero'),
                   'lugar_comision': dj.get('lugar_comision'),
                   'num_victimas': dj.get('num_victimas'), #---
                   }],
               'inicio': {
                   'id_fsc_': dj.get('id_fsc'),
                   'fsc_inicio': dj.get('fsc_inicio'),
                   'fsc_inicio_sigla': dj.get('fsc_inicio_sigla'),
                   'id_agn': dj.get('id_agn'),
                   'agn_inicio': dj.get('agn_inicio'),
                   'id_unidad_investiga': dj.get('id_unidad_investiga'),
                   'unidad_investiga': dj.get('unidad_investiga'),
                   'subprocuraduria': dj.get('subprocuraduria') },
               'domicilio_hechos': {
                   'calle': dj.get('calle'),
                   'calle_2': dj.get('calle_2'),
                   'col': dj.get('col'), #---
                   'ct': dj.get('ct'), #---
                   'alc': dj.get('alc') },
               'geo': {
                   'lat': dj.get('lat'),
                   'long': dj.get('long') },
               'version': dj.get('version'),
               'exordio': dj.get('exordio') }
        if dj.get('long') is not None and dj.get('lat') is not None:
            dn['geo']['type'] = 'Point'
            dn['geo']['coordinates'] = [dj.get('long'), dj.get('lat')]
        if idci is not None and idci in array_imp:
            #print (idci)
            dn['imputados'] = array_imp[idci] # 1234: [{},{},{},{}]
               */


select id_ci,id_dgpec,id_dgtsi,ci,ci_corto,fsc,agn,fc_inicio,tm_inicio,fc_hechos,tm_hechos,fc_creacion,delito,delito_2,modalidad,delito_categoria,
id_delito_tablero,delito_tablero,lugar_comision,num_victimas,fsc_inicio,fsc_inicio_sigla,id_agn,agn_inicio,unidad_investiga,subprocuraduria,
calle,calle_2,col,ct,alc,lat,long,version,exordio
from sis_db_data_dev.stg_dom_ci;

select count(*) from sis_db_data_dev.stg_dom_ci;--1,264,421
select count(distinct id_ci) from sis_db_data_dev.stg_dom_ci;--1,264,421


select count(distinct id_ci) from (
    select id_ci, group_concat(concat_ws('','id_imp:',cast(id_imp as string),'status:',status,'status_historico:',status_historico),'¬')
    from(
        select t1.id_ci,t1.id_imp,t1.status,t2.status_historico
        from (select id_ci,id_imp, group_concat(concat_ws('',
                                                    'id_seg:',ifnull(cast(id_seg as string),''),
                                                    'id_persona:',ifnull(cast(id_persona as string),''),
                                                    'nombre_completo:',ifnull(cast(nombre_completo as string),''),
                                                    'nom',ifnull(cast(nom as string),''),
                                                    'ap:',ifnull(cast(ap as string),''),
                                                    'am:',ifnull(cast(am as string),''),
                                                    'fc_nacimiento',ifnull(cast(fc_nacimiento as string),''),
                                                    'edad:',ifnull(cast(edad as string),''),
                                                    'sexo:',ifnull(cast(sexo as string),''),
                                                    'nacionalidad:', ifnull(cast(nacionalidad as string),''),
                                                    'corporacion_remite:',ifnull(cast(corporacion_remite as string),''),
                                                    'nivel_binario:',ifnull(cast(nivel_binario as string),''),
                                                    'status_binario:',ifnull(cast(status_binario as string),''),
                                                    'orden_aprehension:',ifnull(cast(orden_aprehension as string),''),
                                                    'motivo_libertad:',ifnull(cast(motivo_libertad as string),''),
                                                    'observaciones_dgpec_1:',ifnull(cast(observaciones_dgpec_1 as string),''),
                                                    'observaciones_dgpec_2:',ifnull(cast(observaciones_dgpec_2 as string),'')),'|') status
            from sis_db_data_dev.stg_dom_ci_imp
            where id_ci is not null and id_imp is not null
            group by id_ci, id_imp) t1
        left join 
            (select id_imp, group_concat(concat_ws('','id_imp_hist:',cast(id_imp_hist as string),
                                                    'id_ci:',cast(id_ci as string),
                                                    'fc_actualizacion:',cast(fc_actualizacion as string),
                                                    'nivel_binario:',cast(nivel_binario as string),
                                                    'status_binario',cast(status_binario as string),
                                                    'libertad:',cast(libertad as string),
                                                    'incompetencia:',cast(incompetencia as string),
                                                    'mecanismo_alternativo',cast(mecanismo_alternativo as string),
                                                    'judicializacion:',cast(judicializacion as string),
                                                    'orden_aprehension:',cast(orden_aprehension as string),
                                                    'legal_detencion:', cast(legal_detencion as string),
                                                    'vincula_proceso:',cast(vincula_proceso as string),
                                                    'pp_oficiosa:',cast(pp_oficiosa as string),
                                                    'pp_justificada:',cast(pp_justificada as string),
                                                    'otra_medida_cautelar:',cast(otra_medida_cautelar as string),
                                                    'sin_medida_cautelar:',cast(sin_medida_cautelar as string),
                                                    'suspension_condicional:',cast(suspension_condicional as string),
                                                    'acuerdo_reparatorio:',cast(acuerdo_reparatorio as string)),'|') status_historico
            from sis_db_data_dev.stg_dom_ci_imp_hist
            where id_imp is not null
            group by id_imp) t2
        on t1.id_imp=t2.id_imp
    ) t3
    group by id_ci
) t4; --84,850

select * from(
    select t_1.id_ci,id_dgpec,id_dgtsi,ci,ci_corto,fsc,agn,fc_inicio,tm_inicio,fc_hechos,tm_hechos,fc_creacion,delito,delito_2,modalidad,delito_categoria,id_delito_tablero,
    delito_tablero,lugar_comision,num_victimas,fsc_inicio,fsc_inicio_sigla,id_agn,agn_inicio,unidad_investiga,subprocuraduria,calle,calle_2,col,ct,alc,lat,long,version,exordio,t_2.historico_imputados
    from sis_db_data_dev.stg_dom_ci t_1
    left join 
    (select id_ci, group_concat(concat_ws('','id_imp:',cast(id_imp as string),'status:',status,'status_historico:',status_historico),'¬') historico_imputados
        from(
            select t1.id_ci,t1.id_imp,t1.status,t2.status_historico
            from (select id_ci,id_imp, group_concat(concat_ws('',
                                                        'id_seg:',ifnull(cast(id_seg as string),''),
                                                        'id_persona:',ifnull(cast(id_persona as string),''),
                                                        'nombre_completo:',ifnull(cast(nombre_completo as string),''),
                                                        'nom:',ifnull(cast(nom as string),''),
                                                        'ap:',ifnull(cast(ap as string),''),
                                                        'am:',ifnull(cast(am as string),''),
                                                        'fc_nacimiento:',ifnull(cast(fc_nacimiento as string),''),
                                                        'edad:',ifnull(cast(edad as string),''),
                                                        'sexo:',ifnull(cast(sexo as string),''),
                                                        'nacionalidad:', ifnull(cast(nacionalidad as string),''),
                                                        'corporacion_remite:',ifnull(cast(corporacion_remite as string),''),
                                                        'nivel_binario:',ifnull(cast(nivel_binario as string),''),
                                                        'status_binario:',ifnull(cast(status_binario as string),''),
                                                        'orden_aprehension:',ifnull(cast(orden_aprehension as string),''),
                                                        'motivo_libertad:',ifnull(cast(motivo_libertad as string),''),
                                                        'observaciones_dgpec_1:',ifnull(cast(observaciones_dgpec_1 as string),''),
                                                        'observaciones_dgpec_2:',ifnull(cast(observaciones_dgpec_2 as string),'')),'|') status
                from sis_db_data_dev.stg_dom_ci_imp
                where id_ci is not null and id_imp is not null
                group by id_ci, id_imp) t1
            left join 
                (select id_imp, group_concat(concat_ws('','id_imp_hist:',cast(id_imp_hist as string),
                                                        'id_ci:',cast(id_ci as string),
                                                        'fc_actualizacion:',cast(fc_actualizacion as string),
                                                        'nivel_binario:',cast(nivel_binario as string),
                                                        'status_binario:',cast(status_binario as string),
                                                        'libertad:',cast(libertad as string),
                                                        'incompetencia:',cast(incompetencia as string),
                                                        'mecanismo_alternativo:',cast(mecanismo_alternativo as string),
                                                        'judicializacion:',cast(judicializacion as string),
                                                        'orden_aprehension:',cast(orden_aprehension as string),
                                                        'legal_detencion:', cast(legal_detencion as string),
                                                        'vincula_proceso:',cast(vincula_proceso as string),
                                                        'pp_oficiosa:',cast(pp_oficiosa as string),
                                                        'pp_justificada:',cast(pp_justificada as string),
                                                        'otra_medida_cautelar:',cast(otra_medida_cautelar as string),
                                                        'sin_medida_cautelar:',cast(sin_medida_cautelar as string),
                                                        'suspension_condicional:',cast(suspension_condicional as string),
                                                        'acuerdo_reparatorio:',cast(acuerdo_reparatorio as string)),'|') status_historico
                from sis_db_data_dev.stg_dom_ci_imp_hist
                where id_imp is not null
                group by id_imp) t2
            on t1.id_imp=t2.id_imp
        ) t3
        group by id_ci) t_2
    on t_1.id_ci = t_2.id_ci
    where t_1.id_ci is not null
) final
where historico_imputados is not null;






    
SELECT * from ( 

    select cis from (
        SELECT t1.*,t2.cis
        FROM (select concat_ws('',nc,regexp_replace(ifnull(fcnacnac,''),'-','')) match1 from sis_db_data_cln.tmp_join_pers) t1
        LEFT JOIN (select concat_ws('',regexp_replace(regexp_replace(ifnull(upper(nombre_completo),''),'[^A-ZÑ ]+',''),'[ ]+',''),regexp_replace(ifnull(fc_nacimiento,''),'-','')) match2,
                    group_concat(concat_ws('','id_seg:',regexp_replace(cast(id_seg as string),'\\|',''),
                                                'nombre_completo:',regexp_replace(nombre_completo,'\\|',''),
                                                'edad:',regexp_replace(cast(edad as string),'\\|',''),
                                                'sexo:',regexp_replace(sexo,'\\|',''),
                                                'status_binario:',regexp_replace(status_binario,'\\|',''),
                                                'id_ci:',regexp_replace(cast(id_ci as string),'\\|',''),
                                                'ci_corto:',regexp_replace(ci_corto,'\\|','')
                                            ),'|') cis
                    from sis_db_data_dev.stg_dom_ci_imp
                    group by match2
                    ) t2
        on match1 = match2
    )t3
    WHERE t3.cis is not null

) t; 

select count(*) from sis_db_data_dev.stg_dom_ci_imp;--110,222
select count(distinct ci_corto) from sis_db_data_dev.stg_dom_ci_imp;--

select ci_corto, count(*) from sis_db_data_dev.stg_dom_ci_imp group by ci_corto;

select * from sis_db_data_dev.stg_dom_ci_imp where ci_corto='CI-FMH/MH5/UI2CD/000333/032019';








select * from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef
where nomnac='VICTORIA IVANA' or nomllv='VICTORIA IVANA' or nomvac='VICTORIA IVANA'




select * from sis_db_data_cln.rc_stg_nac_1
where titular_nombre='VICTORIA IVANA' --46354908

select * from sis_db_data_cln.rc_stg_def_5
where difuntonom='VICTORIA IVANA'


select * from sis_db_data_cln.join_stg_nac_def_vac_llv_direc_telef
where apnac='ROJAS' and amnac='AGUILAR' and nomnac LIKE '%VICTORIA%'


select * from sis_db_data_raw.rc_nacimientos
where pk_nacimientos='68584770'


OMAR ARTURO ROJAS ALVARADO

SELECT * from sis_db_data_raw.rc_matrimonios
where el_ap_paterno='ROJAS' and el_ap_materno='ALVARADO' and el_nombre='OMAR ARTURO'




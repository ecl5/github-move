------------------------------------------------
--APP SMARTPROSPECT FACTOTAL
--Creacion: 2019-11-07
--Owner: Jonathan Nuñez
------------------------------------------------

--CREAMOS EL BACKUP
drop table ${db}.cl_smartprospect_factotal_sf_bkp;
create table ${db}.cl_smartprospect_factotal_sf_bkp_20200707 stored as parquet as 
select * from ${db}.cl_smartprospect_factotal_sf;
--------------------
--Normalizamos datos con el ultimo ARCHIVE
--------------------
DROP TABLE ${db}.cl_smartprospect_factotal_sf ;

create table ${db}.cl_smartprospect_factotal_sf stored as parquet as 
SELECT rut,rut_ind_pj,rut_num,nombre,nombregrupo,comuna,region,sucursaleje,
zonaeje,nombreejecutivo,idejecutivo ,ind_pj,cant_emp_consultan,monto_moras_bed,cant_doc_bed,cant_acre_bed,cant_socios,ind_emp_creada_un_dia,monto_protesto,
cant_acre_protesto,cant_doc_protesto,clean2,tamano_empresa,rango_facturacion,fecha_inicio_act,sumc,monto_icom,cant_icom,
consultas_sbif,consultas_no_sbif,tipo_vehi,ind_propyme,avaluo_bbrr,region_efx,comuna_efx,rubro,sub_rubro,ind_inicio_act,ind_fin_act,
n_documento_infra,n_documento_multa,archive,
    nvl2(comuna, initcap(comuna), "Sin información") as comuna_norm,
    nvl2(region, initcap(region), "Sin información") as region_norm,
    nvl2(sucursaleje, initcap(sucursaleje), "Sin información") as sucursaleje_norm,
    nvl2(zonaeje, initcap(zonaeje), "Sin información") as zonaeje_norm,
    nvl2(nombreejecutivo, initcap(nombreejecutivo), "Sin información") as nombreejecutivo_norm,
    case
        when ind_pj = 0 then 'No'
        when ind_pj = 1 then 'Si'
        else 'Sin información'
    end as ind_pj_norm,
    case
        when clean2 = 0 then 'Tiene deudas'
        when clean2 = 1 then 'No tiene deudas'
        else 'Tiene deudas'
    end as clean2_norm,
    nvl2(tamano_empresa, initcap(tamano_empresa),"Sin información") as tamano_empresa_norm,
    nvl2(rango_facturacion, initcap(rango_facturacion),"Sin información") as rango_facturacion_norm,
    --nvl2(tipo_vehi, initcap(tipo_vehi),"Sin información") as tipo_vehi_norm,
    nvl2(region_efx, initcap(region_efx),"Sin información") as region_efx_norm,
    nvl2(comuna_efx, initcap(comuna_efx),"Sin información") as comuna_efx_norm,
    nvl2(rubro, initcap(rubro),"Sin información") as rubro_norm,
    nvl2(sub_rubro, initcap(sub_rubro),"Sin información") as sub_rubro_norm,
    nvl2(ind_propyme, initcap(ind_propyme),'Sin información') as ind_propyme_norm,
    case
        when ind_inicio_act = "N" then "No" 
        when ind_inicio_act = "S" then "Si"
        else "Sin información"
    end as ind_inicio_act_norm,
    case
        when ind_fin_act = "N" then "No" 
        when ind_fin_act = "S" then "Si"
        else "Sin información"
    end as ind_fin_act_norm,
    case
        when tipo_vehi = "SIN VEHICULO" and avaluo_bbrr = 0 then "Sin bienes"
        when tipo_vehi = "SIN VEHICULO" and avaluo_bbrr != 0 then "Sólo BBRR"
        when tipo_vehi != "SIN VEHICULO" and avaluo_bbrr = 0 then 'Sólo Vehículo'
        when tipo_vehi != "SIN VEHICULO" and avaluo_bbrr != 0 then 'BBRR y Vehículos'
        else 'Sin información'
    end as tipo_patrimonio,
    case
        when rut_ind_pj = 1 then "PJ Cliente Factotal"
        when rut_ind_pj = 0 then "PN Cliente Factotal"
        else "Sin información"
    end as rut_ind_pj_norm,
    case
        when rut_ind_pj is null and ind_pj = 0 then 'Prospecto PN'
        when rut_ind_pj is null and ind_pj = 1 then 'Prospecto PJ'
        when rut_ind_pj = 0 and ind_pj = 0 then 'Cliente PN'
        when rut_ind_pj = 1 and ind_pj = 1 then 'Cliente PJ'
        else 'Raro'
    end as tipo_prospecto,
    monto_protesto + monto_icom + monto_moras_bed as monto_publicaciones,
    nvl2(tipo_vehi, initcap(tipo_vehi),'Sin vehículo') as tipo_vehi_norm,
    case
        when monto_protesto + monto_icom + monto_moras_bed = 0 then '0'
        when monto_protesto + monto_icom + monto_moras_bed > 0 and monto_protesto + monto_icom + monto_moras_bed <= 40000 then '>0 - <=40000'
        when monto_protesto + monto_icom + monto_moras_bed > 40000 and monto_protesto + monto_icom + monto_moras_bed <= 90000 then '>40000 - <=90000'
        when monto_protesto + monto_icom + monto_moras_bed > 90000 and monto_protesto + monto_icom + monto_moras_bed <= 150000 then '>90000 - <=150000'
        when monto_protesto + monto_icom + monto_moras_bed > 150000 and monto_protesto + monto_icom + monto_moras_bed <= 300000 then '>150000 - <=300000'
        when monto_protesto + monto_icom + monto_moras_bed > 300000 and monto_protesto + monto_icom + monto_moras_bed <= 500000 then '>300000 - <=500000'
        when monto_protesto + monto_icom + monto_moras_bed > 500000 and monto_protesto + monto_icom + monto_moras_bed <= 1000000 then '>500000 - <=1000000'
        when monto_protesto + monto_icom + monto_moras_bed > 1000000 and monto_protesto + monto_icom + monto_moras_bed <= 1500000 then '>1000000 - <=1500000'
        when monto_protesto + monto_icom + monto_moras_bed > 1500000 and monto_protesto + monto_icom + monto_moras_bed <= 3000000 then '>1500000 - <=3000000'
        when monto_protesto + monto_icom + monto_moras_bed > 3000000 and monto_protesto + monto_icom + monto_moras_bed <= 5000000 then '>3000000 - <=5000000'
        when monto_protesto + monto_icom + monto_moras_bed > 5000000 and monto_protesto + monto_icom + monto_moras_bed <= 25000000 then '>5000000 - <=25000000'
        when monto_protesto + monto_icom + monto_moras_bed > 25000000 then '>25000000'
        else 'Sin información'
    end as monto_publicaciones_binned,
    case
        when cant_emp_consultan = 0 then '0' 
        when cant_emp_consultan > 0 and cant_emp_consultan <= 1 then '0 - 1'
        when cant_emp_consultan > 1 and cant_emp_consultan <= 3 then '2 - 3'
        when cant_emp_consultan > 3 and cant_emp_consultan <= 5 then '4 - 5'
        when cant_emp_consultan > 6 and cant_emp_consultan <= 9 then '6 - 9'
        when cant_emp_consultan > 9 then '>9'
        else '0'
    end as cant_emp_consultan_binned,
    case
        when cant_consultas >= 0 then cant_consultas
        else 0
    end as cant_consultas,
    case
        when cant_consultas = 0 then '0' 
        when cant_consultas > 0 and cant_consultas <= 1 then '0 - 1'
        when cant_consultas > 1 and cant_consultas <= 3 then '2 - 3'
        when cant_consultas > 3 and cant_consultas <= 5 then '4 - 5'
        when cant_consultas > 5 and cant_consultas <= 9 then '6 - 9'
        when cant_consultas > 9 then '>9'
        else '0'
    end as cant_consultas_binned,
    -- concat(substr(fechaultope,1,4),"-",substr(fechaultope,6,2)) as aniomes_ultope,
    case when substr(fechaultope,3,1) = "-" then concat(substr(fechaultope,7,4),"-",substr(fechaultope,4,2))
        when substr(fechaultope,5,1) = "-" then concat(substr(fechaultope,1,4),"-",substr(fechaultope,6,2))
        when substr(fechaultope,3,1) = "/" then concat(substr(fechaultope,7,4),"-",substr(fechaultope,4,2))
        when substr(fechaultope,5,1) = "/" then concat(substr(fechaultope,1,4),"-",substr(fechaultope,6,2))
        else  '1900-01'
    end as aniomes_ultope,
    cast(n_trabajadores as int) as n_trabajadores,
    nvl(cast(colocacion as float),0) as colocacion,
    nvl(cast(colocaciongrp as float),0) as colocaciongrp,
    nvl(cast(saldovigente as float),0) as saldovigente,
    nvl(cast(saldovigentegrp as float),0) as saldovigentegrp,
    nvl(cast(mora as float),0) as mora,
    nvl(cast(moragrp as float),0) as moragrp,
    nvl(cast(cantidadope as float),0) as cantidadope,
    nvl(cast(cantidadopegrp as float),0) as cantidadopegrp,
    nvl(cast(moraleasing as float),0) as moraleasing,
    nvl(cast(moraleasinggrp as float),0) as moraleasinggrp,
    nvl(cast(colocacionleasing as float),0) as colocacionleasing,
    nvl(cast(colocacionleasinggrp as float),0) as colocacionleasinggrp,
    nvl(cast(monto_infra as float),0) as monto_infra,
    nvl(cast(monto_multa as float),0) as monto_multa,
    case when substr(fechapriope,3,1) = "-" then fechapriope
        when substr(fechapriope,5,1) = "-" then concat(substr(fechapriope,9,2),"-",substr(fechapriope,6,2),"-",substr(fechapriope,1,4))
        when substr(fechapriope,3,1) = "/" then concat(substr(fechaultope,7,4),"-",substr(fechaultope,4,2),"-",substr(fechapriope,1,2))
        when substr(fechapriope,5,1) = "/" then concat(substr(fechapriope,9,2),"-",substr(fechapriope,6,2),"-",substr(fechapriope,1,4))
        else  '01-01-1900'
    end as fechapriope,
    case when substr(fechaultope,3,1) = "-" then fechaultope
        when substr(fechaultope,5,1) = "-" then concat(substr(fechaultope,9,2),"-",substr(fechaultope,6,2),"-",substr(fechaultope,1,4))
        when substr(fechaultope,3,1) = "/" then concat(substr(fechaultope,7,4),"-",substr(fechaultope,4,2),"-",substr(fechaultope,1,2))
        when substr(fechaultope,5,1) = "/" then concat(substr(fechaultope,9,2),"-",substr(fechaultope,6,2),"-",substr(fechaultope,1,4))
        else  '01-01-1900'
    end as fechaultope,
    case when substr(fechapriopegrp,3,1) = "-" then fechapriopegrp
        when substr(fechapriopegrp,5,1) = "-" then concat(substr(fechapriopegrp,9,2),"-",substr(fechapriopegrp,6,2),"-",substr(fechapriopegrp,1,4))
        when substr(fechapriopegrp,3,1) = "/" then concat(substr(fechapriopegrp,7,4),"-",substr(fechapriopegrp,4,2),"-",substr(fechapriopegrp,1,2))
        when substr(fechapriopegrp,5,1) = "/" then concat(substr(fechapriopegrp,9,2),"-",substr(fechapriopegrp,6,2),"-",substr(fechapriopegrp,1,4))
        else  '01-01-1900'
    end as fechapriopegrp,
    case when substr(fechaultopegrp,3,1) = "-" then fechaultopegrp
        when substr(fechaultopegrp,5,1) = "-" then concat(substr(fechaultopegrp,9,2),"-",substr(fechaultopegrp,6,2),"-",substr(fechaultopegrp,1,4))
        when substr(fechaultopegrp,3,1) = "/" then concat(substr(fechaultopegrp,7,4),"-",substr(fechaultopegrp,4,2),"-",substr(fechaultopegrp,1,2))
        when substr(fechaultopegrp,5,1) = "/" then concat(substr(fechaultopegrp,9,2),"-",substr(fechaultopegrp,6,2),"-",substr(fechaultopegrp,1,4))
        else  '01-01-1900'
    end as fechaultopegrp,
    case when substr(fechapricontrol,3,1) = "-" then fechapricontrol
        when substr(fechapricontrol,5,1) = "-" then concat(substr(fechapricontrol,9,2),"-",substr(fechapricontrol,6,2),"-",substr(fechapricontrol,1,4))
        when substr(fechapricontrol,3,1) = "/" then concat(substr(fechapricontrol,7,4),"-",substr(fechapricontrol,4,2),"-",substr(fechapricontrol,1,2))
        when substr(fechapricontrol,5,1) = "/" then concat(substr(fechapricontrol,9,2),"-",substr(fechapricontrol,6,2),"-",substr(fechapricontrol,1,4))
        else  '01-01-1900'
    end as fechapricontrol,
    case when substr(fechaultcontrol,3,1) = "-" then fechaultcontrol
        when substr(fechaultcontrol,5,1) = "-" then concat(substr(fechaultcontrol,9,2),"-",substr(fechaultcontrol,6,2),"-",substr(fechaultcontrol,1,4))
        when substr(fechaultcontrol,3,1) = "/" then concat(substr(fechaultcontrol,7,4),"-",substr(fechaultcontrol,4,2),"-",substr(fechaultcontrol,1,2))
        when substr(fechaultcontrol,5,1) = "/" then concat(substr(fechaultcontrol,9,2),"-",substr(fechaultcontrol,6,2),"-",substr(fechaultcontrol,1,4))
        else  '01-01-1900'
    end as fechaultcontrol,
    case when substr(fechapricontrolgrp,3,1) = "-" then fechapricontrolgrp
        when substr(fechapricontrolgrp,5,1) = "-" then concat(substr(fechapricontrolgrp,9,2),"-",substr(fechapricontrolgrp,6,2),"-",substr(fechapricontrolgrp,1,4))
        when substr(fechapricontrolgrp,3,1) = "/" then concat(substr(fechapricontrolgrp,7,4),"-",substr(fechapricontrolgrp,4,2),"-",substr(fechapricontrolgrp,1,2))
        when substr(fechapricontrolgrp,5,1) = "/" then concat(substr(fechapricontrolgrp,9,2),"-",substr(fechapricontrolgrp,6,2),"-",substr(fechapricontrolgrp,1,4))
        else  '01-01-1900'
    end as fechapricontrolgrp,
    case when substr(fechaultcontrolgrp,3,1) = "-" then fechaultcontrolgrp
        when substr(fechaultcontrolgrp,5,1) = "-" then concat(substr(fechaultcontrolgrp,9,2),"-",substr(fechaultcontrolgrp,6,2),"-",substr(fechaultcontrolgrp,1,4))
        when substr(fechaultcontrolgrp,3,1) = "/" then concat(substr(fechaultcontrolgrp,7,4),"-",substr(fechaultcontrolgrp,4,2),"-",substr(fechaultcontrolgrp,1,2))
        when substr(fechaultcontrolgrp,5,1) = "/" then concat(substr(fechaultcontrolgrp,9,2),"-",substr(fechaultcontrolgrp,6,2),"-",substr(fechaultcontrolgrp,1,4))
        else  '01-01-1900'
    end as fechaultcontrolgrp,
    case when substr(fechaproxcontrol,3,1) = "-" then fechaproxcontrol
        when substr(fechaproxcontrol,5,1) = "-" then concat(substr(fechaproxcontrol,9,2),"-",substr(fechaproxcontrol,6,2),"-",substr(fechaproxcontrol,1,4))
        when substr(fechaproxcontrol,3,1) = "/" then concat(substr(fechaproxcontrol,7,4),"-",substr(fechaproxcontrol,4,2),"-",substr(fechaproxcontrol,1,2))
        when substr(fechaproxcontrol,5,1) = "/" then concat(substr(fechaproxcontrol,9,2),"-",substr(fechaproxcontrol,6,2),"-",substr(fechaproxcontrol,1,4))
        else  '01-01-1900'
    end as fechaproxcontrol,
    case when substr(fechaproxcontrolgrp,3,1) = "-" then fechaproxcontrolgrp
        when substr(fechaproxcontrolgrp,5,1) = "-" then concat(substr(fechaproxcontrolgrp,9,2),"-",substr(fechaproxcontrolgrp,6,2),"-",substr(fechaproxcontrolgrp,1,4))
        when substr(fechaproxcontrolgrp,3,1) = "/" then concat(substr(fechaproxcontrolgrp,7,4),"-",substr(fechaproxcontrolgrp,4,2),"-",substr(fechaproxcontrolgrp,1,2))
        when substr(fechaproxcontrolgrp,5,1) = "/" then concat(substr(fechaproxcontrolgrp,9,2),"-",substr(fechaproxcontrolgrp,6,2),"-",substr(fechaproxcontrolgrp,1,4))
        else  '01-01-1900'
    end as fechaproxcontrolgrp,
    ${create_date} as create_date,
    ${create_user} as create_user,
    ${update_date} as update_date,
    ${update_user} as update_user
FROM ${db}.salida_app_factotal_ref
WHERE archive = 20200706;


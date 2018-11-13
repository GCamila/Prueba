delete from [1DataSources_1Exp].[dbo].[TMultiIntegrado]

/*PARTE 1 Chahuas*/
WITH Z as (
select a.Canal,a.Fec,a.FIn,a.AÒo,a.[DÌa (numÈrico)],a.[Mes (numÈrico)],a.[FA],
a.[FAM],a.[Sol],'NULL' as ProductoNegocio,a.[ProductoNegocio2],a.[Doc],a.[Telf],
a.[Tema],a.[Tema] as tema2,'NULL' as Temaincidente,a.[Tipo InteracciÛn],a.[OperaciÛn],a.[SuboperaciÛn],
a.[Familia Plan Tarifario],UPPER(a.[Tipo InteracciÛn]+'-'+a.[OperaciÛn]+'-'+a.[SuboperaciÛn]) as NuevoTema,
b.OperacionPortal, b.SubOperacionPortal, b.DescripcionPortal, b.DetallePortal, 
a.[Motivo estado], /*b.tipo*/ 'NULL' as tipo, /*b.tipo2*/ 'NULL' as tipo2, /*b.tipo3*/ 'NULL' as tipo3,a.[Segmento Cliente],'Siebel' as fuente,
[Departamento Cliente]as [GeoVenta],[Ingresado por (Usuario)] as [Creador],[Plan Tarifario] as [PlanTarifario],
[FAM2] as antiguedad,'-' as canalnuevo
from [2Staging].dbo.[1TInt] a
left join
(
  select * from (
  select UPPER([TipoInteraccionSiebel]+'-'+[OperacionSiebel]+'-'+[SuboperacionSiebel]) as llave, OperacionPortal, 
  SubOperacionPortal, DescripcionPortal, DetallePortal, /*tipo, tipo2, tipo3,*/ FuentePortal, Cantidad,
  Row_number() OVER(partition BY UPPER([TipoInteraccionSiebel]+'-'+[OperacionSiebel]+'-'+[SuboperacionSiebel]) ORDER BY Cantidad desc) rn
  from [1DataSources_1Exp].[dbo].[TMultiCanGlosario]) as subquery where rn=1
) b
on UPPER(a.[Tipo InteracciÛn]+'-'+a.[OperaciÛn]+'-'+a.[SuboperaciÛn])=b.llave
where a.[Mes (numÈrico)] = '201809' and a.Canal in ('TIENDASPROPIAS','FRANQUICIA','CALLCENTER')
)
insert into [1DataSources_1Exp].[dbo].[TMultiIntegrado]
select Z.Canal,Z.Fec,Z.FIn,Z.AÒo,Z.[DÌa (numÈrico)],Z.[Mes (numÈrico)],Z.[FA],
Z.[FAM],Z.[Sol],Z.ProductoNegocio,Z.[ProductoNegocio2],Z.[Doc],Z.[Telf],
Z.[Tema],Z.tema2,Z.Temaincidente,Z.[Tipo InteracciÛn],Z.[OperaciÛn],Z.[SuboperaciÛn],
Z.[Familia Plan Tarifario],Z.NuevoTema,
Z.OperacionPortal, Z.SubOperacionPortal, Z.DescripcionPortal, Z.DetallePortal, 
Z.[Motivo estado],Z.tipo,Z.tipo2,Z.tipo3,Z.[Segmento Cliente],Z.fuente,count(1) as cantidad,
Z.[GeoVenta],Z.[Creador],Z.[PlanTarifario],Z.antiguedad,Z.canalnuevo
from Z
group by Z.Canal,Z.Fec,Z.FIn,Z.AÒo,Z.[DÌa (numÈrico)],Z.[Mes (numÈrico)],Z.[FA],
Z.[FAM],Z.[Sol],Z.ProductoNegocio,Z.[ProductoNegocio2],Z.[Doc],Z.[Telf],
Z.[Tema],Z.tema2,Z.Temaincidente,Z.[Tipo InteracciÛn],Z.[OperaciÛn],Z.[SuboperaciÛn],
Z.[Familia Plan Tarifario],Z.NuevoTema,
Z.OperacionPortal, Z.SubOperacionPortal, Z.DescripcionPortal, Z.DetallePortal, 
Z.[Motivo estado],Z.tipo,Z.tipo2,Z.tipo3,Z.[Segmento Cliente],Z.fuente,
Z.[GeoVenta],Z.[Creador],Z.[PlanTarifario],Z.antiguedad,Z.canalnuevo

/*PARTE 2 Camila*/
WITH W as(
Select a.Canal, a.Fec, a.FIn, a.FInAnio as AÒo, a.FI as [DÌa (numÈrico)], a.FIM as [Mes (numÈrico)], a.FA, 
a.FAM, a.Sol, a.ProductoNegocio,a.ProductoNegocio2, a.Doc, a.Telf, 
a.Tema, a.Tema2, a.Temaincidente, ISNULL(b.[TipoInteraccionSiebel],'NULL') as [Tipo InteracciÛn],ISNULL(b.[OperacionSiebel],'NULL') as [OperaciÛn],ISNULL(b.[SuboperacionSiebel],'NULL') as [SuboperaciÛn],
a.Solucion as [Familia Plan Tarifario], a.NuevoTema,
a.[Tipo de Incidente] as OperacionPortal, a.NuevoTema as SubOperacionPortal, a.Tema as DescripcionPortal, a.[Campo para Estadisticas Valor] as DetallePortal,
'NULL' as [Motivo Estado],a.tipo,a.tipo2,a.tipo3,a.segmento as [Segmento Cliente],'Portal' as fuente,Departamento as [GeoVenta],[Incidente Creado Por] as [Creador],
[Plan Tarifario] as [PlanTarifario],[FAM2] as antiguedad,'-' as canalnuevo
from [2Staging].dbo.[1TInc] a
left join
(select * from (
  select [TipoInteraccionSiebel],[OperacionSiebel],[SuboperacionSiebel],llave, OperacionPortal, 
  SubOperacionPortal, DescripcionPortal, DetallePortal, FuentePortal, Cantidad,
  Row_number() OVER(partition BY UPPER(llave) ORDER BY Cantidad desc) rn
  from [1DataSources_1Exp].[dbo].[TMultiCanGlosario]) as subquery where rn=1) b
on UPPER(a.[Tipo de Incidente]+a.NuevoTema+ a.Tema+a.[Campo para Estadisticas Valor])=b.llave
where a.FIM = '201809' and a.Canal in ('Call In_','Tiendas Franquiciadas_','Tiendas Propias_','Tiendas Islas_')
)
insert into [1DataSources_1Exp].dbo.[TMultiIntegrado]
select W.Canal,W.Fec,W.FIn,W.AÒo,W.[DÌa (numÈrico)],W.[Mes (numÈrico)],W.[FA],
W.[FAM],W.[Sol],W.ProductoNegocio,W.[ProductoNegocio2],W.[Doc],W.[Telf],
W.[Tema],W.tema2,W.Temaincidente,W.[Tipo InteracciÛn],W.[OperaciÛn],W.[SuboperaciÛn],
W.[Familia Plan Tarifario],W.NuevoTema,
W.OperacionPortal, W.SubOperacionPortal, W.DescripcionPortal, W.DetallePortal, 
W.[Motivo estado],W.tipo,W.tipo2,W.tipo3,W.[Segmento Cliente],W.fuente,count(1) as cantidad,
W.[GeoVenta],W.[Creador],W.[PlanTarifario],W.antiguedad,W.canalnuevo
from W
group by W.Canal,W.Fec,W.FIn,W.AÒo,W.[DÌa (numÈrico)],W.[Mes (numÈrico)],W.[FA],
W.[FAM],W.[Sol],W.ProductoNegocio,W.[ProductoNegocio2],W.[Doc],W.[Telf],
W.[Tema],W.tema2,W.Temaincidente,W.[Tipo InteracciÛn],W.[OperaciÛn],W.[SuboperaciÛn],
W.[Familia Plan Tarifario],W.NuevoTema,
W.OperacionPortal, W.SubOperacionPortal, W.DescripcionPortal, W.DetallePortal, 
W.[Motivo estado],W.tipo,W.tipo2,W.tipo3,W.[Segmento Cliente],W.fuente,
W.[GeoVenta],W.[Creador],W.[PlanTarifario],W.antiguedad,W.canalnuevo

--PARTE 3
--alter table [1DataSources_1Exp].[dbo].[TMultiIntegrado] add CanalNvo NVARCHAR(255) NULL;

MERGE [1DataSources_1Exp].[dbo].[TMultiIntegrado]  a
USING (SELECT [USUARIO DEL ASESOR],[NOMBRE DEL ASESOR],DNI,ANTIG‹EDAD,
		[CONDICION LABORAL],[NOMBRE DEL SUPERVISOR O JEFE INMEDIATO],
		[NOMBRE SUPERVISORA],[USUARIO SIEBEL],CAL,PLATAFORMA,PERIODO FROM (
		select	[USUARIO DEL ASESOR],[NOMBRE DEL ASESOR],DNI,ANTIG‹EDAD,
				[CONDICION LABORAL],[NOMBRE DEL SUPERVISOR O JEFE INMEDIATO],
				[NOMBRE SUPERVISORA],[USUARIO SIEBEL],CAL,PLATAFORMA,
				FORMAT(MES,'yyyyMM') AS PERIODO, ROW_NUMBER () OVER (PARTITION BY [USUARIO DEL ASESOR] ORDER BY FORMAT(MES,'yyyyMM') DESC) AS ORDEN
		from [2LandingZone].[dbo].[1Servicios]
		) AS X
		WHERE X.ORDEN=1 AND PLATAFORMA IN ('CHAT ON LINE','WHATSAPP','REDES SOCIALES')
	) b
    ON a.Creador = b.[USUARIO DEL ASESOR]
    WHEN MATCHED 
     THEN
       UPDATE SET 
              a.CanalNvo = b.PLATAFORMA;

update [1DataSources_1Exp].[dbo].[TMultiIntegrado] set CanalNvo=Canal where CanalNvo='-' 

--PARTE 4
delete from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes]

insert into [1DataSources_1Exp].[dbo].[TMultiIntegradoRes]
select CASE Canal 
             WHEN 'CALLCENTER' THEN 'Call In_' 
             WHEN 'FRANQUICIA' THEN 'Tiendas Franquiciadas_' 
             WHEN 'TIENDASPROPIAS' THEN 'Tiendas Propias_' 
             ELSE Canal END as Canal,
Fec,FIn,AÒo,[DÌa (numÈrico)],[Mes (numÈrico)],[Sol],ProductoNegocio,[ProductoNegocio2],
[Doc],[Tema],tema2,Temaincidente,[Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],
[Familia Plan Tarifario],NuevoTema,
OperacionPortal, SubOperacionPortal, DescripcionPortal, DetallePortal, 
[Motivo estado],tipo,tipo2,tipo3,[Segmento Cliente],fuente,'NULL' as TipoInteraccionG,'NULL' as TipoInteraccionT,
'NULL' as TipoConsumo,'NULL' as Excluidos,
SUM(cantidad) as CantReal,
GeoVenta,antiguedad,CASE CanalNvo 
             WHEN 'CALLCENTER' THEN 'Call In_' 
             WHEN 'FRANQUICIA' THEN 'Tiendas Franquiciadas_' 
             WHEN 'TIENDASPROPIAS' THEN 'Tiendas Propias_' 
             ELSE CanalNvo END as CanalNvo,
'-' as TipoInteraccionPS,'-' as TipoInteraccionPNvo,'-' as RegionGeoVenta
from [1DataSources_1Exp].[dbo].[TMultiIntegrado]
where [Mes (numÈrico)] = '201809'
group by CASE Canal 
             WHEN 'CALLCENTER' THEN 'Call In_' 
             WHEN 'FRANQUICIA' THEN 'Tiendas Franquiciadas_' 
             WHEN 'TIENDASPROPIAS' THEN 'Tiendas Propias_' 
             ELSE Canal END,
Fec,FIn,AÒo,[DÌa (numÈrico)],[Mes (numÈrico)],[Sol],ProductoNegocio,[ProductoNegocio2],
[Doc],[Tema],tema2,Temaincidente,[Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],
[Familia Plan Tarifario],NuevoTema,
OperacionPortal, SubOperacionPortal, DescripcionPortal, DetallePortal, 
[Motivo estado],tipo,tipo2,tipo3,[Segmento Cliente],fuente,GeoVenta,antiguedad,CASE CanalNvo 
             WHEN 'CALLCENTER' THEN 'Call In_' 
             WHEN 'FRANQUICIA' THEN 'Tiendas Franquiciadas_' 
             WHEN 'TIENDASPROPIAS' THEN 'Tiendas Propias_' 
             ELSE CanalNvo END;

--PARTE 5
/*EXEC sp_RENAME '[1DataSources_1Exp].dbo.[TMultiIntegradoRes].TipoInteraccionG','TipoInteraccionP','COLUMN';
EXEC sp_RENAME '[1DataSources_1Exp].dbo.[TMultiIntegradoRes].TipoInteraccionT','TipoInteraccionS','COLUMN';

alter table [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] add TipoInteraccionPS NVARCHAR(255) NULL
alter table [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] add TipoInteraccionPNvo NVARCHAR(255) NULL
alter table [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] add RegionGeoVenta NVARCHAR(255) NULL

alter table [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] alter column TipoInteraccionG NVARCHAR(255) NULL
alter table [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] alter column TipoInteraccionT NVARCHAR(255) NULL
alter table [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] alter column TipoConsumo NVARCHAR(255) NULL
alter table [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] alter column Excluidos NVARCHAR(255) NULL*/

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set RegionGeoVenta='Sur' where GeoVenta in ('TACNA','PUNO','MOQUEGUA','ICA','CUSCO','AYACUCHO','AREQUIPA','APURIMAC')
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set RegionGeoVenta='Centro' where GeoVenta in ('UCAYALLI','UCAYALI','SAN MARTIN','PASCO','MADRE DE DIOS','LORETO','JUNIN','HUANUCO','HUANCAVELICA','AMAZONAS','SOLOCO')
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set RegionGeoVenta='Norte' where GeoVenta in ('PIURA','LAMBAYEQUE','LA LIBERTAD','CAJAMARCA','ANCASH','TUMBES','¡ÅNCASH')
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set RegionGeoVenta='Lima' where GeoVenta in ('CALLAO','LIMA')

--PARTE 6 (HIJOS)
WITH e AS 
(select [Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Tiendas Propias_')
MERGE INTO e 
USING (select TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] where FuentePortal='TIENDA') AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo InteracciÛn] = eu.TipoInteraccionSiebel, e.[OperaciÛn] = eu.OperacionSiebel, e.[SuboperaciÛn]=SuboperacionSiebel;
--T:00:01:02 - Q:8339
--116547
WITH e AS 
(select [Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Tiendas Franquiciadas_')
MERGE INTO e 
USING (select TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] where FuentePortal='FRANQUICIADA') AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo InteracciÛn] = eu.TipoInteraccionSiebel, e.[OperaciÛn] = eu.OperacionSiebel, e.[SuboperaciÛn]=SuboperacionSiebel;
--T:00:05:09 - Q:825
--80914
WITH e AS 
(select [Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Call In_')
MERGE INTO e 
USING (
select a.TipoInteraccionSiebel,a.OperacionSiebel,a.SuboperacionSiebel,a.llave
from 
(
select cantidad,TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,
       UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave, 
       ROW_NUMBER() OVER(PARTITION BY UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) ORDER BY Cantidad desc) AS num
from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] 
where FuentePortal='CALLCENTER'
) as a
where num=1
) AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo InteracciÛn] = eu.TipoInteraccionSiebel, e.[OperaciÛn] = eu.OperacionSiebel, e.[SuboperaciÛn]=SuboperacionSiebel;
--T:00:10:08 - Q:26329
--186684

--PARTE 7 (PADRES)
WITH e AS 
(select [Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Tiendas Propias_' and [Tipo InteracciÛn]='NULL')
MERGE INTO e 
USING (select TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] where FuentePortal='TIENDA' and DetallePortal='NULL') AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo InteracciÛn] = eu.TipoInteraccionSiebel, e.[OperaciÛn] = eu.OperacionSiebel, e.[SuboperaciÛn]=SuboperacionSiebel;
--T:00:01:02 - Q:8339
--116547
WITH e AS 
(select [Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Tiendas Franquiciadas_' and [Tipo InteracciÛn]='NULL')
MERGE INTO e 
USING (select TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] where FuentePortal='FRANQUICIADA' and DetallePortal='NULL') AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo InteracciÛn] = eu.TipoInteraccionSiebel, e.[OperaciÛn] = eu.OperacionSiebel, e.[SuboperaciÛn]=SuboperacionSiebel;
--T:00:05:09 - Q:825
--80914
WITH e AS 
(select [Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Call In_' and [Tipo InteracciÛn]='NULL')
MERGE INTO e 
USING (
select a.TipoInteraccionSiebel,a.OperacionSiebel,a.SuboperacionSiebel,a.llave
from 
(
select cantidad,TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,
       UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) as llave, 
       ROW_NUMBER() OVER(PARTITION BY UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '·', 'a'), 'È','e'), 'Ì', 'i'), 'Û', 'o'), '˙','u'),char(9),'')))) ORDER BY Cantidad desc) AS num
from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] 
where FuentePortal='CALLCENTER' and DetallePortal='NULL'
) as a  
where num=1
) AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo InteracciÛn] = eu.TipoInteraccionSiebel, e.[OperaciÛn] = eu.OperacionSiebel, e.[SuboperaciÛn]=SuboperacionSiebel;
--T:00:10:08 - Q:26329
--186684

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set [Tipo InteracciÛn]='Otros', [OperaciÛn]='Otros', [SuboperaciÛn]='Otros' where [Tipo InteracciÛn] = 'NULL'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Con Consumo' where fuente = 'Portal' and NuevoTema='InformaciÛn de Consumos';
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Sin Consumo' where fuente = 'Portal' and NuevoTema<>'InformaciÛn de Consumos';
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Con Consumo' where SubOperacionPortal='InformaciÛn de Consumos';
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Sin Consumo' where SubOperacionPortal<>'InformaciÛn de Consumos';
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Con Consumo' where fuente = 'Siebel' and ([Tipo InteracciÛn]='Consulta' and [OperaciÛn]='Consumos Y Saldos');
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Sin Consumo' where fuente = 'Siebel' and TipoConsumo='-';

--PARTE 8.1
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='-'

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='InformaciÛn' 
where SubOperacionPortal in ('InformaciÛn de Centros de AtenciÛn','InformaciÛn de Cobertura','InformaciÛn de Consumos',
'InformaciÛn de Estado de Solicitud','InformaciÛn de FacturaciÛn y Recibo','InformaciÛn de Funciones de Equipo',
'InformaciÛn de la LÌnea','InformaciÛn de Oferta Comercial','InformaciÛn de Procesos','InformaciÛn de Servicios');

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Reclamo' 
where SubOperacionPortal in ('Reclamos de FacturaciÛn/Dinero','Reclamos de Servicio','Reclamos Indecopi','Reclamos de Servicio',
'Reclamos por ActivaciÛn','Reclamos por DesactivaciÛn','Reclamos por Equipos','Reclamos por Fraude',
'Reclamos por Mala AtenciÛn en Canales','Reclamos por Pedidos','Reclamos por Publicidad EngaÒosa','Reclamos por Saldos/Consumos');

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='OperaciÛn' 
where SubOperacionPortal in ('ActivaciÛn de Recargas','ActivaciÛn/DesactivaciÛn de SVA''s','Cambio de Plan','CesiÛn de Equipo','ReconexiÛn de LÌnea',
'RenovaciÛn de Equipo','ReposiciÛn de Chip','RevisiÛn de Servicio TÈcnico','Solicitudes Generales','SuspensiÛn de Equipo - Cliente',
'SuspensiÛn de Equipo - Robo','SuspensiÛn de Equipo - Uso Indebido');

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='InformaciÛn'
where TipoInteraccionP='-' and Excluidos='No' and [Tipo InteracciÛn] in ('Consulta')

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Reclamo'
where TipoInteraccionP='-' and Excluidos='No' and [Tipo InteracciÛn] in ('Problema','Reclamo')

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Orden'
where TipoInteraccionP='-' and Excluidos='No' and [Tipo InteracciÛn] in ('Orden','Solicitud')

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Excluidos'
where TipoInteraccionP='-' and Excluidos='Si'

--PARTE 8.2
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS='-'

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS=[Tipo InteracciÛn]

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS='Reclamo'
where TipoInteraccionS='Reclamos'

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS='Orden'
where SubOperacionPortal in ('','','','','','','','','')
'ActivaciÛn de Recargas','ActivaciÛn/DesactivaciÛn de SVA''''s','Activaciones por USSD',
'Cambio de Plan','CesiÛn de Equipo','Reclamos de FacturaciÛn/Dinero','Reclamos de Servicio',
'Reclamos por Equipos','Reclamos por Mala AtenciÛn en Canales','Reclamos por Pedidos',
'ReconexiÛn de LÌnea','RenovaciÛn de Equipo','ReposiciÛn de Chip','RevisiÛn de Servicio TÈcnico',
'Ventas y Portabilidad',

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS='Excluidos'
where TipoInteraccionP='-' and Excluidos='Si'

--PARTE 9
update [TMultiIntegradoRes] set Excluidos='No'
update [TMultiIntegradoRes] 
set Excluidos='Si' 
where SubOperacionPortal in ('Activaciones por USSD','Derivaciones',
                             'Problema en la Llamada al Call','Ventas y Portabilidad');
update [TMultiIntegradoRes] 
set Excluidos='Si' 
where SubOperacionPortal in ('Solicitudes Generales') and DescripcionPortal='PAGOS - PEDIDO - PAGO DE PENALIDADES';

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS=TipoInteraccionS where fuente='Portal'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS=Tipo2 where fuente='Portal' and TipoInteraccionPS='Reclamo'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='Problema' where fuente='Portal' and TipoInteraccionPS='Reporta Inconveniente'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='InformaciÛn' where TipoInteraccionS in ('InformaciÛn','Consulta','Informacion') and fuente='Siebel'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='OperaciÛn' where TipoInteraccionS in ('OperaciÛn','Orden','Solicitud') and fuente='Siebel'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='Reclamos' where TipoInteraccionS in ('Reclamo') and fuente='Siebel'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='Problema' where TipoInteraccionS in ('Problema') and fuente='Siebel'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='SA' where TipoInteraccionS in ('Reclamos') and [Motivo estado]='SOLUCI”NANTICIPADA' and fuente='Siebel'

exec sp_columns [TMultiResumen];
exec sp_columns [TMultiIntegrado];

INSERT INTO [dbo].[TMultiResumen] 
SELECT Canal,Fec,FIn,[AÒo],[DÌa (numÈrico)],[Mes (numÈrico)],FA,FAM,
LEFT(Sol,10),ProductoNegocio,LEFT(ProductoNegocio2,5),LEFT(Doc,3),Telf,
Tema,tema2,Temaincidente,[Tipo InteracciÛn],OperaciÛn,SuboperaciÛn,[Familia Plan Tarifario],
NuevoTema,OperacionPortal,SubOperacionPortal,DescripcionPortal,DetallePortal,[Motivo estado],
LEFT(tipo,10),tipo2,tipo3,[Segmento Cliente],LEFT(fuente,6),cantidad,GeoVenta,Creador,PlanTarifario,LEFT(antiguedad,13),
CanalNvo FROM [dbo].[TMultiIntegrado] a
WHERE a.Fec = '01/08/2018' 

exec sp_columns [TMultiRep];
exec sp_columns [TMultiIntegradoRes];

INSERT INTO [dbo].[TMultiRep]  
SELECT [Canal],[Fec],[FIn],[AÒo],[DÌa (numÈrico)],[Mes (numÈrico)],LEFT([Sol],10),
[ProductoNegocio],LEFT([ProductoNegocio2],5),LEFT([Doc],3),[Tema],[tema2],
[Temaincidente],[Tipo InteracciÛn],[OperaciÛn],[SuboperaciÛn],[Familia Plan Tarifario],
[NuevoTema],[OperacionPortal],[SubOperacionPortal],[DescripcionPortal],[DetallePortal],
[Motivo estado],LEFT([tipo],11),[tipo2],[tipo3],[Segmento Cliente],
LEFT([fuente],6),[CantReal],[GeoVenta],LEFT([antiguedad],13),[CanalNvo],
[TipoInteraccionPNvo],[TipoInteraccionP],[TipoInteraccionS],[TipoInteraccionPS],[RegionGeoVenta],
[TipoConsumo],[Excluidos] FROM [dbo].[TMultiIntegradoRes] a
WHERE a.Fec = '01/08/2018' 

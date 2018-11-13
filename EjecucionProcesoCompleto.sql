delete from [1DataSources_1Exp].[dbo].[TMultiIntegrado]

/*PARTE 1 Chahuas*/
WITH Z as (
select a.Canal,a.Fec,a.FIn,a.A�o,a.[D�a (num�rico)],a.[Mes (num�rico)],a.[FA],
a.[FAM],a.[Sol],'NULL' as ProductoNegocio,a.[ProductoNegocio2],a.[Doc],a.[Telf],
a.[Tema],a.[Tema] as tema2,'NULL' as Temaincidente,a.[Tipo Interacci�n],a.[Operaci�n],a.[Suboperaci�n],
a.[Familia Plan Tarifario],UPPER(a.[Tipo Interacci�n]+'-'+a.[Operaci�n]+'-'+a.[Suboperaci�n]) as NuevoTema,
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
on UPPER(a.[Tipo Interacci�n]+'-'+a.[Operaci�n]+'-'+a.[Suboperaci�n])=b.llave
where a.[Mes (num�rico)] = '201809' and a.Canal in ('TIENDASPROPIAS','FRANQUICIA','CALLCENTER')
)
insert into [1DataSources_1Exp].[dbo].[TMultiIntegrado]
select Z.Canal,Z.Fec,Z.FIn,Z.A�o,Z.[D�a (num�rico)],Z.[Mes (num�rico)],Z.[FA],
Z.[FAM],Z.[Sol],Z.ProductoNegocio,Z.[ProductoNegocio2],Z.[Doc],Z.[Telf],
Z.[Tema],Z.tema2,Z.Temaincidente,Z.[Tipo Interacci�n],Z.[Operaci�n],Z.[Suboperaci�n],
Z.[Familia Plan Tarifario],Z.NuevoTema,
Z.OperacionPortal, Z.SubOperacionPortal, Z.DescripcionPortal, Z.DetallePortal, 
Z.[Motivo estado],Z.tipo,Z.tipo2,Z.tipo3,Z.[Segmento Cliente],Z.fuente,count(1) as cantidad,
Z.[GeoVenta],Z.[Creador],Z.[PlanTarifario],Z.antiguedad,Z.canalnuevo
from Z
group by Z.Canal,Z.Fec,Z.FIn,Z.A�o,Z.[D�a (num�rico)],Z.[Mes (num�rico)],Z.[FA],
Z.[FAM],Z.[Sol],Z.ProductoNegocio,Z.[ProductoNegocio2],Z.[Doc],Z.[Telf],
Z.[Tema],Z.tema2,Z.Temaincidente,Z.[Tipo Interacci�n],Z.[Operaci�n],Z.[Suboperaci�n],
Z.[Familia Plan Tarifario],Z.NuevoTema,
Z.OperacionPortal, Z.SubOperacionPortal, Z.DescripcionPortal, Z.DetallePortal, 
Z.[Motivo estado],Z.tipo,Z.tipo2,Z.tipo3,Z.[Segmento Cliente],Z.fuente,
Z.[GeoVenta],Z.[Creador],Z.[PlanTarifario],Z.antiguedad,Z.canalnuevo

/*PARTE 2 Camila*/
WITH W as(
Select a.Canal, a.Fec, a.FIn, a.FInAnio as A�o, a.FI as [D�a (num�rico)], a.FIM as [Mes (num�rico)], a.FA, 
a.FAM, a.Sol, a.ProductoNegocio,a.ProductoNegocio2, a.Doc, a.Telf, 
a.Tema, a.Tema2, a.Temaincidente, ISNULL(b.[TipoInteraccionSiebel],'NULL') as [Tipo Interacci�n],ISNULL(b.[OperacionSiebel],'NULL') as [Operaci�n],ISNULL(b.[SuboperacionSiebel],'NULL') as [Suboperaci�n],
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
select W.Canal,W.Fec,W.FIn,W.A�o,W.[D�a (num�rico)],W.[Mes (num�rico)],W.[FA],
W.[FAM],W.[Sol],W.ProductoNegocio,W.[ProductoNegocio2],W.[Doc],W.[Telf],
W.[Tema],W.tema2,W.Temaincidente,W.[Tipo Interacci�n],W.[Operaci�n],W.[Suboperaci�n],
W.[Familia Plan Tarifario],W.NuevoTema,
W.OperacionPortal, W.SubOperacionPortal, W.DescripcionPortal, W.DetallePortal, 
W.[Motivo estado],W.tipo,W.tipo2,W.tipo3,W.[Segmento Cliente],W.fuente,count(1) as cantidad,
W.[GeoVenta],W.[Creador],W.[PlanTarifario],W.antiguedad,W.canalnuevo
from W
group by W.Canal,W.Fec,W.FIn,W.A�o,W.[D�a (num�rico)],W.[Mes (num�rico)],W.[FA],
W.[FAM],W.[Sol],W.ProductoNegocio,W.[ProductoNegocio2],W.[Doc],W.[Telf],
W.[Tema],W.tema2,W.Temaincidente,W.[Tipo Interacci�n],W.[Operaci�n],W.[Suboperaci�n],
W.[Familia Plan Tarifario],W.NuevoTema,
W.OperacionPortal, W.SubOperacionPortal, W.DescripcionPortal, W.DetallePortal, 
W.[Motivo estado],W.tipo,W.tipo2,W.tipo3,W.[Segmento Cliente],W.fuente,
W.[GeoVenta],W.[Creador],W.[PlanTarifario],W.antiguedad,W.canalnuevo

--PARTE 3
--alter table [1DataSources_1Exp].[dbo].[TMultiIntegrado] add CanalNvo NVARCHAR(255) NULL;

MERGE [1DataSources_1Exp].[dbo].[TMultiIntegrado]  a
USING (SELECT [USUARIO DEL ASESOR],[NOMBRE DEL ASESOR],DNI,ANTIG�EDAD,
		[CONDICION LABORAL],[NOMBRE DEL SUPERVISOR O JEFE INMEDIATO],
		[NOMBRE SUPERVISORA],[USUARIO SIEBEL],CAL,PLATAFORMA,PERIODO FROM (
		select	[USUARIO DEL ASESOR],[NOMBRE DEL ASESOR],DNI,ANTIG�EDAD,
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
Fec,FIn,A�o,[D�a (num�rico)],[Mes (num�rico)],[Sol],ProductoNegocio,[ProductoNegocio2],
[Doc],[Tema],tema2,Temaincidente,[Tipo Interacci�n],[Operaci�n],[Suboperaci�n],
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
where [Mes (num�rico)] = '201809'
group by CASE Canal 
             WHEN 'CALLCENTER' THEN 'Call In_' 
             WHEN 'FRANQUICIA' THEN 'Tiendas Franquiciadas_' 
             WHEN 'TIENDASPROPIAS' THEN 'Tiendas Propias_' 
             ELSE Canal END,
Fec,FIn,A�o,[D�a (num�rico)],[Mes (num�rico)],[Sol],ProductoNegocio,[ProductoNegocio2],
[Doc],[Tema],tema2,Temaincidente,[Tipo Interacci�n],[Operaci�n],[Suboperaci�n],
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
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set RegionGeoVenta='Norte' where GeoVenta in ('PIURA','LAMBAYEQUE','LA LIBERTAD','CAJAMARCA','ANCASH','TUMBES','��NCASH')
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set RegionGeoVenta='Lima' where GeoVenta in ('CALLAO','LIMA')

--PARTE 6 (HIJOS)
WITH e AS 
(select [Tipo Interacci�n],[Operaci�n],[Suboperaci�n],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Tiendas Propias_')
MERGE INTO e 
USING (select TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] where FuentePortal='TIENDA') AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo Interacci�n] = eu.TipoInteraccionSiebel, e.[Operaci�n] = eu.OperacionSiebel, e.[Suboperaci�n]=SuboperacionSiebel;
--T:00:01:02 - Q:8339
--116547
WITH e AS 
(select [Tipo Interacci�n],[Operaci�n],[Suboperaci�n],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Tiendas Franquiciadas_')
MERGE INTO e 
USING (select TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] where FuentePortal='FRANQUICIADA') AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo Interacci�n] = eu.TipoInteraccionSiebel, e.[Operaci�n] = eu.OperacionSiebel, e.[Suboperaci�n]=SuboperacionSiebel;
--T:00:05:09 - Q:825
--80914
WITH e AS 
(select [Tipo Interacci�n],[Operaci�n],[Suboperaci�n],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Call In_')
MERGE INTO e 
USING (
select a.TipoInteraccionSiebel,a.OperacionSiebel,a.SuboperacionSiebel,a.llave
from 
(
select cantidad,TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,
       UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave, 
       ROW_NUMBER() OVER(PARTITION BY UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) ORDER BY Cantidad desc) AS num
from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] 
where FuentePortal='CALLCENTER'
) as a
where num=1
) AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo Interacci�n] = eu.TipoInteraccionSiebel, e.[Operaci�n] = eu.OperacionSiebel, e.[Suboperaci�n]=SuboperacionSiebel;
--T:00:10:08 - Q:26329
--186684

--PARTE 7 (PADRES)
WITH e AS 
(select [Tipo Interacci�n],[Operaci�n],[Suboperaci�n],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal+DetallePortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Tiendas Propias_' and [Tipo Interacci�n]='NULL')
MERGE INTO e 
USING (select TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] where FuentePortal='TIENDA' and DetallePortal='NULL') AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo Interacci�n] = eu.TipoInteraccionSiebel, e.[Operaci�n] = eu.OperacionSiebel, e.[Suboperaci�n]=SuboperacionSiebel;
--T:00:01:02 - Q:8339
--116547
WITH e AS 
(select [Tipo Interacci�n],[Operaci�n],[Suboperaci�n],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Tiendas Franquiciadas_' and [Tipo Interacci�n]='NULL')
MERGE INTO e 
USING (select TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] where FuentePortal='FRANQUICIADA' and DetallePortal='NULL') AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo Interacci�n] = eu.TipoInteraccionSiebel, e.[Operaci�n] = eu.OperacionSiebel, e.[Suboperaci�n]=SuboperacionSiebel;
--T:00:05:09 - Q:825
--80914
WITH e AS 
(select [Tipo Interacci�n],[Operaci�n],[Suboperaci�n],UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave from [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] where fuente = 'Portal' and Canal = 'Call In_' and [Tipo Interacci�n]='NULL')
MERGE INTO e 
USING (
select a.TipoInteraccionSiebel,a.OperacionSiebel,a.SuboperacionSiebel,a.llave
from 
(
select cantidad,TipoInteraccionSiebel,OperacionSiebel,SuboperacionSiebel,
       UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) as llave, 
       ROW_NUMBER() OVER(PARTITION BY UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(OperacionPortal+SubOperacionPortal+DescripcionPortal, '�', 'a'), '�','e'), '�', 'i'), '�', 'o'), '�','u'),char(9),'')))) ORDER BY Cantidad desc) AS num
from [1DataSources_1Exp].[dbo].[TMultiCanGlosario] 
where FuentePortal='CALLCENTER' and DetallePortal='NULL'
) as a  
where num=1
) AS eu
ON e.llave = eu.llave
WHEN MATCHED THEN
     UPDATE SET e.[Tipo Interacci�n] = eu.TipoInteraccionSiebel, e.[Operaci�n] = eu.OperacionSiebel, e.[Suboperaci�n]=SuboperacionSiebel;
--T:00:10:08 - Q:26329
--186684

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set [Tipo Interacci�n]='Otros', [Operaci�n]='Otros', [Suboperaci�n]='Otros' where [Tipo Interacci�n] = 'NULL'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Con Consumo' where fuente = 'Portal' and NuevoTema='Informaci�n de Consumos';
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Sin Consumo' where fuente = 'Portal' and NuevoTema<>'Informaci�n de Consumos';
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Con Consumo' where SubOperacionPortal='Informaci�n de Consumos';
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Sin Consumo' where SubOperacionPortal<>'Informaci�n de Consumos';
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Con Consumo' where fuente = 'Siebel' and ([Tipo Interacci�n]='Consulta' and [Operaci�n]='Consumos Y Saldos');
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoConsumo='Sin Consumo' where fuente = 'Siebel' and TipoConsumo='-';

--PARTE 8.1
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='-'

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Informaci�n' 
where SubOperacionPortal in ('Informaci�n de Centros de Atenci�n','Informaci�n de Cobertura','Informaci�n de Consumos',
'Informaci�n de Estado de Solicitud','Informaci�n de Facturaci�n y Recibo','Informaci�n de Funciones de Equipo',
'Informaci�n de la L�nea','Informaci�n de Oferta Comercial','Informaci�n de Procesos','Informaci�n de Servicios');

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Reclamo' 
where SubOperacionPortal in ('Reclamos de Facturaci�n/Dinero','Reclamos de Servicio','Reclamos Indecopi','Reclamos de Servicio',
'Reclamos por Activaci�n','Reclamos por Desactivaci�n','Reclamos por Equipos','Reclamos por Fraude',
'Reclamos por Mala Atenci�n en Canales','Reclamos por Pedidos','Reclamos por Publicidad Enga�osa','Reclamos por Saldos/Consumos');

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Operaci�n' 
where SubOperacionPortal in ('Activaci�n de Recargas','Activaci�n/Desactivaci�n de SVA''s','Cambio de Plan','Cesi�n de Equipo','Reconexi�n de L�nea',
'Renovaci�n de Equipo','Reposici�n de Chip','Revisi�n de Servicio T�cnico','Solicitudes Generales','Suspensi�n de Equipo - Cliente',
'Suspensi�n de Equipo - Robo','Suspensi�n de Equipo - Uso Indebido');

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Informaci�n'
where TipoInteraccionP='-' and Excluidos='No' and [Tipo Interacci�n] in ('Consulta')

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Reclamo'
where TipoInteraccionP='-' and Excluidos='No' and [Tipo Interacci�n] in ('Problema','Reclamo')

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Orden'
where TipoInteraccionP='-' and Excluidos='No' and [Tipo Interacci�n] in ('Orden','Solicitud')

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionP='Excluidos'
where TipoInteraccionP='-' and Excluidos='Si'

--PARTE 8.2
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS='-'

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS=[Tipo Interacci�n]

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS='Reclamo'
where TipoInteraccionS='Reclamos'

update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionS='Orden'
where SubOperacionPortal in ('','','','','','','','','')
'Activaci�n de Recargas','Activaci�n/Desactivaci�n de SVA''''s','Activaciones por USSD',
'Cambio de Plan','Cesi�n de Equipo','Reclamos de Facturaci�n/Dinero','Reclamos de Servicio',
'Reclamos por Equipos','Reclamos por Mala Atenci�n en Canales','Reclamos por Pedidos',
'Reconexi�n de L�nea','Renovaci�n de Equipo','Reposici�n de Chip','Revisi�n de Servicio T�cnico',
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
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='Informaci�n' where TipoInteraccionS in ('Informaci�n','Consulta','Informacion') and fuente='Siebel'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='Operaci�n' where TipoInteraccionS in ('Operaci�n','Orden','Solicitud') and fuente='Siebel'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='Reclamos' where TipoInteraccionS in ('Reclamo') and fuente='Siebel'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='Problema' where TipoInteraccionS in ('Problema') and fuente='Siebel'
update [1DataSources_1Exp].[dbo].[TMultiIntegradoRes] set TipoInteraccionPS='SA' where TipoInteraccionS in ('Reclamos') and [Motivo estado]='SOLUCI�NANTICIPADA' and fuente='Siebel'

exec sp_columns [TMultiResumen];
exec sp_columns [TMultiIntegrado];

INSERT INTO [dbo].[TMultiResumen] 
SELECT Canal,Fec,FIn,[A�o],[D�a (num�rico)],[Mes (num�rico)],FA,FAM,
LEFT(Sol,10),ProductoNegocio,LEFT(ProductoNegocio2,5),LEFT(Doc,3),Telf,
Tema,tema2,Temaincidente,[Tipo Interacci�n],Operaci�n,Suboperaci�n,[Familia Plan Tarifario],
NuevoTema,OperacionPortal,SubOperacionPortal,DescripcionPortal,DetallePortal,[Motivo estado],
LEFT(tipo,10),tipo2,tipo3,[Segmento Cliente],LEFT(fuente,6),cantidad,GeoVenta,Creador,PlanTarifario,LEFT(antiguedad,13),
CanalNvo FROM [dbo].[TMultiIntegrado] a
WHERE a.Fec = '01/08/2018' 

exec sp_columns [TMultiRep];
exec sp_columns [TMultiIntegradoRes];

INSERT INTO [dbo].[TMultiRep]  
SELECT [Canal],[Fec],[FIn],[A�o],[D�a (num�rico)],[Mes (num�rico)],LEFT([Sol],10),
[ProductoNegocio],LEFT([ProductoNegocio2],5),LEFT([Doc],3),[Tema],[tema2],
[Temaincidente],[Tipo Interacci�n],[Operaci�n],[Suboperaci�n],[Familia Plan Tarifario],
[NuevoTema],[OperacionPortal],[SubOperacionPortal],[DescripcionPortal],[DetallePortal],
[Motivo estado],LEFT([tipo],11),[tipo2],[tipo3],[Segmento Cliente],
LEFT([fuente],6),[CantReal],[GeoVenta],LEFT([antiguedad],13),[CanalNvo],
[TipoInteraccionPNvo],[TipoInteraccionP],[TipoInteraccionS],[TipoInteraccionPS],[RegionGeoVenta],
[TipoConsumo],[Excluidos] FROM [dbo].[TMultiIntegradoRes] a
WHERE a.Fec = '01/08/2018' 

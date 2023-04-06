const xlsx = require('xlsx');
const pg = require('pg');
const ibmdb = require('ibm_db');
const dotenv = require('dotenv');
const path = require('path');

dotenv.config();

// constantes para conectarse a las BD
const configSVD = `DATABASE=${process.env.SVD_DATABASE};HOSTNAME=${process.env.SVD_HOSTNAME};UID=${process.env.SVD_UID};PWD=${process.env.SVD_PWD};PORT=${process.env.SVD_PORT};PROTOCOL=TCPIP`;

const configIyMP = {
    user: process.env.IMP_USER,
    host: process.env.IMP_HOST,
    password: process.env.IMP_PASS,
    database: process.env.IMP_NAME,
    port: process.env.IMP_PORT
}

const carpeta = process.env.HOME_REZONIFICACION;

const rutaGeo = path.join(carpeta, 'GEO.xlsx');
const rutaDb = path.join(carpeta, 'DB.xlsx');

const querys = {
    imp: {
        searchBarrios: 'select b.codigo as "CODIGO BARRIO", b.uuid as "UUID", b.descripcion as "BARRIO", sb.id_seccion as "ZONA SECCIÓN" from public.barrio b inner join public.seccion_barrio sb on b.uuid = sb.uuid where b.codigo in ($1);'
    },
    svd: {
        searchBarrios: 'select VARCHAR(b.codigo) as "CODIGO BARRIO", b.uuid as "UUID", b.descripcion as "BARRIO", sb.seccion as "ZONA SECCIÓN" from SVD.barrio b inner join SVD.seccion_barrio sb on b.uuid = sb.barrio where b.codigo in ($1);',
        searchFechaCierre: 'SELECT scv.SECCION, scv.CICLO_VENTA AS "CICLO VENTA", scv.FIN_PEDIODO_PEDIDO AS "FECHA CIERRE" FROM SVD.SECCION_CICLO_VENTA scv WHERE scv.CICLO_VENTA = $1 AND scv.SECCION IN ($2);'
    }
}

const getStringList = (data, campo) => {
    let stringList = data.reduce((str, item) => {
        return str + item[campo] + "','";
    }, "'");
    return stringList.slice(0, -2);
}

const getZona = (barrios) => {
    let zonas = barrios.reduce((pares, barrio) => {
        const parZonas = barrio['ZONA ORIGEN'] + '->' + barrio['ZONA DESTINO'];
        if (pares.inclede(parZonas)) pares.push(parZonas);
        return pares;
    }, []);
    return zonas;
}

const main = async () => {
    try {
        const excelGeo = xlsx.readFile(rutaGeo);
        const barriosGeo = xlsx.utils.sheet_to_json(excelGeo.Sheets['BARRIOS']);
        const ciclo = parseInt(xlsx.utils.sheet_to_json(excelGeo.Sheets['CICLO'])[0]['CICLO VENTA']);
        const codigos = getStringList(barriosGeo, 'CODIGO BARRIO');
        const secciones = getStringList(barriosGeo, 'SECCIÓN ORIGEN');
        //console.log("codigos: ",codigos);

        const queryCodigosImp = querys.imp.searchBarrios.replace('$1', codigos);
        const queryCodigosSvd = querys.svd.searchBarrios.replace('$1', codigos);
        const queryCiclo = querys.svd.searchFechaCierre.replace('$1', ciclo).replace('$2', secciones);

        console.log('conectando a db...');
        const poolImp = new pg.Pool(configIyMP);
        const poolSvd = await ibmdb.open(configSVD);
        console.log('consultando...');
        const [barriosImp, barriosSvd, ciclosSvd] = await Promise.all([
            poolImp.query(queryCodigosImp),
            poolSvd.query(queryCodigosSvd),
            poolSvd.query(queryCiclo)
        ])
        console.log('escribiendo...');
        const excelDb = xlsx.utils.book_new();

        const sheetImp = xlsx.utils.json_to_sheet(barriosImp.rows);
        xlsx.utils.book_append_sheet(excelDb, sheetImp, 'IyMP');

        const sheetSvd = xlsx.utils.json_to_sheet(barriosSvd);
        xlsx.utils.book_append_sheet(excelDb, sheetSvd, 'SVD');

        const sheetCiclo = xlsx.utils.json_to_sheet(ciclosSvd);
        xlsx.utils.book_append_sheet(excelDb, sheetCiclo, 'CICLO');

        xlsx.writeFile(excelDb, rutaDb, sheetCiclo);

        poolImp.end();
        poolSvd.close();

    } catch (error) {
        console.error(error);
    } finally {
        console.log(' terminado ');
    }
}

main();
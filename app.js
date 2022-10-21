const cron = require('node-cron');
const mysql = require('mysql');

const convertir = require('numero-a-letras');
const { response } = require('express');

const axios = require('axios').default;

const connection = mysql.createConnection({
    host: "192.196.159.78",
    user: "esfacturado_reporte",
    password: "contabilidad123",
    database: "esfactur_bd_sys"
});

connection.connect((err) => {
    if(err) throw err;
    console.log("Connected to database");
});

function traer_secuencia_resumen_boletas(connection, data, callback) {
    connection.query("SELECT COUNT(*) as resumenes FROM resumen_boletas WHERE id_contribuyente = " + data.id_contribuyente + " AND serie = "+data.serie + " AND tipo_envio_sunat = '"+ data.tipo_envio_sunat+" ' ",function(err,result){
        if(err) throw err;
        callback(result);
    });
}

function insertar_resumen(connection, data, callback){
    let sql = `INSERT INTO resumen_boletas (id_contribuyente, codigo, serie, secuencia, tipo_envio_sunat, fecha_documentos, fecha_envio_sunat, estado_envio_sunat, hash_cpe, hash_cdr, numero_ticket, cod_sunat, msje_sunat, ruta_xml, name_xml, name_xml_zip, name_cdr, name_cdr_zip, intentos_envio_sunat, estado_documento, accion_sunat, detalle) VALUES (${data.id_contribuyente}, '${data.codigo}', '${data.serie}', '${data.secuencia}', '${data.tipo_envio_sunat}', '${data.fecha_documentos}', '${data.fecha_envio_sunat}', '${data.estado_envio_sunat}', '${data.hash_cpe}', '${data.hash_cdr}', '${data.numero_ticket}', '${data.cod_sunat}', '${data.msje_sunat}', '${data.ruta_xml}', '${data.name_xml}', '${data.name_xml_zip}', '${data.name_cdr}', '${data.name_cdr_zip}', ${data.intentos_envio_sunat}, '${data.estado_documento}', ${data.accion_sunat}, '')`;

    connection.query(sql, function (err, result) {
        if (err) throw err;
        console.log("1 record inserted");
    });
}

function readVentas(connection, callback) {
    connection.query("SELECT *, contribuyente.razon_social as empresa, contribuyente.nombre_comercial as nombreComercial, contribuyente.direccion_fiscal as direccion_empresa FROM doc_electronico INNER JOIN contribuyente ON contribuyente.id_contribuyente = doc_electronico.id_contribuyente INNER JOIN sunat_codigoubigeo ON sunat_codigoubigeo.codigo_ubigeo = contribuyente.codigo_ubigeo INNER JOIN cliente ON cliente.idcliente = doc_electronico.idcliente INNER JOIN sucursal ON sucursal.id_contribuyente = contribuyente.id_contribuyente WHERE doc_electronico.id_contribuyente = 134 AND (doc_electronico.id_tipodoc_electronico = '03') AND doc_electronico.estado_envio_sunat = 'pendiente' ", function(err, result) {
        if(err) throw err;
        callback(result);
    });
}



readVentas(connection, (result) => {
    let array = {};
    let data;
    result.map((venta, index) => {
        let tipo_proceso;

        const ruta = "http://localhost/sis_facturador/api_facturacion/resumen_boletas.php";

        if (venta.tipo_envio_sunat == 'prueba') {
            tipo_proceso = "3";
        } else {
            tipo_proceso = "1";
        }

        let date = new Date();
        let year = date.getFullYear();
        let month = date.getMonth() + 1;
        let day = date.getDate();

        let date_v = new Date(venta.fecha_comprobante);
        let year_v = date_v.getFullYear();
        let month_v = date_v.getMonth() + 1;
        let day_v = date_v.getDate();

        if (month < 10) {
            month = "0"+month;
        }

        if (day <  10) {
            day = "0"+day;
        }

        if (month_v < 10) {
            month_v = "0"+month_v;
        }

        if (day_v <  10) {
            day_v = "0"+day_v;
        }

        let serie = year+""+month+""+day;
        let fecha = year_v+"-"+month_v+"-"+day_v;
        let fecha_doc = year+"-"+month+"-"+day;

        let envio = venta.tipo_envio_sunat;
        let serie_resumen = serie;
        let id_con = venta.id_contribuyente;

        let venta_serie = venta.serie_comprobante;
        let venta_numero = venta.numero_comprobante;
        let ruc_empresa = venta.ruc;

        data = {
            codigo: "RC",
            serie: serie,
            secuencia: (index + 1),
            fecha_referencia: fecha,
            fecha_documento: fecha_doc,
            tipo_proceso: tipo_proceso,
            firma: venta.pass_certificado,
            emisor: {
                ruc: venta.ruc,
                tipo_doc: "6",
                nom_comercial: venta.nombreComercial,
                razon_social: venta.empresa,
                codigo_ubigeo: venta.codigo_ubigeo,
                direccion: venta.direccion_fiscal,
                direccion_departamento: venta.departamento,
                direccion_provincia: venta.provincia,
                direccion_distrito: venta.distrito,
                direccion_codigopais: "PE",
                usuariosol: venta.usuario_sol,
                clavesol: venta.clave_sol
            },
            detalle: [{
                ITEM: 1,
                TIPO_COMPROBANTE: "03",
                NRO_COMPROBANTE: `${venta.serie_comprobante}-${venta.numero_comprobante}`,
                NRO_DOCUMENTO: venta.num_doc,
                TIPO_DOCUMENTO: "1",
                NRO_COMPROBANTE_REF: "0",
                TIPO_COMPROBANTE_REF: "0",
                STATUS: "1",
                COD_MONEDA: "PEN",
                TOTAL: venta.total,
                GRAVADA: venta.total_gravadas,
                EXONERADO: venta.total_exoneradas,
                INAFECTO: venta.total_inafecta,
                EXPORTACION: venta.total_exportacion,
                GRATUITAS: venta.total_gratuitas,
                MONTO_CARGO_X_ASIG: "0",
                CARGO_X_ASIGNACION: "0",
                ISC: venta.total_isc,
                IGV: venta.total_igv,
                OTROS: venta.total_otr_imp
            }]
        };

        axios.post(ruta, data)
        .then(function(response) {
            console.log(response.data);
        })
        
    });

});

return;

cron.schedule('*/2 * * * *', () => {
    readVentas(connection, (result) => {
        result.map((venta, index) => {
            let tipo_proceso;

            console.log(index + 1);

            const ruta = "http://localhost/sis_facturador/api_facturacion/resumen_boletas.php";

            if (venta.tipo_envio_sunat == 'prueba') {
                tipo_proceso = "3";
            } else {
                tipo_proceso = "1";
            }

            let date = new Date();
            let year = date.getFullYear();
            let month = date.getMonth() + 1;
            let day = date.getDate();

            let date_v = new Date(venta.fecha_comprobante);
            let year_v = date_v.getFullYear();
            let month_v = date_v.getMonth() + 1;
            let day_v = date_v.getDate();

            if (month < 10) {
                month = "0"+month;
            }

            if (day <  10) {
                day = "0"+day;
            }

            if (month_v < 10) {
                month_v = "0"+month_v;
            }

            if (day_v <  10) {
                day_v = "0"+day_v;
            }

            let serie = year+""+month+""+day;
            let fecha = year_v+"-"+month_v+"-"+day_v;
            let fecha_doc = year+"-"+month+"-"+day;

            let envio = venta.tipo_envio_sunat;
            let serie_resumen = serie;
            let id_con = venta.id_contribuyente;

            let venta_serie = venta.serie_comprobante;
            let venta_numero = venta.numero_comprobante;
            let ruc_empresa = venta.ruc;

            let data = {
                codigo: "RC",
                serie: serie,
                secuencia: (index + 1),
                fecha_referencia: fecha,
                fecha_documento: fecha_doc,
                tipo_proceso: tipo_proceso,
                firma: venta.pass_certificado,
                emisor: {
                    ruc: venta.ruc,
                    tipo_doc: "6",
                    nom_comercial: venta.nombreComercial,
                    razon_social: venta.empresa,
                    codigo_ubigeo: venta.codigo_ubigeo,
                    direccion: venta.direccion_fiscal,
                    direccion_departamento: venta.departamento,
                    direccion_provincia: venta.provincia,
                    direccion_distrito: venta.distrito,
                    direccion_codigopais: "PE",
                    usuariosol: venta.usuario_sol,
                    clavesol: venta.clave_sol
                },
                detalle: [{
                    ITEM: 1,
                    TIPO_COMPROBANTE: "03",
                    NRO_COMPROBANTE: `${venta.serie_comprobante}-${venta.numero_comprobante}`,
                    NRO_DOCUMENTO: venta.num_doc,
                    TIPO_DOCUMENTO: "1",
                    NRO_COMPROBANTE_REF: "0",
                    TIPO_COMPROBANTE_REF: "0",
                    STATUS: "1",
                    COD_MONEDA: "PEN",
                    TOTAL: venta.total,
                    GRAVADA: venta.total_gravadas,
                    EXONERADO: venta.total_exoneradas,
                    INAFECTO: venta.total_inafecta,
                    EXPORTACION: venta.total_exportacion,
                    GRATUITAS: venta.total_gratuitas,
                    MONTO_CARGO_X_ASIG: "0",
                    CARGO_X_ASIGNACION: "0",
                    ISC: venta.total_isc,
                    IGV: venta.total_igv,
                    OTROS: venta.total_otr_imp
                }]
            };
            
            /*axios.post(ruta,data)
            .then(function (response) {
                    // handle success
                    return response;
            })
            .catch(function (error) {
                    // handle error
                    console.log(error);
            })
            .then(function (response) {
                console.log(response.data);
                console.log(index + 1);
            })*/



        })
    })

})

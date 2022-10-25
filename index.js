const cron = require('node-cron');
const mysql = require('mysql');

const convertir = require('numero-a-letras');

const axios = require('axios').default;

function traer_secuencia_resumen_boletas(connection, data, callback) {
    connection.query("SELECT COUNT(*) as resumenes FROM resumen_boletas WHERE id_contribuyente = " + data.id_contribuyente + " AND serie = "+data.serie + " AND tipo_envio_sunat = '"+ data.tipo_envio_sunat+" ' ",function(err,result){
        if(err) throw err;
        callback(result);
    });
}

function update_doc_electronico(connection, data, callback) {
    let updateQuery = "UPDATE doc_electronico SET estado_envio_sunat = ?,hash_cpe = ?, hash_cdr = ?, cod_sunat = ?, msje_sunat = ?, ruta_xml = ?, name_cdr = ?, name_cdr_zip = ?, intentos_envio_sunat = ?, estado_documento = ?, rb_id_contribuyente = ?, rb_codigo = ?, rb_serie = ?, rb_secuencia = ?, rb_tipo_envio_sunat = ? WHERE id_contribuyente = ? AND id_tipodoc_electronico = ? AND serie_comprobante = ? AND numero_comprobante = ? AND tipo_envio_sunat = ?";
    let query = mysql.format(updateQuery, [data.estado_envio_sunat, data.hash_cpe, data.hash_cdr, data.cod_sunat, data.msje_sunat, data.ruta_xml, data.name_cdr, data.name_cdr_zip, data.intentos_envio_sunat, data.estado_documento, data.rb_id_contribuyente, data.rb_codigo, data.rb_serie, data.rb_secuencia, data.rb_tipo_envio_sunat, data.id_contribuyente, data.id_tipodoc_electronico, data.serie_comprobante, data.numero_comprobante, data.tipo_envio_sunat]);

    connection.query(query, function(err, result) {
        if (err) throw err;
        callback(result);
    });
}

function update_resumen_diario(connection, data, callback) {
    let udpdateQuery = "UPDATE resumen_boletas SET estado_envio_sunat = ?, hash_cdr = ?, cod_sunat = ?, msje_sunat = ?, name_cdr = ?, name_cdr_zip = ?, intentos_envio_sunat = ?, estado_documento = ?, accion_sunat = ? WHERE id_contribuyente = ? AND serie = ? AND secuencia = ? AND tipo_envio_sunat = ?";
    let query = mysql.format(udpdateQuery, [data.estado_envio_sunat, data.hash_cdr, data.cod_sunat, data.msje_sunat, data.name_cdr, data.name_cdr_zip, data.intentos_envio_sunat, data.estado_documento, data.accion_sunat, data.id_contribuyente, data.serie, data.secuencia, data.tipo_envio_sunat]);

    connection.query(query, function(err, result) {
        if (err) throw err;
        callback(result);
    });
}

function insertar_resumen(connection, data, callback){
    let sql = `INSERT INTO resumen_boletas (id_contribuyente, codigo, serie, secuencia, tipo_envio_sunat, fecha_documentos, fecha_envio_sunat, estado_envio_sunat, hash_cpe, hash_cdr, numero_ticket, cod_sunat, msje_sunat, ruta_xml, name_xml, name_xml_zip, name_cdr, name_cdr_zip, intentos_envio_sunat, estado_documento, accion_sunat, detalle) VALUES (${data.id_contribuyente}, '${data.codigo}', '${data.serie}', '${data.secuencia}', '${data.tipo_envio_sunat}', '${data.fecha_documentos}', '${data.fecha_envio_sunat}', '${data.estado_envio_sunat}', '${data.hash_cpe}', '${data.hash_cdr}', '${data.numero_ticket}', '${data.cod_sunat}', '${data.msje_sunat}', '${data.ruta_xml}', '${data.name_xml}', '${data.name_xml_zip}', '${data.name_cdr}', '${data.name_cdr_zip}', ${data.intentos_envio_sunat}, '${data.estado_documento}', ${data.accion_sunat}, '')`;

    connection.query(sql, function (err, result) {
        if (err) throw err;
        return 1;
    });
}

function verificar_si_existe(connection, data, callback) {
    connection.query(`SELECT COUNT(*) as total FROM resumen_boletas where id_contribuyente = ${data.id_contribuyente} AND serie = ${data.serie} AND secuencia = ${data.secuencia} AND tipo_envio_sunat = '${data.tipo_envio_sunat}'`, function(err, result) {
        if(err) throw err;
        callback(result);
    });
}

function readVentas(connection, callback) {
    connection.query("SELECT *, contribuyente.razon_social as empresa, contribuyente.nombre_comercial as nombreComercial, contribuyente.direccion_fiscal as direccion_empresa FROM doc_electronico INNER JOIN contribuyente ON contribuyente.id_contribuyente = doc_electronico.id_contribuyente INNER JOIN sunat_codigoubigeo ON sunat_codigoubigeo.codigo_ubigeo = contribuyente.codigo_ubigeo INNER JOIN cliente ON cliente.idcliente = doc_electronico.idcliente INNER JOIN sucursal ON sucursal.id_contribuyente = contribuyente.id_contribuyente WHERE  (doc_electronico.id_tipodoc_electronico = '03' OR  doc_electronico.id_tipodoc_electronico = '03') AND doc_electronico.tipo_envio_sunat = 'produccion' AND (doc_electronico.estado_envio_sunat = 'pendiente' OR doc_electronico.estado_envio_sunat = 'ticket') LIMIT 0,1", function(err, result) {
        if(err) throw err;
        callback(result);
    });
}

function detalle_doc_electronico(connection, data, callback) {
    connection.query(`SELECT * FROM detalle_doc WHERE detalle_doc.id_contribuyente = ${data.id_contribuyente} AND detalle_doc.id_tipodoc_electronico = '${data.id_tipodoc_electronico}' AND detalle_doc.serie_comprobante = '${data.serie_comprobante}' AND detalle_doc.numero_comprobante = ${data.numero_comprobante} AND tipo_envio_sunat = '${data.tipo_envio_sunat}'`,function(err,result){
        if(err) throw err;
        callback(result);
    });
}

function envio_facturas(data) {
    let data_detalle = {
        id_contribuyente: data.id_contribuyente,
        id_tipodoc_electronico: data.id_tipodoc_electronico,
        serie_comprobante: data.serie_comprobante,
        numero_comprobante: data.numero_comprobante,
        tipo_envio_sunat: data.tipo_envio_sunat
    };

    let envio = data.tipo_envio_sunat;
    let ruc = data.ruc;

    let date = new Date();
    let year = date.getFullYear();
    let month = date.getMonth() + 1;
    let day = date.getDate();

    if (month < 10) {
        month = "0"+month;
    }

    if (day <  10) {
        day = "0"+day;
    }
    
    let fecha_doc = year+"-"+month+"-"+day;

    detalle_doc_electronico(connection, data_detalle, (result) => {
        let detalle = [];

        let fecha_c = new Date(data.fecha_comprobante);
        let fecha_venc = new Date(data.fecha_vto_comprobante);

        let year_c = fecha_c.getFullYear();
        let month_c = fecha_c.getMonth() + 1;
        let day_c = fecha_c.getDate();

        let year_ven = fecha_venc.getFullYear();
        let month_ven = fecha_venc.getMonth() + 1;
        let day_ven = fecha_venc.getDate();

        if (month_c < 10) {
            month_c = "0"+month_c
        }

        if (month_ven < 10) {
            month_ven = "0"+month_ven;
        }

        if (day_c < 10) {
            day_c = "0"+day_c
        }

        if (day_ven < 10) {
            day_ven = "0"+day_ven;
        }

        let f_comp = `${year_c}-${month_c}-${day_c}`;
        let f_venc = `${year_ven}-${month_ven}-${day_ven}`;

        result.forEach((detail, index) => {
            let estado_icbper;
            if (detail.icbper > 0) {
                estado_icbper = 1;
            } else {
                estado_icbper = 0;
            }

            let datos_detalle = {
                txtITEM: (index + 1),
                txtUNIDAD_MEDIDA_DET: detail.unidad_medida,
                txtCANTIDAD_DET: detail.cantidad,
                txtPRECIO_DET: detail.precio,
                txtSUB_TOTAL_DET: detail.sub_total,
                txtPRECIO_TIPO_CODIGO: detail.id_codigoprecio,
                txtIGV: detail.igv,
                txtISC: detail.isc,
                txtIMPORTE_DET: detail.importe,
                txtCOD_TIPO_OPERACION: detail.id_tipoafectacionigv,
                txtCODIGO_DET: detail.codigo_producto,
                txtDESCRIPCION_DET: detail.descripcion,
                txtPRECIO_SIN_IGV_DET: detail.precio_sin_igv,
                txtESTADO_ICBPER: estado_icbper,
                textITEM_DESCUENTO: "0",
                textMONTO_BASE: detail.precio,
                textFACTOR: "0",
                txtCODIGO_PROD_SUNAT: "23251602"
            };

            detalle.push(datos_detalle);
        });

        let tipo_pago;
        let tipo_proceso;

        if (data.tipo_venta == 'contado') {
            tipo_pago = 1;
        } else {
            tipo_pago = 2;
        }

        if (data.tipo_envio_sunat == 'prueba') {
            tipo_proceso = "3";
        } else {
            tipo_proceso = "1";
        }

        const datos = {
            tipo_operacion: data.id_tipo_operacion,
            total_gravadas: data.total_gravadas,
            total_inafecta: data.total_inafecta,
            total_exoneradas: data.total_exoneradas,
            total_gratuitas: data.total_gratuitas,
            total_exportacion: data.total_exportacion,
            total_descuento: data.total_descuento,
            sub_total: data.sub_total,
            porcentaje_igv: data.porcentaje_igv,
            total_igv: data.total_igv,
            total_isc: data.total_isc,
            total_otr_imp: data.total_otr_imp,
            total: data.total,
            total_letras: data.total_letras,
            nro_guia_remision: data.nro_guia_remision,
            cod_guia_remision: data.cod_guia_remision,
            nro_otr_comprobante: data.nro_otr_comprobante,
            serie_comprobante: data.serie_comprobante,
            numero_comprobante: data.numero_comprobante,
            fecha_comprobante: f_comp,
            fecha_vto_comprobante: f_venc,
            cod_tipo_documento: data.id_tipodoc_electronico,
            cod_moneda: data.id_codigomoneda,
            tipo_proceso: tipo_proceso,
            pass_firma: data.pass_certificado,
            monto_icbper: data.total_icbper,
            impuesto_icbper: data.impuesto_icbper,
            tipo_pago: tipo_pago,
            cuotas: [],
            anexo: data.codigo,
            cliente_numerodocumento: data.num_doc,
            cliente_nombre: data.razon_social,
            cliente_tipodocumento: data.id_tipodocidentidad,
            cliente_direccion: data.direccion_fiscal,
            cliente_pais: "PE",
            cliente_ciudad: "",
            cliente_codigoubigeo: "",
            cliente_departamento: "",
            cliente_provincia: "",
            cliente_distrito: "",
            emisor: 
                {
                    ruc: data.ruc,
                    tipo_doc: "6",
                    nom_comercial: data.nombreComercial,
                    razon_social: data.empresa,
                    codigo_ubigeo: data.codigo_ubigeo,
                    direccion: data.direccion_empresa,
                    direccion_departamento: data.departamento,
                    direccion_provincia: data.provincia,
                    direccion_distrito: data.distrito,
                    direccion_codigopais: "PE",
                    usuariosol: data.usuario_sol,
                    clavesol: data.clave_sol
                }
            ,
            detalle: detalle
        };

        const ruta = "https://esfacturador.com/sis_facturador/api_facturacion/factura.php";

        axios.post(ruta,datos)
        .then(function (response) {
            // handle success
            return response;
        })
        .catch(function (error) {
            // handle error
            console.log(error);
        })
        .then(function (response) {
            //console.log(response.data)
            const res = response.data;
            if (res.respuesta == 'ok') {

                const ruta_xml = `/home/esfacturado/.files_contribuyente/${ruc}/${envio}/`;
                const name_xml = `${ruc}-01-${data.serie_comprobante}-${data.numero_comprobante}.xml`;
                const name_xml_zip = `${ruc}-01-${data.serie_comprobante}-${data.numero_comprobante}.zip`;
                const name_cdr = `R-${ruc}-01-${data.serie_comprobante}-${data.numero_comprobante}.xml`;
                const name_cdr_zip = `R-${ruc}-01-${data.serie_comprobante}-${data.numero_comprobante}.zip`;

                let update = {
                    fecha_envio_sunat: fecha_doc,
                    estado_envio_sunat: 'aceptado', 
                    hash_cpe: res.hash_cpe, 
                    hash_cdr: res.hash_cdr, 
                    cod_sunat: 0, 
                    msje_sunat: res.msj_sunat, 
                    ruta_xml: ruta_xml, 
                    name_xml: name_xml, 
                    name_xml_zip: name_xml_zip, 
                    name_cdr: name_cdr, 
                    name_cdr_zip: name_cdr_zip, 
                    intentos_envio_sunat: 1, 
                    estado_documento: 'activo', 
                    rb_id_contribuyente: '', 
                    rb_codigo: '', 
                    rb_serie: '', 
                    rb_secuencia: '', 
                    rb_tipo_envio_sunat: '', 
                    id_contribuyente: data.id_contribuyente, 
                    id_tipodoc_electronico: '01', 
                    serie_comprobante: data.serie_comprobante, 
                    numero_comprobante: data.numero_comprobante,
                    tipo_envio_sunat: data.tipo_envio_sunat
                };

                console.log(update);

                update_doc_electronico(connection, update, (resu) => {
                    console.log('actualizado correctamente')
                })
            } else {
                console.log(response.data);
            }
        })
    })

}

cron.schedule('*/2 * * * *', () => {

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

    readVentas(connection, (result) => {
        //const ruta = "https://esconsultoresyasesores.com/sis_facturacion/api_facturacion/resumen_boletas.php";
        


        result.forEach( (venta, index) => {
            let tipo_proceso;
            let ruc;
            let firma;
            let usuariosol;
            let clavesol;

            const ruta = "https://esfacturador.com/sis_facturador/api_facturacion/resumen_boletas.php";

            if (venta.tipo_envio_sunat == 'prueba') {
                //ruc = '20100066603';
                tipo_proceso = "3";
                //firma = '123456';
                //usuariosol = 'MODDATOS';
                //clavesol = 'moddatos';
            } else {
                //ruc = venta.ruc;
                tipo_proceso = "1";
                //firma = venta.pass_certificado;
                //usuariosol = venta.usuario_sol;
                //clavesol = venta.clave_sol;
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

            let serie = year_v+""+month_v+""+day_v;
            let fecha = year_v+"-"+month_v+"-"+day_v;
            let fecha_doc = year+"-"+month+"-"+day;

            let envio = venta.tipo_envio_sunat;
            let serie_resumen = serie;
            let id_con = venta.id_contribuyente;

            let venta_serie = venta.serie_comprobante;
            let venta_numero = venta.numero_comprobante;
            let ruc_empresa = venta.ruc;

            if (venta.id_tipodoc_electronico == '03') {

                if (venta.estado_envio_sunat === 'ticket') {
                    
                    let data = {
                        codigo: "RC",
                        serie: venta.rb_serie,
                        secuencia: venta.rb_secuencia,
                        fecha_referencia: fecha,
                        fecha_documento: fecha,
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

                    //console.log(data);return;

                    axios.post(ruta,data)
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
                        console.log(venta.numero_comprobante);
                        console.log('ticket');
                        console.log(venta.empresa);
                        const res = response.data;

                        if (res.respuesta === 'ok') {

                            if (res.resp_consulta_ticket === 'ok') {

                                let textCodigoSunat = res.resp_error_consult_ticket;

                                let explode = textCodigoSunat.split(":");

                                let textCod = explode[1].split(" ");

                                let cod_sunat = textCod[1];

                                let msn = res.msj_sunat;

                                if (cod_sunat == '2223') {
                                    msn = "El archivo ya fue presentado anteriormente - El archivo ya fue presentado anteriormente";
                                }

                                if (cod_sunat == '2282') {
                                    msn = "El archivo ya fue presentado anteriormente - El archivo ya fue presentado anteriormente";
                                }

                                const ruta_xml = `/home/esfacturado/.files_contribuyente/${ruc_empresa}/${envio}/`;
                                const name_cdr = `R-${ruc_empresa}-RC-${venta.rb_serie}-${venta.rb_secuencia}.xml`;
                                const name_cdr_zip = `R-${ruc_empresa}-RC-${venta.rb_serie}-${venta.rb_secuencia}.zip`;

                                let update = {
                                    fecha_envio_sunat: fecha_doc,
                                    estado_envio_sunat: 'aceptado', 
                                    hash_cpe: res.hash_cpe, 
                                    hash_cdr: res.hash_cdr, 
                                    cod_sunat: 0, 
                                    msje_sunat: msn, 
                                    ruta_xml: ruta_xml, 
                                    name_cdr: name_cdr, 
                                    name_cdr_zip: name_cdr_zip, 
                                    intentos_envio_sunat: 1, 
                                    estado_documento: 'activo', 
                                    rb_id_contribuyente: id_con, 
                                    rb_codigo: 'RC', 
                                    rb_serie: venta.rb_serie, 
                                    rb_secuencia: venta.rb_secuencia, 
                                    rb_tipo_envio_sunat: envio, 
                                    id_contribuyente: id_con, 
                                    id_tipodoc_electronico: '03', 
                                    serie_comprobante: venta_serie, 
                                    numero_comprobante: venta_numero,
                                    tipo_envio_sunat: venta.tipo_envio_sunat
                                };
    
                                update_doc_electronico(connection, update, (resu) => {
                                    console.log('actualizado correctamente')
                                })

                                let rb_update = {
                                    estado_envio_sunat: 'aceptado', 
                                    hash_cdr: res.hash_cdr, 
                                    cod_sunat: 0, 
                                    msje_sunat: msn, 
                                    name_cdr: name_cdr, 
                                    name_cdr_zip: name_cdr_zip, 
                                    intentos_envio_sunat: 1, 
                                    estado_documento: 'acivo', 
                                    accion_sunat: 1, 
                                    id_contribuyente: id_con, 
                                    serie: venta.rb_serie, 
                                    secuencia: venta.rb_secuencia, 
                                    tipo_envio_sunat: venta.tipo_envio_sunat
                                };

                                update_resumen_diario(connection, rb_update, (re) => {
                                    console.log('actualizado resumen diario');
                                })
                            }
                        }
                    })

                } else {
                    traer_secuencia_resumen_boletas(connection,{id_contribuyente: id_con,serie: serie_resumen, tipo_envio_sunat: envio}, (resultado) => {
                        var resumen_secuencia = resultado[0].resumenes + 1;
                            
                                let data = {
                                    codigo: "RC",
                                    serie: serie,
                                    secuencia: resumen_secuencia,
                                    fecha_referencia: fecha,
                                    fecha_documento: fecha,
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

                                //console.log(data); return;
                    
                                axios.post(ruta,data)
                                    .then(function (response) {
                                            // handle success
                                            return response;
                                    })
                                    .catch(function (error) {
                                            // handle error
                                            console.log(error);
                                    })
                                    .then(function (response) {
                                        //console.log(data);
                                        console.log(response.data);
                                        console.log(venta.numero_comprobante);
                                        console.log('pendiente');
                                        console.log(venta.empresa);
                                        const res = response.data;
                
                                        if (res.respuesta === 'ok') {
    
                                            console.log('primera entrada');
    
                                            if (res.resp_consulta_ticket === 'ok') {
                                                console.log("segunda entrada");
                                                const ruta_xml = `/home/esfacturado/.files_contribuyente/${ruc_empresa}/${envio}/`;
                                                const name_xml = `${ruc_empresa}-RC-${serie_resumen}-${resumen_secuencia}.xml`;
                                                const name_xml_zip = `${ruc_empresa}-RC-${serie_resumen}-${resumen_secuencia}.zip`;
                                                const name_cdr = `R-${ruc_empresa}-RC-${serie_resumen}-${resumen_secuencia}.xml`;
                                                const name_cdr_zip = `R-${ruc_empresa}-RC-${serie_resumen}-${resumen_secuencia}.zip`;

                                                let textCodigoSunat = res.resp_error_consult_ticket;

                                                let explode = textCodigoSunat.split(":");

                                                let textCod = explode[1].split(" ");

                                                let cod_sunat = textCod[1];

                                                let msn = res.msj_sunat;

                                                if (cod_sunat == '2223' || cod_sunat == '2282') {
                                                    msn = "El archivo ya fue presentado anteriormente - El archivo ya fue presentado anteriormente";

                                                    let rb_update = {
                                                        estado_envio_sunat: 'aceptado', 
                                                        hash_cdr: res.hash_cdr, 
                                                        cod_sunat: 0, 
                                                        msje_sunat: msn, 
                                                        name_cdr: name_cdr, 
                                                        name_cdr_zip: name_cdr_zip, 
                                                        intentos_envio_sunat: 1, 
                                                        estado_documento: 'acivo', 
                                                        accion_sunat: 1, 
                                                        id_contribuyente: id_con, 
                                                        serie: venta.rb_serie, 
                                                        secuencia: venta.rb_secuencia, 
                                                        tipo_envio_sunat: venta.tipo_envio_sunat
                                                    };
                    
                                                    update_resumen_diario(connection, rb_update, (re) => {
                                                        console.log('actualizado resumen diario');
                                                    })

                                                }

                                                if (cod_sunat == '0') {
                                                    const insert = {
                                                        id_contribuyente: id_con,
                                                        codigo: 'RC',
                                                        serie: serie_resumen,
                                                        secuencia: resumen_secuencia,
                                                        tipo_envio_sunat: envio,
                                                        fecha_documentos: fecha,
                                                        fecha_envio_sunat: fecha_doc,
                                                        estado_envio_sunat: 'aceptado',
                                                        hash_cpe: res.hash_cpe,
                                                        hash_cdr: res.hash_cdr,
                                                        numero_ticket: '',
                                                        cod_sunat: 0,
                                                        msje_sunat: msn,
                                                        ruta_xml: ruta_xml,
                                                        name_xml: name_xml,
                                                        name_xml_zip: name_xml_zip,
                                                        name_cdr: name_cdr,
                                                        name_cdr_zip: name_cdr_zip,
                                                        intentos_envio_sunat: 1,
                                                        estado_documento: 'activo',
                                                        accion_sunat: 1,
                                                        detalle: ""
                                                    };
        
                                                    verificar_si_existe(connection, {id_contribuyente: id_con,serie: serie_resumen, secuencia: resumen_secuencia, tipo_envio_sunat: envio}, (r) => {
                                                        var r = r[0].total;
        
                                                        if (r == 0) {
                                                            console.log('tercera entrada');
                                                            insertar_resumen(connection, insert, (resu) => {
        
                                                            })
        
                                                            
                                                        }
        
                                                    });
                                                }

                                                let update = {
                                                    fecha_envio_sunat: fecha_doc,
                                                    estado_envio_sunat: 'aceptado', 
                                                    hash_cpe: res.hash_cpe, 
                                                    hash_cdr: res.hash_cdr, 
                                                    cod_sunat: 0, 
                                                    msje_sunat: msn, 
                                                    ruta_xml: ruta_xml, 
                                                    name_cdr: name_cdr, 
                                                    name_cdr_zip: name_cdr_zip, 
                                                    intentos_envio_sunat: 1, 
                                                    estado_documento: 'activo', 
                                                    rb_id_contribuyente: id_con, 
                                                    rb_codigo: 'RC', 
                                                    rb_serie: serie_resumen, 
                                                    rb_secuencia: resumen_secuencia, 
                                                    rb_tipo_envio_sunat: envio, 
                                                    id_contribuyente: id_con, 
                                                    id_tipodoc_electronico: '03', 
                                                    serie_comprobante: venta_serie, 
                                                    numero_comprobante: venta_numero,
                                                    tipo_envio_sunat: venta.tipo_envio_sunat
                                                };
                    
                                                update_doc_electronico(connection, update, (resu) => {
                                                    console.log('actualizado correctamente')
                                                })
                    
                                                
                                            }
                                            
                
                                        }
                                        
                                    })
                            
                    })
                }
                
            } else {
                envio_facturas(venta);
            }


        });
    });
});
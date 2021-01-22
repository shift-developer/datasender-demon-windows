const config = {
    user: 'user',
    password: 'password',
    server: 'SERVER', 
    database: 'database',
    options: {
        encrypt: false
    },
    token: 'token'
};

const sql = require('mssql');
const moment = require('moment');
let EventLogger2 = require('node-windows').EventLogger;var log2 = new EventLogger2('datasender');log2.info('REWORK');
let dbfb = firebaseapp.database();
let Parse = require('parse/node');Parse.initialize("ejemploid");
const _http = require('follow-redirects').http;
const fs = require('fs');

dbfb.ref('entorno-produccion').once('value', (snapshot) => {
    const ip = snapshot.val();
    Parse.serverURL = ip.ip;

try { sql.close(); } catch (e) { console.log('its closed') }

const savedData = {
    clients: false,
    articles: false,
    priceList: false,
    sellers: false,
    routes: false,
    banks: false,
    deposits: false,
    salesCondition: false,
    stock: false,
    vats: false,
    currentAccount: false,
    orders: false,
    collections: false
}

const validateArray = function (x, y) {
    return (
        Array.isArray(x) ?
            x[y ? y : 0]
                ?
                x[y ? y : 0].replace(/\//g, "") :
                'a' :
            x ?
                x :
                'a'
    );
};

const sendToApi=(_tabla,_datos,_action,fn,fn2)=>{

    const options = {
        'method': 'POST',
        'hostname': ip.api,
        'port': ip.p,
        'path': '/'+(_tabla.charAt(0).toUpperCase() + _tabla.slice(1))+'/'+config.token,
        'headers': {
        'Content-Type': 'application/json'
        },
        'maxRedirects': 20
    };

    const req = _http.request(options, function (res) {
        const chunks = [];

        res.on("data", function (chunk) {
            chunks.push(chunk);
        });

        res.on("end", function (chunk) {
            var body = Buffer.concat(chunks);
            var response=JSON.parse(body.toString())
            if(response.error){
                console.log(response.error)
            fn(true)
            }
            fn()
        });

        res.on("error", function (error) {
            console.log('error send to api',error)
        });
    });

    const postData = JSON.stringify({[_action]:_datos});
    req.write(postData);

    req.end();
}

const sendData = async function(ref, dataSend, a, fn, fn2) {

  let saveObjects = [];
  let changeObjects = [];
  let deleteObjects = [];
  
  if (a == 'set') {
    for (var i in dataSend) {
      dataSend[i].createdAt = Date.now()
    }
    sendToApi(ref,dataSend,'created',function(error) {
      if (error) {
        fn2()
      } else {
        fn()
      }
    })
  
  } else if (a == "update") {
    try {
      var updates = {}

      for (var i in dataSend) {
        var elem = dataSend[i]
        elem.updatedAt = Date.now()
        updates["/" + ref + "/" + elem.k] = elem
      }
       sendToApi(ref,dataSend,'updated',function(error) {
      if (error) {
        fn2()
      } else {
        fn()
      }
    })
  
    } catch (e) {
      fn2()
    }
  } else if (a == "delete") {
    
    for (var i in dataSend) {
        dataSend[i]={k:i}
    }
   sendToApi(ref,dataSend,'deleted',function(error) {
      if (error) {
        fn2()
      } else {
        fn()
      }
    })
  
  }
};
const changesControl = (x, y, z) => {

    const d = y;
    const ds = JSON.stringify(y);
    fs.readFile('files/' + x, function (err, res) {

        if (err) {

            console.log('err__',x)
            sendData(x, d, 'set', function () {
                saveFile(x, JSON.parse(ds));
            },()=>{
                savedData[x] = true
            });
        } else {


            var st = {
                a1: false,
                a2: false,
                a3: false
            }
            function tryEnd() {
                var ready = true

                for (var i in st) {
                    if (st[i] === false) {
                        ready = false
                    }
                }

                if (ready) {
                    saveFile(x, JSON.parse(ds));
                }
            }
            const c = { changes: {} };
            const cre = { changes: {} };
            const del = { changes: {} };
            let r
            try {

                r = JSON.parse(res.toString());

            }
            catch (e) {

                r = {}
            }
            if (!y || Object.keys(y).length < 1 || Object.keys(y)[0] == "undefined" || y[Object.keys(y)[0]] == "undefined" || ! y[Object.keys(y)[0]]) {

                savedData[x] = true
                return false
            }
            else {
                if (JSON.stringify(r) == JSON.stringify(d)) {
                    console.log('equals', x)
                    savedData[x] = true

                } else {

                    //compare a->b
                    for (let i in d) {
                        if (r[i]) {
                            if (JSON.stringify(r[i]) == JSON.stringify(d[i])) {
                            } else {

                                c.changes[i] = d[i];
                            }
                        } else {

                            cre.changes[i] = d[i];
                        }
                    }
                    for (let i in r) {
                        if (d[i]) {
                        } else {
                            del.changes[i] = null;
                            delete c.changes[i]
                        }
                    }

                    if (Object.keys(cre.changes).length > 0) {
                        console.log('__2',x,Object.keys(cre.changes).length)

                        sendData(x, cre.changes, 'set', function () {
                            st.a1 = true
                            tryEnd()

                        }, function () {
                            savedData[x] = true
                        });
                    }
                    else {
                        st.a1 = true
                        tryEnd()

                    }
                    if (Object.keys(c.changes).length > 0) {
                        console.log('__3',x,Object.keys(c.changes).length)
                        sendData(x, c.changes, 'update', function () {
                            st.a2 = true

                            if (x == "clients") {
                                tryEnd(1)
                            }
                            else {
                                tryEnd()

                            }
                            st.a2 = true

                        },
                            function () {

                                savedData[x] = true
                            });
                    }
                    else {
                        st.a2 = true
                        tryEnd()

                    }
                    if (Object.keys(del.changes).length > 0) {
                        console.log('__5',x,Object.keys(del.changes).length)
                        sendData(x, del.changes, 'delete', function () {
                            st.a3 = true
                            tryEnd()

                        },
                            function () {

                                savedData[x] = true
                            });
                    }
                    else {
                        st.a3 = true
                        tryEnd()

                    }

                }
            }
        }
    });
}

const saveFile = (x, y) => {

    fs.writeFile('files/' + x, JSON.stringify(y), function (err) {
        savedData[x] = true
        if (err) {
            return console.log(err);
        }
    });
}

const executeQuery = async (x, y, z, ka) => {
    try {
        let p = z;
        let r = await p.request().query(x);
        let rs = r.recordset;
        y(rs, ka);
    } catch (e) {
        y(e)
    }
}
let data = {
    clients: {},
    articles: {},
    priceList: {},
    sellers: {},
    routes: {},
    banks: {},
    deposits: {},
    reasonsNotBuy: {},
    salesCondition: {},
    stock: {},
    apiSalesClients: {},
    groupers: {},
    apiSalesArticles: {},
    currentAccount: {},
    // packs: {},
    vats: {},
    brands: {},
};
const v = (x, y) => {
    const p = y.split('.');
    let val = x;
    for (let i in p) {
        if (val[p[i]] == undefined) {
            return '';
        } else {
            val = val[p[i]];
        }
    }
    return val;
}
(async function () {
    try {
        let pool = await sql.connect(config);
        executeQuery("select * from CLIENTES as CL left join ALICUOTA_INGBRU as AIB on CL.AIB_CODIGO=AIB.AIB_CODIGO where CL.ES_CODIGO='01' order by CL.CL_CODIGO asc ", function (rs) {
            for (let i in rs) {
                let a = rs[i];
                (data && data.clients) &&
                    (data.clients[a.CL_CODIGO] = {
                        k: "" + a.CL_CODIGO,
                        n: a.CL_NOMBRE,
                        it: a.CL_TIPDOC,
                        i: a.CL_DOCUME,
                        bn: a.CL_RAZSOC,
                        pl: a.LP_CODIGO,
                        sc: a.CVE_CODIGO,
                        s: a.VEN_CODIGO ? {
                            [a.VEN_CODIGO]: true
                        } : null,
                        f: {
                            c: a.CL_CUIT,
                            v: a.CL_IVA,
                        },
                        a: {
                            "a": {
                                a: a.CL_DIRECC,
                                n: a.CL_BARRIO,
                                s: a.CL_LOCALI,
                                z: a.CL_CODPOS
                            }
                        },
                        e: {
                            "a": {
                                e: a.CL_EMAIL
                            }
                        },
                       
                        p: {}
                    });
            };
            executeQuery("select * from CLIENTES_TELEFONO left join TIPO_TELEFONO on CLIENTES_TELEFONO.TT_CODIGO=TIPO_TELEFONO.TT_CODIGO", function (rs2) {
                for (let i in rs2) {
                    let d = rs2[i];
                    (data && data.clients && data.clients[d.CL_CODIGO] && data.clients[d.CL_CODIGO].p) &&
                        (data.clients[d.CL_CODIGO].p[d.CLT_ID] = {
                            k: "" + d.CLT_ID,
                            d: d.TT_DESCRI,
                            p: d.CLT_NUMERO
                        });
                };
                changesControl('clients', data.clients);
            }, pool);
        }, pool);


        executeQuery(`SELECT 
        AR.AR_CODIGO+DE.DE_CODIGO+LP.LP_CODIGO AS mpk, AR.AR_CODIGO, 
        REPLACE(AR.AR_DESCRI,'''',' ') as AR_DESCRI, FA.FA_CODIGO, FA.FA_NOMBRE, UM.UM_CODIGO, UM.UM_DESCRI, 
        UM_CODCOM, AR.AR_EQUUM,
        CASE WHEN AR.II_CODIGO IS NOT NULL AND AR.II_CODIGO <> '' THEN AR.II_CODIGO ELSE '' END AS MA_CODIGO2, 
        '0' AS MA_DESCRI2,MA.MA_DESCRI,MA.MA_CODIGO, ST.DE_CODIGO, DE.DE_DESCRI, ST.ST_STOCK, 
        AP.LP_CODIGO, LP.LP_DESCRI, 
        REPLACE(CONVERT(VARCHAR,AP.AP_PRECIO),',','.') AS AP_PRECIO, 
        CASE WHEN ALI.ALI_PORCEN IS NULL THEN '21' ELSE ALI.ALI_PORCEN END AS ALI_PORCEN, 
     CASE WHEN AP.AP_PRECIO>0 AND II.II_PORCEN IS NOT NULL THEN 
       REPLACE(CONVERT(VARCHAR,II.II_PORCEN),',','.') 
     ELSE 
     '0' END AS II_PORCEN, 
        REPLACE(CONVERT(VARCHAR,AR.AR_COSNET),',','.') AS AR_COSNET 
        FROM ARTICULO AR
       LEFT JOIN MARCA MA on AR.MA_CODIGO = MA.MA_CODIGO
       LEFT JOIN FAMILIA FA ON AR.FA_CODIGO = FA.FA_CODIGO 
       LEFT JOIN UNIDAD_MEDIDA UM ON AR.UM_CODIGO = UM.UM_CODIGO
       LEFT JOIN STOCK ST ON AR.AR_CODIGO = ST.AR_CODIGO
       LEFT JOIN DEPOSITO DE ON DE.DE_CODIGO = ST.DE_CODIGO
       LEFT JOIN ART_PRE AP ON AP.AR_CODIGO = AR.AR_CODIGO
       LEFT JOIN LISTA_PRECIO LP ON LP.LP_CODIGO = AP.LP_CODIGO
       LEFT JOIN ALICUOTA_IVA ALI ON AR.ALI_CODVEN = ALI.ALI_CODIGO
       LEFT JOIN IMPUESTO_INTERNO II ON AR.II_CODIGO=II.II_CODIGO
       WHERE ST.DE_CODIGO IN (01)  AND  AP.LP_CODIGO IN (01,02,03,04,05) AND AR.ESA_CODIGO <> '03' `, function (rs) {



            for (let i in rs) {

                
                let c = rs[i];
                if ((data && data.groupers && (!data.groupers[validateArray(c.FA_CODIGO)]))) {


                    data.groupers[validateArray(c.FA_CODIGO)] = {
                        k: "" + validateArray(c.FA_CODIGO),
                        n: c.FA_NOMBRE,
                         i: 'https://ejemplo.com'  + validateArray(c.FA_CODIGO) + '.jpg',
                        a: {}
                    };
                }
                if ((data && data.brands && (!data.brands[validateArray(c.MA_CODIGO)]))) {


                    data.brands[validateArray(c.MA_CODIGO)] = {
                        k: "" + validateArray(c.FA_CODIGO),
                        n: c.FA_NOMBRE,
                          i: 'https://ejemplo.com/' + validateArray(c.FA_CODIGO) + '.jpg',
                        a: {}
                    };
                }
               
                if ((data && data.apiSalesArticles)) {
                    data.apiSalesArticles[c.AR_CODIGO] = {
                        k: "" + c.AR_CODIGO,
                         i: 'https://ejemplo.com/' + validateArray(c.AR_CODIGO) + '.jpg',
                        n: c.AR_DESCRI,
                        v: {
                            a: c.ALI_PORCEN ? c.ALI_PORCEN : 21,
                            f: false
                        },tx:{
                            "II": {
                                t: 0,
                                a: c.II_PORCEN,
                                f: false,
                                k: "II"
                            }
                        },
                        fa: c.FA_CODIGO,
                        dum: c.AR_UMEXACTA == 'S' ? validateArray(c.UM_CODIGO, 0) : c.UM_CODCOM,
                        w: "" + c.AR_PESO,
                        nc: c.AR_COSNET + "",
                        b: c.MA_CODIGO,
                        um: c.AR_UMEXACTA == 'S' ? {
                            [validateArray(c.UM_CODIGO)]: {
                                k: "" + validateArray(c.UM_CODIGO, 0),
                                e: 1,
                                n: validateArray(c.UM_DESCRI, 0)
                            }
                        } : {
                                [c.UM_CODCOM]: {
                                    k: "" + c.UM_CODCOM,
                                    e: c.AR_EQUUM,
                                    n: validateArray(c.UM_DESCRI, 1)
                                },
                            }
                    }
                }
                if (data.groupers[validateArray(c.FA_CODIGO)]) {
                    data.groupers[validateArray(c.FA_CODIGO)].a[validateArray(c.AR_CODIGO)] =validateArray(c.AR_DESCRI)
                    
                }
                  if (data.brands[validateArray(c.MA_CODIGO)]) {
                    data.brands[validateArray(c.MA_CODIGO)].a[validateArray(c.AR_CODIGO)] =validateArray(c.AR_DESCRI)
                    
                }

            };
             changesControl('apiSalesArticles', data.apiSalesArticles);
             changesControl('groupers', data.groupers);
             changesControl('brands', data.brands);

        }, pool);

        executeQuery(" SELECT \
        AR.AR_CODIGO+DE.DE_CODIGO+LP.LP_CODIGO AS mpk, \
        AR.AR_CODIGO, \
              REPLACE(AR.AR_DESCRI,'''',' ') as AR_DESCRI, \
        FA.FA_CODIGO, FA.FA_NOMBRE, UM.UM_CODIGO, UM.UM_DESCRI, \
              UM_CODCOM, AR.AR_EQUUM, \
              CASE WHEN AR.II_CODIGO IS NOT NULL \
       AND AR.II_CODIGO <> '' THEN AR.II_CODIGO ELSE '' END AS MA_CODIGO, \
             '0' AS MA_DESCRI, ST.DE_CODIGO, DE.DE_DESCRI, ST.ST_STOCK, \
              AP.LP_CODIGO, LP.LP_DESCRI, \
              REPLACE(CONVERT(VARCHAR,AP.AP_PRECIO),',','.') AS AP_PRECIO, \
            CASE WHEN ALI.ALI_PORCEN IS NULL THEN '21' ELSE ALI.ALI_PORCEN END AS ALI_PORCEN, \
           CASE WHEN AP.AP_PRECIO>0 AND II.II_PORCEN IS NOT NULL \
       THEN \
             REPLACE(CONVERT(VARCHAR,II.II_PORCEN),',','.') \
           ELSE \
           '0' END AS II_PORCEN, \
              REPLACE(CONVERT(VARCHAR,AR.AR_COSNET),',','.') \
        AS AR_COSNET \
              FROM ARTICULO AR \
             LEFT JOIN \
       MARCA MA on AR.MA_CODIGO = MA.MA_CODIGO \
             LEFT JOIN \
       FAMILIA FA ON AR.FA_CODIGO = FA.FA_CODIGO \
             LEFT JOIN \
       UNIDAD_MEDIDA UM ON AR.UM_CODIGO = UM.UM_CODIGO \
             LEFT JOIN \
       STOCK ST ON AR.AR_CODIGO = ST.AR_CODIGO \
             LEFT JOIN \
       DEPOSITO DE ON DE.DE_CODIGO = ST.DE_CODIGO \
             LEFT JOIN \
       ART_PRE AP ON AP.AR_CODIGO = AR.AR_CODIGO \
             LEFT JOIN \
       LISTA_PRECIO LP ON LP.LP_CODIGO = AP.LP_CODIGO \
             LEFT JOIN \
       ALICUOTA_IVA ALI ON AR.ALI_CODVEN = ALI.ALI_CODIGO \
             LEFT JOIN \
       IMPUESTO_INTERNO II ON AR.II_CODIGO=II.II_CODIGO \
        WHERE ST.DE_CODIGO IN (01)  AND  AP.LP_CODIGO IN (01,02,03,04,05) \
       AND AR.ESA_CODIGO <> '03' "

            , function (rs) {
                for (let i in rs) {
                    let c = rs[i];
                    if (!data.articles[c.AR_CODIGO]) {
                        data.articles[c.AR_CODIGO] = {}
                    }
                      
                    data.articles[c.AR_CODIGO] = {
                        k: "" + c.AR_CODIGO,
                        n: c.AR_DESCRI,
                        v: {
                            a: c.ALI_PORCEN ? c.ALI_PORCEN : 21,
                            f: false
                        },
                        i:'https://ejemplo.com/'+ validateArray(c.AR_CODIGO) + '.jpg',
                        dum: validateArray(c.UM_CODIGO, 0),
                        w: "" + c.AR_PESO,
                        nc: "" + c.AR_COSNET,
                        b: c.MA_CODIGO,
                        fa:c.FA_CODIGO,
                        tx: {
                            "II": {
                                t: 0,
                                a: c.II_PORCEN,
                                f: false,
                                k: "II"
                            }
                        },
                        um: {
                            [validateArray(c.UM_CODIGO)]: {
                                k: "" + validateArray(c.UM_CODIGO, 0),
                                e: 1,
                                n: validateArray(c.UM_DESCRI, 0)
                            },
                            [c.UM_CODCOM]: {
                                k: "" + c.UM_CODCOM,
                                e: c.AR_EQUUM,
                                n: validateArray(c.UM_DESCRI, 1)
                            },
                        }
                    }
                };
                changesControl('articles', data.articles);
            }, pool);
        executeQuery("select * from LISTA_PRECIO", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.priceList) &&
                    (data.priceList[d.LP_CODIGO] = {
                        k: "" + d.LP_CODIGO,
                        n: d.LP_DESCRI,
                        a: {}
                    });
            };
            executeQuery("select * from ART_PRE where AR_CODIGO not like '%.%' ", function (rs2) {
                for (let i in rs2) {
                    let d = rs2[i];
                    (data && data.priceList && data.priceList[d.LP_CODIGO] && data.priceList[d.LP_CODIGO].a) &&
                        (data.priceList[d.LP_CODIGO].a[d.AR_CODIGO] = {
                            k: "" + d.AR_CODIGO,
                            u: {
                                a: d.AP_UTILID,
                                f: false
                            }
                        });
                };
                changesControl('priceList', data.priceList);
            }, pool);
        }, pool);
        executeQuery("select * from VENDEDOR as ve left join PERSONAL as pe on ve.PER_CODIGO=pe.PER_CODIGO", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.sellers) &&
                    (data.sellers[d.VEN_CODIGO] = {
                        k: "" + d.VEN_CODIGO,
                        n: d.PER_NOMBRE,
                        p: d.VEN_PASSWORD,
                        i: d.PER_DOCUME,
                        it: d.PER_TIPDOC
                    });
            };
            changesControl('sellers', data.sellers);
        }, pool);
        executeQuery("select * from HOJA_RUTA", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.routes) &&
                    (data.routes[d.HR_CODIGO] = {
                        k: "" + d.HR_CODIGO,
                        n: d.HR_DESCRI,
                        cl: {},
                        d: d.DIA_CODIGO,
                        s: {
                            [d.VEN_CODIGO]: true
                        }
                    });
            };
            executeQuery("select * from HOJA_RUTA_CLIENTE", function (rs2) {
                for (let i in rs2) {
                    let d = rs2[i];
                    (data && data.routes && data.routes[d.HR_CODIGO] && data.routes[d.HR_CODIGO].cl) &&
                        (data.routes[d.HR_CODIGO].cl[d.CL_CODIGO] = {
                            k: "" + d.CL_CODIGO,
                            o: d.HRD_ORDEN
                        });
                };
                changesControl('routes', data.routes);
            }, pool);
        }, pool);
        executeQuery("select * from BANCOS", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.banks) &&
                    (data.banks[d.BA_CODIGO] = {
                        k: "" + d.BA_CODIGO,
                        n: d.BA_NOMBRE
                    });
            };
            changesControl('banks', data.banks);
        }, pool);
        executeQuery("select * from DEPOSITO", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.deposits) &&
                    (data.deposits[d.DE_CODIGO] = {
                        k: "" + d.DE_CODIGO,
                        n: d.DE_DESCRI
                    });
            };
            changesControl('deposits', data.deposits);
        }, pool);
        executeQuery("select * from MOTIVO_NO_COMPRA", function (rs) {
            for (let i in rs) {

                let d = rs[i];
                (data && data.reasonsNotBuy) &&
                    (data.reasonsNotBuy[d.MNC_CODIGO] = {
                        k: "" + d.MNC_CODIGO,
                        n: d.MNC_DESCRI
                    });
            };
              changesControl('reasonsNotBuy', data.reasonsNotBuy);
        }, pool);
        executeQuery("select * from COND_VENTA", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.salesCondition) &&
                    (data.salesCondition[d.CVE_CODIGO] = {
                        k: "" + d.CVE_CODIGO,
                        n: d.CVE_DESCRI,
                        d: d.CVE_OBSERV,
                        mc: d.MO_CODIGO ? d.MO_CODIGO : null,
                        b: d.CVE_BONIFI ? {
                            "a": {
                                a: d.CVE_BONIFI,
                                f: false
                            },
                        } : null,
                        s: d.CVE_RECARG ? {
                            "a": {
                                a: d.CVE_RECARG,
                                f: false
                            }
                        } : null
                    });
            };
            changesControl('salesCondition', data.salesCondition);
        }, pool);
        executeQuery("select * from STOCK", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.stock) &&
                    (data.stock[d.DE_CODIGO + d.AR_CODIGO] = {
                        k: "" + d.DE_CODIGO + d.AR_CODIGO,
                        a: d.AR_CODIGO,
                        d: d.DE_CODIGO,
                        q: d.ST_STOCK
                    });
            };
              changesControl('stock', data.stock);
        }, pool);
        executeQuery("select * from CLIENTES where CL_ACCESOWEB='S'", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.apiSalesClients) &&
                    (data.apiSalesClients[d.CL_CODIGO] = {
                        k: "" + d.CL_CODIGO,
                        e: d.CL_EMAIL,
                        p: d.CL_CODIGO,
                        sp: "1"
                    });
            };
             changesControl('apiSalesClients', data.apiSalesClients);
        }, pool);
        executeQuery("select * from CONDICIONIVA", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.vats) &&
                    (data.vats[d.COI_CODIGO] = {
                        k: "" + d.COI_CODIGO,
                        n: d.COI_DESCRI
                    });
            };
            changesControl('vats', data.vats);
        }, pool);
     
        executeQuery("select * from COMPROBANTES where CO_FECANU is null and CO_FECCAN is null and CO_CODIGO in ('FAR','FAC','NCR','NDE','ANT')", function (rs) {
            for (let i in rs) {
                let d = rs[i];
                (data && data.currentAccount) &&
                    (data.currentAccount[d.CO_CODIGO + d.CO_SUCURS + d.CO_NUMERO + d.CO_TIPO] = {
                        k: "" + d.CO_CODIGO + d.CO_SUCURS + d.CO_NUMERO + d.CO_TIPO,
                        c: d.CL_CODIGO,
                        cn: d.CL_NOMBRE,
                        bn: d.CL_RAZSOC,
                        s: d.VEN_CODIGO,
                        dc: d.CO_CODIGO,
                        do: d.CO_SUCURS + "",
                        dn: d.CO_NUMERO + "",
                        dt: d.CO_TIPO,
                        dd: d.CO_DESCRI,
                        da: d.CO_TOTCOM,
                        tc: ((new Date(d.CO_FECHA)).getTime() + (3600 * 1000 * 6)),
                        te: ((new Date(d.CO_FECVEN)).getTime() + (3600 * 1000 * 6)),
                        q: {}
                    });
            };
            executeQuery("select * from COMP_CUO where CO_CODIGO+CO_SUCURS+CO_NUMERO+CO_TIPO in (select CO_CODIGO+CO_SUCURS+CO_NUMERO+CO_TIPO from COMPROBANTES where CO_FECANU is null and CO_FECCAN is null and CO_CODIGO IN ('FAR','FAC','NCR','NDE','ANT'))", function (rs) {
                for (let i in rs) {
                    let d = rs[i];
                    (data && data.currentAccount && data.currentAccount[d.CO_CODIGO + d.CO_SUCURS + d.CO_NUMERO + d.CO_TIPO] && data.currentAccount[d.CO_CODIGO + d.CO_SUCURS + d.CO_NUMERO + d.CO_TIPO].q) &&
                        (data.currentAccount[d.CO_CODIGO + d.CO_SUCURS + d.CO_NUMERO + d.CO_TIPO].q[d.COC_NUMERO] = {
                            k: "" + d.COC_NUMERO,
                            ia: d.COC_IMPORT,
                            pa: d.COC_IMPENT,
                            qn: d.COC_NUMERO,
                            te: ((new Date(d.COC_FECVEN)).getTime() + (3600 * 1000 * 6))
                        });
                };
                changesControl('currentAccount', data.currentAccount);
            }, pool);
        }, pool);
    
        const queryOrders = new Parse.Query('Orders')
        queryOrders.equalTo('envId', config.token)
            .equalTo('s', 1)
        const res = await queryOrders.find()
        orderSaved = 0
        if (Object.keys(res).length < 1) {
            savedData.orders = true
        }
        if (res) {
            for (let i in res) {
                const s = res[i].attributes;
                let query = "";
                query += "insert into DATASENDER.dbo.PEDIDOS_HNDHLD \
                    (PH_CODIGO,CL_CODIGO, PH_FECINI, PH_HORINI, PH_FECFIN, PH_HORFIN, PH_FECENT, \
                        CVE_CODIGO, PH_TOTCOM, PH_NETO, PH_CANITEM, VEN_CODIGO, PH_OBSERV, PH_BONIFI, \
                        PH_RECARG, PH_ORIGEN, EM_CODIGO, PH_TIPODOC) values \
                    ('"+ ((s.sk) || i) + "','"
                    + (s.c && s.c ? s.c.k : '') + "','"
                    + moment((s.ts||s.te)).format('YYYY-MM-DD 00:00:00') + "','"
                    + moment((s.ts||s.te)).format('HH:mm:ss') + "','"
                    + moment((s.te)).format('YYYY-MM-DD 00:00:00') + "','"
                    + moment((s.te)).format('HH:mm:ss') + "','"
                    + (moment((s.td) || (s.te)).format('YYYY-MM-DD 00:00:00')) + "','"
                    + (s.sc && s.sc.k ? s.sc.k : (s.c.sc) ? s.c.sc : "") + "','"
                    + ((s.tt.t) || 0).toFixed(4) + "','"
                    + ((s.tt.tn) || 0).toFixed(4) + "','"
                    + Object.keys((s.o)).length + "','"
                    + (s.se) + "','"
                    + (s.ob) + "','"
                    + (s.gb) + "','"
                    + (s.su ? s.su : 0) + "','"
                    + ((s.or) == 0 ? 'A' : (s.or) == 1 ? 'V' : 'A') + "','"
                    + (s.co ? s.co : '0003') + "','"
                    + "1" + "') ";
                for (let j in s.o) {
                    const t = s.o[j];

                    query += "insert into DATASENDER.dbo.PEDIDOS_HNDHLD_DET ( \
                            PH_CODIGO, \
                            AR_CODIGO, \
                            UN_CODIGO, \
                            PHD_CANTID, \
                            PHD_IMPUNI, \
                            PHD_BONIFI, \
                            PHD_IMPLIS, \
                            LP_CODIGO, \
                            HH_LINEA\
                            ) values (  '"+ ((s.sk) || i) + "', \
                                '"+ ((t.a)) + "', \
                                null, \
                                '"+ (((t.q) || 0)) + "', \
                                '"+ (parseFloat((t.up ? t.up : t.s ? t.s : 0) || 0).toFixed(4)) + "', \
                                '"+ ((t.b) || 0) + "', \
                                '"+ (parseFloat((t.nc) || 0).toFixed(4)) + "', \
                                '"+ ((t.plk)) + "', \
                                '"+ ((t.ln)) + "' \
                                ) ";
                }

                executeQuery(query, async (rs) => {
                    console.log(rs)
                    if (rs == undefined) {
                        const order = new Parse.Query('Orders')
                        await order.get(res[i].id).then((resorder) => {
                            resorder.set({ 's': 2 })
                            resorder.save()
                            orderSaved++
                            if (orderSaved == Object.keys(res).length) {
                                savedData.orders = true
                            }
                        })
                    } else {
                        if (rs.number == 2627) {
                            const order = new Parse.Query('Orders')
                            await order.get(res[i].id).then((resorder) => {
                                resorder.set({ 's': 2 })
                                resorder.save()
                                orderSaved++
                                if (orderSaved == Object.keys(res).length) {
                                    savedData.orders = true
                                }
                            })
                        }
                        else {
                            orderSaved++
                            if (orderSaved == Object.keys(res).length) {
                                savedData.orders = true
                            }
                        }
                    }
                }, pool);
            }
        }

        const queryCollections = new Parse.Query('Collections')
        queryCollections.equalTo('envId', config.token)
            .equalTo('st', 0)
        const rescoll = await queryCollections.find()
        collSaved = 0
        if (Object.keys(rescoll).length < 1) {
            savedData.collSaved = true
        }
        if (rescoll) {

            for (var i in rescoll) {
                const coll = rescoll[i].attributes

                var query = ""
                query += " INSERT INTO [DATASENDER].dbo.HH_COBRANZA (ID_COBRANZA,CL_CODIGO,FEC_INI_COBRANZA,HORA_INI_COBRANZA,FEC_FIN_COBRANZA,HORA_FIN_COBRANZA,MONTOTOTAL,NRO_RECIBO,VEN_CODIGO,ESTADO) values \
                ( '"+ coll.fk + "','" + coll.c.k + "','" + moment((+coll.t)).format("YYYY-MM-DD 00:00:00") + "','" + moment((+coll.t)).format("HH:mm:ss") + "','" + moment((+coll.t)).format("YYYY-MM-DD 00:00:00") + "','" + moment((+coll.t)).format("HH:mm:ss") + "','" + coll.ta + "','" + (coll.r ? coll.r.substr(0,8) : '') + "','" + coll.s + "',0 ) "
                var counter = 1

                for (var j in coll.impdoc) {
                    const docimp = coll.impdoc[j]
                    for (var k in docimp.q) {
                        const qdoc = docimp.q[k]

                        query += " INSERT INTO [DATASENDER].dbo.HH_DOCIMPUTADOS (ID_COBRANZA,CO_CODIGO,CO_SUCURS,CO_NUMERO,CO_TIPO,MONTO,CUOTA,HH_LINEA) values \
                       ('"+ coll.fk + "','" + docimp.dc + "','" + docimp.do + "' ,'" + docimp.dn + "','" + docimp.dt + "','" + (qdoc.py ? qdoc.py : 0) + "','" + (qdoc.qn ? qdoc.qn : 1) + "','" + counter + "' ) "
                        counter++
                    }

                }
                var counter2 = 0

                for (var j in coll.py) {
                    const py = coll.py[j]
                    const n = (py.m == 1) ? py.cn : '0'
                    const pm = ((py.m == 0) ? '$' : (py.m == 1) ? 'CT' : '')
                    const b = ((py.m == 1) ? py.b.k : ' ')
                    const fec1 = ((py.m == 1) ? moment(+py.pd).format("YYYY-MM-DD 00:00:00") : null)
                    const fec2 = ((py.m == 1) ? moment(+py.di).format("YYYY-MM-DD 00:00:00") : null)
                    query += " INSERT INTO [DATASENDER2].dbo.HH_DETALLEPAGOS (ID_COBRANZA,NUMERO,ID_MEDIOPAGO,CODIGOBANCO,PLAZA,FECHA,FECVEN,COTIZACION,MONTO,HH_LINEA) values \
                        ('"+ coll.fk + "','" + (n) + "','" + pm + " ','" + b + "',' '," + (fec1?("'"+fec1+"'"):null) + "," + (fec1?("'"+fec1+"'"):null) + ",1.00,'" + (+py.a) + "'," + (+counter2) + ") "
                        counter2++
                }
                console.log(query)
                executeQuery(query, async (rs, a) => {

                    if (rs == undefined) {

                        const order = new Parse.Query('Collections')
                        await order.get(rescoll[i].id).then((resorder) => {

                            resorder.set({ 'st': 2 })
                            resorder.save()

                            collSaved++
                            if (collSaved == Object.keys(rescoll).length) {
                                savedData.collections = true
                            }
                        })
                    } else {
                        if (rs.number == 2627) {

                            const order = new Parse.Query('Collections')
                            await order.get(rescoll[i].id).then((resorder) => {
                                resorder.set({ 'st': 2 })
                                resorder.save()
                                collSaved++
                                if (collSaved == Object.keys(rescoll).length) {
                                    savedData.collections = true
                                }
                            })
                        }
                        else {
                            collSaved++
                            if (collSaved == Object.keys(rescoll).length) {
                                savedData.collections = true
                            }
                        }
                    }
                }, pool);

            }

        }

        var intervalo = setInterval(function () {
            var encontro = false
            for (var i in savedData) {

                if (!savedData[i]) {
                    console.log(i)
                    encontro = true
                    break

                }

            }
            if (!encontro) {
                clearInterval(intervalo)
                working = false
            }
        }, 3000);
    } catch (err) {
        log2.info('err', err)

        console.log(err);
    }
})();});

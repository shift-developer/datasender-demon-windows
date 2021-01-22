const config = {
    token: 'token'
};

const Parse = require('parse/node');
Parse.initialize("ejemploID");
const _http = require('follow-redirects').http;
const fs = require('fs');
let dbfb2 = firebaseapp.database();
dbfb2.ref('entorno-produccion').once('value', (snapshot) => {
    const ip = snapshot.val();
    Parse.serverURL = ip.ip;

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
const data = {
    clients: {},
    companies: {},
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
    packs: {},
    vats: {},
    brands: {},
};

const savedData = {
    clients: false,
    companies: false,
    articles: false,    
    groupers: false,
    apiSalesArticles: false,
    priceList: false,
    sellers: false,
    routes: false,
    deposits: false,
    banks: false,
    reasonsNotBuy: false,
    salesCondition: false,
    stock: false,
    apiSalesClients: false,
    currentAccount: false,
    packs: false,
    vats: false,
    brands: false,
    orders: false,
    collections: false,
    notBuy:false
}

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
    console.error(error);
  });
});
  var postData = JSON.stringify({[_action]:_datos});
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
    fs.readFile('./files/' + x, function (err, res) {

        if (err) {

            sendData(x, d, 'set', function () {
                saveFile(x, JSON.parse(ds));
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
            if (!y || Object.keys(y).length < 1 || Object.keys(y)[0] == "undefined") {

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

    fs.writeFile('./files/' + x, JSON.stringify(y), function (err) {
        savedData[x] = true
        if (err) {
            return console.log(err);
        }
    });
}

const executeQuery = async (x, y, z) => {
    try {
        var p = z;
        await p.query(x).then(r => {
            y(r)
        });
    } catch (e) {
        y(e)
    }
}
const executeQuery2 = async (x, y, z) => {
    try {
        var p = z;
        await p.execute(x).then(r => {
            y(r)
        });
    } catch (e) {
        y(e)
    }
}

var rs = {

}
var tablas = [
    'ART_PRE', 'ARTICULOS', 'BANCOS','STOCK', 'CLIENTES', 'CLIENTES_TELEFONO',
     'COMPROBANTES','COMP_CUO', 'COND_VENTA', 'CONDICIONIVA', 'DEPOSITO', 'EMPRESA', 
    'HOJA_RUTA_CLIENTE', 'HOJA_RUTA',
     'LISTA_PRECIO', 'MARCA', 'MOTIVO_NO_COMPRA',  'VENDEDOR'
]


async function readJson(x, y) {

    let promise = new Promise((res2, rej) => {
        fs.readFile('../ds/data/' + x + '.JSON', function (err, res) {

            if (err) {
                return true
            }
            try{
                if(res.length>5){
                    rs[x] = JSON.parse(res)

                }
                else{
                    rs[x] =[]
                }
                res2("Now it's done!")
            }
            catch(e){
                rs[x] =[]
                console.log(x,e)
            }
            
        })
    });
    var aAs = await promise

}
async function readTransactions(x, y) {

    let promise = new Promise((res2, rej) => {
        fs.readFile('../ds/data/files/' + x + '.JSON', function (err, res) {

            if (err) {
                return true
            }
            rs[x] = JSON.parse(res)
            res2("Now it's done!")
        })
    });
    var aAs = await promise
}
const saveTransaction = (x, y, z) => {

    savedData[z]=true
    fs.writeFile('../ds/' + z + '/' + x + ".JSON", JSON.stringify(y), function (err) {
        if (err) {
            return console.log(err);
        }
    });
}

const v = (x, y) => {
    const p = y.split('.');
    var val = x;
    for (var i in p) {
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
        for (var i in tablas) {
            console.log(i)
            var alguito = await readJson(tablas[i])

        }
        readTransactions('orders')
        readTransactions('collection')
        readTransactions('notBuy')


        for (var i in rs.EMPRESA) {
            var a = rs.EMPRESA[i];



            (data && data.companies) &&
                (data.companies[a.EM_CODIGO] = {
                    k: a.EM_CODIGO,
                    n: a.EM_DESCRI
                })
        }
        changesControl('companies', data.companies);
        for (var i in rs.CLIENTES) {
            var a = rs.CLIENTES[i];
            var sll={}
        for(var i in a.VEN_CODIGO){
            sll[a.VEN_CODIGO[i]]=true
        }
            (data && data.clients) &&
                (data.clients[(a.CL_CODIGO)] = {
                    k: a.CL_CODIGO + "",
                    n: (a.CL_NOMBRE ? a.CL_NOMBRE : '').split(' ').filter((b) => { if (b != '') { return true } else { false } }).join(' '),
                    it: a.CL_TIPDOC,
                    i: a.CL_DOCUME,
                    bn: (a.CL_RAZSOC ? a.CL_RAZSOC : '').split(' ').filter((b) => { if (b != '') { return true } else { false } }).join(' '),
                    pl: a.LP_CODIGO + "",
                    sc: a.CVE_CODIGO + "",
                    s: sll?sll:null,
                    f: {
                        c: a.CL_CUIT,
                        v: a.CL_IVA,
                    },
                    a: {
                        "a1": {
                            a: a.CL_DIRECC,
                            n: a.CL_BARRIO,
                            s: a.CL_LOCALI,
                            z: a.CL_CODPOS
                        }
                    },
                    e: {
                        "e": {
                            e: a.CL_EMAIL
                        }
                    },
                   
                    p: {}
                });
        };
        var phones = rs.CLIENTES_TELEFONO;
        for (var i in phones) {
            var d = rs[i];
            for (var i in d) {
                var dd = d[i];
                (data && data.clients && data.clients[dd.CL_CODIGO.replace(/\./g, '')] && data.clients[dd.CL_CODIGO.replace(/\./g, '')].p) &&
                    (data.clients[dd.CL_CODIGO.replace(/\./g, '')].p[('a' + dd.CLT_ID.replace(/\./g, ''))] = {
                        k: ('a' + dd.CLT_ID.replace(/\./g, '')),
                        d: dd.TT_DESCRI,
                        p: dd.CLT_NUMERO
                    });
            }

        };
        changesControl('clients', data.clients);
        for (var i in rs) {
            var c = rs[i];
            (data && data.groupers && (!data.groupers[validateArray(c.FA_CODIGO)])) &&
                (data.groupers[validateArray(c.FA_CODIGO)] = {
                    k: validateArray(c.FA_CODIGO),
                    n: c.FA_NOMBRE,
                    a: {}
                });
            (data && data.apiSalesArticles) &&
                (data.apiSalesArticles[c.AR_CODIGO] = {
                    k: c.AR_CODIGO,
                    n: c.AR_DESCRI,
                    v: {
                        a: c.ALI_PORCEN ? c.ALI_PORCEN : 21,
                        f: false
                    },
                    dum: c.AR_UMEXACTA == 'S' ? validateArray(c.UM_CODIGO, 0) : c.UM_CODCOM,
                    w: c.AR_PESO,
                    nc: c.AR_COSNET,
                    b: c.MA_CODIGO,
                    um: c.AR_UMEXACTA == 'S' ? {
                        [validateArray(c.UM_CODIGO)]: {
                            k: validateArray(c.UM_CODIGO, 0),
                            e: 1,
                            n: validateArray(c.UM_DESCRI, 0)
                        }
                    } : {
                            [c.UM_CODCOM]: {
                                k: c.UM_CODCOM,
                                e: c.AR_EQUUM,
                                n: validateArray(c.UM_DESCRI, 1)
                            },
                        }
                }) &&
                (data.groupers[validateArray(c.FA_CODIGO)].a &&
                    (data.groupers[validateArray(c.FA_CODIGO)].a[validateArray(c.AR_CODIGO)] = {
                        k: validateArray(c.AR_CODIGO)
                    }));
        };
        changesControl('apiSalesArticles', data.apiSalesArticles);
        changesControl('groupers', data.groupers);
        for (var i in rs.ARTICULOS) {
            var c = rs.ARTICULOS[i];
            (data && data.articles) &&
                (data.articles[c.AR_CODIGO] = {
                    k: c.AR_CODIGO + "",
                    n: c.AR_DESCRI,
                    v: {
                        a: c.ALI_PORCEN ? c.ALI_PORCEN : 21,
                        f: false
                    },
                    dum: validateArray(c.UM_CODIGO, 0) + "",
                    w: 0 + "",
                    nc: c.AR_COSNET + "",
                    br: c.MA_CODIGO + "",
                    um: {
                        [validateArray(c.UM_CODIGO)]: {
                            k: validateArray(c.UM_CODIGO, 0),
                            e: 1,
                            n: validateArray(c.UM_DESCRI, 0)
                        },
                        [c.UM_CODCOM]: {
                            k: c.UM_CODCOM,
                            e: c.AR_EQUUM,
                            n: validateArray(c.UM_DESCRI, 0)
                        },
                    }
                });

        };
        changesControl('articles', data.articles);
        for (var i in rs.LISTA_PRECIO) {
            var d = rs.LISTA_PRECIO[i];
            (data && data.priceList) &&
                (data.priceList[d.LP_CODIGO] = {
                    k: d.LP_CODIGO + "",
                    n: d.LP_DESCRI,
                    a: {}
                });
        };
       

        for (var i in rs.ART_PRE) {
            var d = rs.ART_PRE[i];
            if (!data.priceList) {
                data.priceList = {}
            }
            for (var j in d) {
                var dd = d[j]
                if (!data.priceList[dd.NLP_CODIGO]) {
                    data.priceList[dd.NLP_CODIGO] = {}
                }

                if (!data.priceList[dd.NLP_CODIGO].a) {
                    data.priceList[dd.NLP_CODIGO].a = {}
                }
               
                data.priceList[dd.NLP_CODIGO].a[dd.AR_CODIGO] = {
                    k: dd.AR_CODIGO + "",
                    u: {
                        a: parseFloat(dd.AP_UTILID.replace(',','.')),
                        f: true
                    }
                }

            }


        };
        changesControl('priceList', data.priceList);
        for (var i in rs.VENDEDOR) {
            var d = rs.VENDEDOR[i];
            (data && data.sellers) &&
                (data.sellers[d.VEN_CODIGO] = {
                    k: d.VEN_CODIGO + "",
                    n: d.PER_NOMBRE,
                    p: d.VEN_PASSWORD + "",
                    i: d.PER_DOCUME,
                    it: d.PER_TIPDOC + ""
                });
        };
        changesControl('sellers', data.sellers);
        for (var i in rs.HOJA_RUTA) {
            var d = rs.HOJA_RUTA[i];
            //console.log(d);
            (data && data.routes) &&
                (data.routes[d.HR_CODIGO] = {
                    k: d.HR_CODIGO,
                    n: d.HR_DESCRI,
                    cl: {},
                    d: +d.DIA_CODIGO,
                    s: {
                        [d.VEN_CODIGO]: true
                    }
                });
        };
        for (var i in rs.HOJA_RUTA_CLIENTE) {
            var dd = rs.HOJA_RUTA_CLIENTE[i];
            for(var j in dd){
                var d =dd[j]
                if(data && data.routes && !data.routes[d.HR_CODIGO]){
                    data.routes[d.HR_CODIGO]={
                        k: d.HR_CODIGO,
                        n: d.HR_DESCRI,
                        cl: {},
                        d: 0,
                        s: {
                            [d.VEN_CODIGO]: true
                        }
                    }
                }
                (data && data.routes && data.routes[d.HR_CODIGO] && data.routes[d.HR_CODIGO].cl) &&
                    (data.routes[d.HR_CODIGO].cl[d.CL_CODIGO] = {
                        k: d.CL_CODIGO,
                        o: d.HRD_ORDEN
                    });  
            }
           
        }
        changesControl('routes', data.routes);
        for (var i in rs.DEPOSITO) {
            var d = rs.DEPOSITO[i];
            (data && data.deposits) &&
                (data.deposits[d.DE_CODIGO] = {
                    k: d.DE_CODIGO + "",
                    n: d.DE_DESCRI
                });
        };
        changesControl('deposits', data.deposits);

        for (var i in rs.COND_VENTA) {
            var d = rs.COND_VENTA[i];
            (data && data.salesCondition) &&
                (data.salesCondition[d.CVE_CODIGO] = {
                    k: d.CVE_CODIGO + "",
                    n: d.CVE_DESCRI
                });
        };
        changesControl('salesCondition', data.salesCondition);
        for (var i in rs.STOCK) {
            var d = rs.STOCK[i];
            for(var j in d){
                var dd=d[j];
                (data && data.stock) &&
                (data.stock[dd.DE_CODIGO + "" + dd.AR_CODIGO] = {
                    k: dd.DE_CODIGO + "" + dd.AR_CODIGO + "",
                    a: dd.AR_CODIGO + "",
                    d: dd.DE_CODIGO + "",
                    q: dd.ST_STOCK
                });
            }
           
        };
        changesControl('stock', data.stock);
        for (var i in rs.CLIENTES) {
            var d = rs.CLIENTES[i];
            (data && data.apiSalesClients) &&
                (data.apiSalesClients[d.CL_CODIGO] = {
                    k: d.CL_CODIGO + "",
                    e: d.CL_EMAIL,
                    p: d.CL_CODIGO + "",
                    sp: 1 + ""
                });
        };
        changesControl('apiSalesClients', data.apiSalesClients);
       
        for (var i in rs.CONDICIONIVA) {
            var d = rs.CONDICIONIVA[i];
            (data && data.vats) &&
                (data.vats[d.COI_CODIGO] = {
                    k: d.COI_CODIGO + "",
                    n: d.COI_DESCRI
                });
        };
        changesControl('vats', data.vats);
       
        for (var i in rs.MARCA) {
            var d = rs.MARCA[i];
            (data && data.brands) &&
                (data.brands[d.MA_CODIGO] = {
                    k: d.MA_CODIGO + "",
                    n: d.MA_DESCRI
                });
        };
        changesControl('brands', data.brands);
       
        for (var i in rs.COMPROBANTES) {
            var d = rs.COMPROBANTES[i];
            (data && data.currentAccount) &&
                (data.currentAccount[i] = {
                    k: i,
                    k2: d.CO_CODIGO + d.CO_SUCURS + d.CO_NUMERO + d.CO_TIPO,
                    c: d.CL_CODIGO + "",
                    cn: d.CL_NOMBRE,
                    bn: d.CL_RAZSOC,
                    co: d.EM_CODIGO + "",
                    s: d.VEN_CODIGO + "",
                    dc: d.CO_CODIGO,
                    do: d.CO_SUCURS + "",
                    dn: d.CO_NUMERO + "",
                    dt: d.CO_TIPO,
                    dd: d.CO_DESCRI,
                    da: d.CO_TOTCOM,
                    tc: (new Date((d.CO_FECHA ? d.CO_FECHA : '2020-01-03') + " 00:00:00")).getTime(),
                    te: (new Date((d.CO_FECVEN ? d.CO_FECVEN : '2020-01-03') + " 00:00:00")).getTime(),
                    q: {
                      
                    }
                });
        }
       
        for (var i in rs.COMP_CUO) {
            var d = rs.COMP_CUO[i];
            (data && data.currentAccount && data.currentAccount[i] && data.currentAccount[i].q) &&
                (data.currentAccount[i].q[d.COC_NUMERO] = {
                    k: d.COC_NUMERO,
                    k2:d.CO_CODIGO + d.CO_SUCURS + d.CO_NUMERO + d.CO_TIPO,
                    ia: data.currentAccount[i].da,
                    pa: d.COC_IMPENT,
                    qn: d.COC_NUMERO,

                     te: (new Date(d.COC_FECVEN)).getTime()
                 });
        };
        changesControl('currentAccount', data.currentAccount);
        

        const queryOrders = new Parse.Query('Orders')
        queryOrders.equalTo('envId', config.token)
            .equalTo('s', 1)
        const res = await queryOrders.find()
        orderSaved = 0
        if (Object.keys(res).length < 1) {
            savedData.orders = true
        }
        function saveOrder(x,param,value,type,folder){
            x.set({ [param]: value })
            x.save().then((x1)=>{
                var order = x.attributes
                rs.orders[order.sk] = order
                 orderSaved++
                 saveTransaction(type+"" + Date.now()+"_"+(Math.random()*1000),{[order.sk] : order}, folder)
            },(err)=>{
                console.log('err pedidos save', err)
                orderSaved++  
            })
        }
        for (var i in res) {
            if (!rs.orders) {
                rs.orders = {}
            }
            
            saveOrder(res[i],'s',2,'order_','orders')
        }
        if (Object.keys(res).length > 0) {
            
            
        }
       

        const queryCollections = new Parse.Query('Collections')
        queryCollections.equalTo('envId', config.token)
        
            .equalTo('st', 0)
            
        const resC = await queryCollections.find()
        collSaved = 0
            if (Object.keys(resC).length < 1) {
                savedData.collections = true
            }
        for (var i in resC) {
            if (!rs.collections) {
                rs.collections = {}
            }
            saveOrder(resC[i],'st',1,"collection_","collections")
           

        }
        if (Object.keys(resC).length > 0) {
           
        }

        const queryNotBuy = new Parse.Query('NotBuy')
        queryNotBuy.equalTo('envId', config.token)
            .equalTo('st', 0)

        const resN = await queryNotBuy.find()
        nbsaved = 0
        if (Object.keys(resN).length < 1) {
            savedData.notBuy = true

        }
            
        for (var i in resN) {
            if (!rs.notBuy) {
                rs.notBuy = {}
            }
            
            saveOrder(resN[i],'st',2,"notBuy_","notBuy")
          
           

        }
        if (Object.keys(resN).length > 0) {
          
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
        console.log(err);
    }
})();})

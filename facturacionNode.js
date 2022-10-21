const Service = require('node-windows').Service

const svc = new Service({
    name: "facturacionRestaurante",
    description: "This is software facturación electrónica",
    script: "F:\\Youtube\\index.js"
})

svc.on('install', function(){
    svc.start()
})

svc.install()
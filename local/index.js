const express = require('express');
const bodyParser = require('body-parser');

//crear la aplicacion express
const app = express();

//middleware para los cuerpos JSON
app.use(bodyParser.json());

//Definicion del endpoint para recibir el webhook y exportarlo como la funcion
exports.webhookReceiver = async(req, res)=>{
    try{
        const data = req.body; //obtencion de los datos del webhook
        console.log('webhook recibido', JSON.stringify(data, null, 2)); //mostrar el JSON formateado

        //respuesta con los datos recibidos
        res.status(200).json({
            message: 'Webhook recibido correctamente',
            receivedData: data,
        });
    }catch(error){
        console.error('Error al procesar el webhook', error);
        res.status(500).json({
            message: 'Hubo un error procesando el webhook',
            error: error.message,
        });
    }
};

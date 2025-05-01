#!/usr/bin/env node
const JSONStream = require('JSONStream');
const es = require('event-stream');
const commandLineArgs = require('command-line-args');
const fs = require('fs');
const csvParse = require('csv-parse/sync');
const csvStringify = require('csv-stringify/sync');

const optionDefinitions = [
    { name: 'transform', alias: 't', type: String }
];
const args = commandLineArgs(optionDefinitions);

let cachePath = './cache.csv';
let nameCache = loadCache();
let unifyingListPath = './sujetosobligados.csv';
let sujetosObligados = loadUnifyingList();

process.stdin.setEncoding('utf8');

process.stdin
.pipe(JSONStream.parse())
.pipe(es.mapSync(function (obj) {
    return transformObject(obj);
}))
.pipe(JSONStream.stringify(false))
.pipe(process.stdout);

process.stdin.on('end', () => {
    process.stdout.write('\n');
});

const normalizeName = str =>
    str.normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/\s{2,}/g, ' ')
        .toUpperCase();

function transformObject(obj) {
    if(obj.hasOwnProperty('complementoPrincipal') && obj.complementoPrincipal.hasOwnProperty('nombre')) {
        let nombre = normalizeName(obj.complementoPrincipal.nombre);
        if(!nameCache.hasOwnProperty(nombre)) {
            // Ir a buscar el nombre
            // let gender = genders[Math.floor(Math.random() * 100) % 2];
            // writeToCache([nombre, gender]);
            // nameCache[nombre] = gender;
        }
        else {
            Object.assign(obj, nameCache[nombre]);
        }

        obj.sujetoobligado = fixSujeto(obj.sujetoobligado);

        // TODO: quitar objetos informacionPrincipal y complementoPrincipal para reducir la data generada
        
        if(obj.complementoPrincipal.hasOwnProperty('fechaInicioPeriodo')) {
            let fecha = parseDate(obj.complementoPrincipal.fechaInicioPeriodo);
            Object.assign(obj, { fecha: fecha });
        }

        if(obj.complementoPrincipal.hasOwnProperty('montoNeto')) {
            Object.assign(obj, {
                sueldo_neto: obj.complementoPrincipal.montoNeto,
                sueldo_bruto: obj.complementoPrincipal.montoBruto
            })
        }
    }
    return obj;
}

function parseDate(str) {
    let parts = str.split('/');
    return parts[2] + '-' + parts[1] + '-' + parts[0] + 'T00:00:00.000-06:00';
}

function fixSujeto(str) {
    if(sujetosObligados.hasOwnProperty(str)) {
        if(sujetosObligados[str].corregido != '') return sujetosObligados[str].corregido;
        else return str;
    }
    else {
        console.log('ERROR: no matching fix found for sujeto', str);
        process.exit(1);
    }
}

function loadCache() {
    let cache = {};
    if( fs.existsSync(cachePath) ) {
        const names_file = fs.readFileSync(cachePath, 'utf8');
        const names = csvParse.parse(names_file);
        names.map( n => {
            cache[n[0]] = { genero: n[1], probabilidad: n[2] };
        } )
    }
    return cache;
}

function loadUnifyingList() {
    let list = {};
    if( fs.existsSync(unifyingListPath) ) {
        const sujetos_file = fs.readFileSync(unifyingListPath, 'utf8');
        const sujetos = csvParse.parse(sujetos_file);
        sujetos.map( s => {
            list[s[0]] = { corregido: s[1] };
        } )
    }
    return list;
}

function writeToCache(arr) {
    let data = csvStringify.stringify([arr]);
    fs.appendFileSync(cachePath, data);
}

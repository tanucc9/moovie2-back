const {MongoClient} = require('mongodb');
const csv = require('csv-parser');
const fs = require('fs');

async function main(){

    /**
     * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
     * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
     */
    const uri = "mongodb+srv://root:loriamauro@cluster0.yaaq6.mongodb.net/test?retryWrites=true&w=majority"; 

    const client = new MongoClient(uri, { useUnifiedTopology: true });
 
    try {
        // Connect to the MongoDB cluster
        await client.connect();
        const db = client.db("moovie2");
        insertDocuments(db);

    } catch (e) {
        console.error(e);
    } finally {
       // await client.close();
    }
}

main();

function insertDocuments(db) {

    mapArrayFields().then(function(datas){
        console.log("Inserimento film in corso...");
        db.collection('film').insertMany(datas)
        .then(() => {
          console.log("Inserimento film completato.");

          //Indici per i film
          db.collection('film').createIndex({ anno: 1 });
          db.collection('film').createIndex({ voti: -1 });

          //Creazione collezione attori
          createAttoriCollection(db);
        })
        .catch(function(v) {
          console.log(v);
        });     

    });  
}

function createAttoriCollection(db) {
  console.log("Avvio inserimento attori...");
  
  //Creazione collezione Attori
  db.createCollection("attori");

  //Query per il prelievo degli attori dalla collezione film
  db.collection('film', function(err, collection) {

    const unwindAttori = { $unwind: "$attori" };
    const group = {
      $group: {
        _id : "$attori",
        films : {
            $push: {
                    idDocumentFilm: "$_id",
                    titolo_originale: "$titolo_originale",
                    durata: "$durata",
                    voto_medio : "$voto_medio",
                    anno: "$anno",
                    genere: "$genere"
            }
        }
      }
    };
    const project = {
      $project : {
          nome : "$_id",
          films: "$films",
          _id: 0
      }
    }

    collection.aggregate(
        [
          unwindAttori,
          group,
          project
        ],
    ).toArray(function(err, results) {
        console.log(err);

        console.log("Inserimento attori in corso...");
        db.collection('attori', function(err, attori) {
          attori.insertMany(
            results
          ).then( () => {
            
            //Creazione indice sul campo nome
            attori.createIndex( {"nome" : 1} );
            
            console.log("Inserimento attori completato.");
            console.log("Finished.");
          });
        });
        
    });

  });

}

/**
 * Campi da gestire:
 * - paese
 * - registi
 * - attori
 */
function mapArrayFields() {

    return new Promise(function(resolve, reject) {
        let datas = [];

        fs.createReadStream('filmtv_movies.csv').pipe(csv())
        .on('data', (row) => {
            const paeseList = row.paese.trim() !== '' ? row.paese.split(',').map(paese => paese.trim()) : [];
            const registiList = row.registi.trim() !== '' ? row.registi.split(',').map(regista => regista.trim()) : [];
            const attoriList = row.attori.trim() !== '' ? row.attori.split(',').map(attore => attore.trim()) : [];

            const film = {
                titolo_originale : row.titolo_originale,
                titolo_italiano : row.titolo_italiano,
                anno : parseInt(row.anno), //int
                genere : row.genere,
                durata : parseInt(row.durata), //int
                paese : paeseList,
                registi : registiList,
                attori : attoriList,
                voto_medio : parseFloat(row.voto_medio), //float
                voti : parseInt(row.voti), //int
                descrizione : row.descrizione,
                note : row.note
            }
    
            datas.push(film);
        })
      .on('end', () => {
        console.log('CSV file successfully processed');
        resolve(datas.slice(0, 30000));
      });
    });

}
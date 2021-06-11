const MongoClient = require('mongodb').MongoClient;
const uri = "mongodb+srv://root:loriamauro@cluster0.yaaq6.mongodb.net/moovie?retryWrites=true&w=majority";
const client = new MongoClient(uri, {useNewUrlParser: true, useUnifiedTopology: true});

module.exports = () => client.connect().then(() => {
	const db = client.db('moovie2');
	console.log('MongoDB connection established');
	return {
		films: db.collection('film'),
	};
});

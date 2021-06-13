const mongo = require('./dbconn');
const ObjectId = require('mongodb').ObjectID;

const express = require('express');
const {query, param, validationResult} = require('express-validator');
const cors = require('cors');

const app = express();
app.use(cors())

mongo().then(({films}) => {

	// Tutti i film paginati con filtri titolo-anno-genere, ordinati per titolo-anno-genere-durata (de/)crescenti
	app.route('/film').get(
		query('titolo').isString().optional(),
		query('anno').isInt().toInt().optional(),
		query('genere').isString().optional(),
		query('sort').matches(/(titolo_italiano|anno|genere|durata|voto_medio) (asc|desc)/i).toLowerCase().optional(),
		query('pageNum').isInt({min: 1}).toInt().optional(),
		query('pageSize').isInt({min: 1}).toInt().optional(),
		async (req, res) => {
			const errors = validationResult(req);
			if (!errors.isEmpty()) {
				return res.status(400).json({errors: errors.array()});
			}

			// Filters
			let criteria = {
				$and: [],
			};
			let {titolo, anno, genere} = req.query;
			if (titolo) {
				const reg = new RegExp('^.*' + titolo + '.*$', 'i');
				// criteria.$and.push({titolo_italiano: {$regex: reg}});
				criteria.$and.push({
					$or: [
						{titolo_originale: {$regex: reg}},
						{titolo_italiano: {$regex: reg}},
					],
				});
			}
			if (anno) {
				criteria.$and.push({
					anno: {$eq: anno},
				});
			}
			if (genere) {
				criteria.$and.push({
					genere: {$eq: genere},
				});
			}
			if (criteria.$and.length === 0) {
				criteria = {};
			}

			// Sorting
			let sort;
			if (!req.query.sort) {
				req.query.sort = 'titolo_italiano asc';
			}
			const [field, orderStr] = req.query.sort.split(' ');
			const orderInt = orderStr === 'asc' ? 1 : -1;
			const sortingObjects = {
				'titolo_italiano': {titolo_italiano: orderInt, _id: 1},
				'anno': {anno: orderInt, _id: 1},
				'genere': {genere: orderInt, _id: 1},
				'durata': {durata: orderInt, _id: 1},
				'voto_medio': {voto_medio: orderInt, _id: 1},
			};
			sort = sortingObjects[field]

			// Pagination
			const pagination = {
				pageNum: req.query.pageNum || 1,
				pageSize: req.query.pageSize || 10,
			};

			const numSkip = (pagination.pageNum - 1) * pagination.pageSize;
			const paginatedFilms = await films
				.find(criteria)
				.sort(sort)
				.limit(pagination.pageSize)
				.skip(numSkip)
				.toArray()
				.catch(e => {
					console.error(e);
				});
			const totalFilms = await films.countDocuments(criteria);
			console.log('Paginated ' + paginatedFilms.length + ' films over ' + totalFilms + ' skipping ' + numSkip);
			const result = {
				paginatedFilms,
				totalFilms,
				pageNum: pagination.pageNum,
				pageSize: pagination.pageSize,
				sortBy: field,
				descending: orderInt < 0,
			};
			return res.json(result);
		});

	// Il film corrispondente a un dato ID
	app.route('/film/:id').get(
		param('id').isString(),
		async (req, res) => {
			const errors = validationResult(req);
			if (!errors.isEmpty()) {
				return res.status(400).json({errors: errors.array()});
			}
			const id = req.params.id;
			console.log('Looking for film ' + id);
			try {
				const retrievedFilm = await films.findOne({_id: {$eq: new ObjectId(id)}});
				if (retrievedFilm) {
					return res.json(retrievedFilm);
				}
				return res.sendStatus(404);
			} catch (err) {
				console.error(err);
				return res.sendStatus(400);
			}
		});

	// Tutti i film usciti in un dato anno con un dato attore
	app.route('/film/:year([0-9]+)/:actor').get(
		async (req, res) => {
			const {year, actor} = req.params;
			const matchingFilms = await films
				.find({
					anno: {$eq: parseInt(year)},
					attori: {
						$in: [actor],
					},
				})
				.toArray()
				.catch(e => {
					console.error(e);
				})
			return res.json(matchingFilms);
		});

	// Tutti i film girati da un dato regista, ordinati per anno di uscita crescente
	app.route('/film/regista/:director').get(
		param('director').isString().isLength({min: 1}),
		async (req, res) => {
			const errors = validationResult(req);
			if (!errors.isEmpty()) {
				return res.status(400).json({errors: errors.array()});
			}
			const {director} = req.params;
			const matchingFilms = await films
				.find({
					registi: {
						$in: [director],
					},
				})
				.sort({anno: 1})
				.toArray()
				.catch(e => {
					console.error(e);
				})
			return res.json(matchingFilms);
		});

	// Tutti i film di un dato genere votati da un dato numero minimo, ordinati per numero di voti decrescente
	app.route('/film/genere/:genere/voti-minimi/:minimo([0-9]+)').get(
		param('genere').isString().isLength({min: 1}),
		param('minimo').isInt({min: 0, max: 10}),
		async (req, res) => {
			const {genere, minimo} = req.params;
			const matchingFilms = await films
				.find({
					genere: {$eq: genere},
					voti: {$gte: parseInt(minimo)},
				})
				.sort({voti: -1})
				.toArray()
				.catch(e => {
					console.error(e);
				})
			return res.json(matchingFilms);
		});

})

const port = process.env.PORT || 8080;
app.listen(port);

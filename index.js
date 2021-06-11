const mongo = require('./dbconn');

const express = require('express');
const {query, validationResult} = require('express-validator');
const app = express();
const bodyParser = require('body-parser');
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}));

mongo().then(({films}) => {

	// Tutti i film paginati con filtri titolo-anno-genere, ordinati per titolo-anno-genere-durata (de/)crescenti
	app.route('/film').get(
		query('titleQuery').isString().optional(),
		query('year').isInt().toInt().optional(),
		query('genre').isString().optional(),
		query('sort').matches(/(title|year|genre|duration) (asc|desc)/i).toLowerCase().optional(),
		query('pageNum').isInt({min: 1}).toInt().optional(),
		query('pageSize').isInt({min: 1}).toInt().optional(),
		async (req, res) => {
			const errors = validationResult(req);
			if (!errors.isEmpty()) {
				return res.status(400).json({errors: errors.array()});
			}

			// Filters
			const criteria = {
				$and: [],
			};
			let {titleQuery, year, genre} = req.query;
			if (titleQuery) {
				const uniqueTitleWords = titleQuery.trim().replace(/ +/g, ' ').split(' ')
					.filter((item, index, arr) => arr.indexOf(item) === index);
				const reg = new RegExp(uniqueTitleWords.join('|'), 'i');
				criteria.$and.push({
					$or: [
						{titolo_originale: {$regex: reg}},
						{titolo_italiano: {$regex: reg}},
					],
				});
			}
			if (year) {
				criteria.$and.push({
					anno: {$eq: year},
				});
			}
			if (genre) {
				criteria.$and.push({
					genere: {$eq: genre},
				});
			}

			// Sorting
			let sort;
			if (req.query.sort) {
				const [field, orderStr] = req.query.sort.split(' ');
				const orderInt = orderStr === 'asc' ? 1 : -1;
				const sortingObjects = {
					'title': {titolo_italiano: orderInt},
					'year': {anno: orderInt},
					'genre': {genere: orderInt},
					'duration': {durata: orderInt},
				};
				sort = sortingObjects[field]
			} else {
				sort = {titolo_italiano: 1};
			}

			// Pagination
			const pagination = {
				pageNum: req.query.pageNum || 1,
				pageSize: req.query.pageSize || 10,
			};

			const paginatedFilms = await films.find(criteria).sort(sort)
				.limit(pagination.pageSize).skip((pagination.pageNum - 1) * pagination.pageSize)
				.toArray().catch(e => {
					console.error(e);
				})
			const totalFilms = await films.countDocuments(criteria);
			console.log('Paginated films ' + paginatedFilms.length + ' over ' + totalFilms);
			const result = {paginatedFilms, totalFilms};
			return res.json(result);
		});

	// Tutti i film usciti in un dato anno con un dato attore
	app.route('/film/:year([0-9]+)/:actor').get(
		async (req, res) => {
			const {year, actor} = req.params;
			const matchingFilms = await films.find({
				anno: {$eq: parseInt(year)},
				attori: {
					$in: [actor],
				},
			}).toArray().catch(e => {
				console.error(e);
			})
			return res.json(matchingFilms);
		});

	// Tutti i film girati da un dato regista, ordinati per anno di uscita crescente
	app.route('/film/:director').get(
		async (req, res) => {
			const {director} = req.params;
			const matchingFilms = await films.find({
				registi: {
					$in: [director],
				},
			}).sort({anno: 1}).toArray().catch(e => {
				console.error(e);
			})
			return res.json(matchingFilms);
		});

	// Tutti i film di un dato genere votati da un dato numero minimo, ordinati per numero di voti decrescente
	app.route('/film/:genre/:minimum([0-9]+)').get(
		async (req, res) => {
			const {genre, minimum} = req.params;
			const matchingFilms = await films.find({
				genere: {$eq: genre},
				voti: {$gte: parseInt(minimum)},
			}).sort({voti: -1}).toArray().catch(e => {
				console.error(e);
			})
			return res.json(matchingFilms);
		});

})

const port = process.env.PORT || 80;
app.listen(port);

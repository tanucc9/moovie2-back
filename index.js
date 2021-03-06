const mongo = require('./dbconn');
const ObjectId = require('mongodb').ObjectID;

const express = require('express');
const {query, param, validationResult} = require('express-validator');
const cors = require('cors');

const app = express();
app.use(cors())

mongo().then(({films, attori}) => {

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
	
	//I tre migliori registi in assoluto (best director)
	app.route('/registi/best/from/:from([0-9]+)/to/:to([0-9]+)').get(
		async (req, res) => {
			let {from, to} = req.params;

			from = parseInt(from);
			to = parseInt(to);

			//le operazione da passare all'aggregate
			const stages = [
				{
					$unwind: "$registi"
				},
				{
					$group: {
						_id: "$registi",
						media_voti: { $avg: "$voto_medio" },
						film_girati: {$sum : 1}
					}
				},
				{
					$project: {
						media_voti : 1,
						film_girati: 1,
						punteggioRankig : 
						{
							$let: {
								vars : {
									bonusPunteggio : {
										$switch: {
											branches: [
												{case: {$gte: ['$film_girati', 40]}, then: 2 },
												{case: {$gte: ['$film_girati', 30]}, then: 1.8 },
												{case: {$gte: ['$film_girati', 20]}, then: 1.6 },
												{case: {$gt: ['$film_girati', 10]}, then: 1.4 },
												{case: {$gte: ['$film_girati', 5]}, then: 1.2 },
											],
											default: 1
										}
									},
								},
								in: {
									$multiply: ['$$bonusPunteggio', '$media_voti']
								}
							}
						}
					}
				},
				{$sort: {punteggioRankig:-1}},
				{$limit: 3}
			];
			
			//Controllo dei parametri opzionali per la query
			if (from !== 0  && to  !== 0) {
				stages.unshift(
					{
						$match: {
							anno :{
								$gte: from,
								$lte: to 
							}
						}
					}
				);
			} else if (from !== 0) {
				stages.unshift(
					{
						$match: {
							anno :{
								$gte: from
							}
						}
					}
				);
			} else if (to !== 0) {
				stages.unshift(
					{
						$match: {
							anno :{
								$lte: to
							}
						}
					}
				);
			}

			const result = await films.aggregate( stages ).toArray().catch(e => {
				console.error(e);
			});

			return res.json(result);
		}
	);

	//I 5 registi con pi?? film girati
	app.route('/registi/most-films').get(
		async (req, res) => {

			const result = await films.aggregate(
				[
					{ $unwind: "$registi" },
					{
					  $group: {
						 _id: "$registi",
						 film_girati: { $sum: 1 }
					  }
					},
					{$sort: {film_girati:-1}},
					{$limit: 5}
				  ],
			).toArray().catch(e => {
				console.error(e);
			})

			return res.json(result);
		}
	);

	//I 5 registi con la media voto migliore
	app.route('/registi/best-avg').get(
		async (req, res) => {

			const result = await films.aggregate(
				[
					{
						$match: {
							voti : { $gte: 2 }
						}
					},
					{ $unwind: "$registi" },
					{
					  $group: {
						 _id: "$registi",
						 media_voti: { $avg: "$voto_medio" },
						 count: {$sum : 1}
					  }
					},
					{
						$match: {
							count : { $gte: 2 }
						}
					},
					{$sort: {media_voti:-1}},
					{$limit: 5}
				],
			).toArray().catch(e => {
				console.error(e);
			})

			return res.json(result);
		}
	);
	

	//Attori filtrati in base all'anno di uscita del film, durata e genere. Gestio con paginazione.
	app.route('/attori/filtered').get(
		query('durata').isInt().toInt().optional(),
		query('anno').isInt().toInt().optional(),
		query('genere').isString().optional(),
		query('nome').isString().optional(),
		query('perpage').isInt({min: 1}).toInt(),
		query('page').isInt({min: 1}).toInt(),
		async (req, res) => {
			
			const errors = validationResult(req);
			if (!errors.isEmpty()) {
				return res.status(400).json({errors: errors.array()});
			}

			//Paginazione
			const numPerPage = req.query['perpage'];
			console.log(numPerPage);
			const numPage = req.query['page'];
			const skipVal = numPage > 0 ? ( (numPage - 1) * numPerPage ) : 0;

			//Parametri
			const {anno, durata, genere, nome } = req.query;

			let match = {$match:{}};
			
			if (nome) {
				const regex = new RegExp('^' + nome, 'i');		
				match.$match['nome'] = {$regex: regex };
			}
	
			if(anno)
				match.$match["films.anno"] = anno;

			if(durata) 
				match.$match["films.durata"] = {$gte: durata};

			if (genere)
				match.$match["films.genere"] = genere;
				console.log(match);
				
			//unwind array films
			const unwind = 	{
				$unwind: "$films"
			};

			//group
			const group = {
				$group: {
					_id: "$nome",
					films: {$push: "$films"}
				}
			};

			//Query attori con film
			const attoriResult = await attori.aggregate(
				[
					unwind,
					match,
					group,
					{
						$sort: {_id:1}
					},
					{
						$skip: skipVal
					},
					{
						$limit : numPerPage
					},	
					{
						$project : {
							nome: "$_id",
							_id: 0,
							films : "$films"
						}
					}	
				],
			).toArray().catch(e => {
				console.error(e);
			});

			//Query per il numero totale di record (utile alla paginazione)
			const numMaxRecordQuery = await attori.aggregate(
				[
					unwind,
					match,
					group,
					{
						$count : "numMaxRecord"
					}
				]
			).toArray().catch(e => {
				console.error(e);
			});

			//Compatibilit?? con il front
			const numMaxRecord = numMaxRecordQuery.length > 0 ? numMaxRecordQuery[0].numMaxRecord : 0;
			
			const result = {
				attoriResult,
				numMaxRecord
			}
			return res.json(result);
		}
	);

	//Attori con paginazione.
	app.route('/attori/paginated/page/:page/perpage/:perpage').get(
		async (req, res) => {
			
			//Paginazione
			const numPerPage = parseInt(req.params['perpage']);
			const numPage = parseInt(req.params['page']);
			const skipVal = numPage > 0 ? ( (numPage - 1) * numPerPage ) : 0;

			//Query attori con film
			const attoriResult = await attori.find()
										.sort({nome : 1})
										.skip(skipVal)
										.limit(numPerPage)
										.toArray().catch(e => {
											console.error(e);
										});

			//Query per il numero totale di record (utile alla paginazione)
			const numMaxRecord = await attori.countDocuments();
			const result = {
				attoriResult,
				numMaxRecord
			}
			return res.json(result);
		}
	);



})

const port = process.env.PORT || 8081;
app.listen(port);

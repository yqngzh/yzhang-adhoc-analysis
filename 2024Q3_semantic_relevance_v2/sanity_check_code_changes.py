class SemrelModelServingV2(tf.keras.models.Model):
    def __init__(
        self,
        nir_wrapper: NirWrapper,
        n_classes,
        hidden_sizes=(256,),
        activation="relu",
        call_fn_use_logits=True,
        nir_emb_key="final",
        nir_emb_proj=None,
        nir_emb_fusion="lth",
    ):
        super().__init__()
        self.nir_wrapper = nir_wrapper

        # Create mlp: hidden layers, and finally logits head
        mlp_layers = []
        for size in hidden_sizes:
            mlp_layers += [
                tf.keras.layers.Dense(size, activation=activation),
                tf.keras.layers.LayerNormalization(),
            ]

        mlp_layers.append(tf.keras.layers.Dense(n_classes))
        self.mlp = tf.keras.Sequential(mlp_layers)

        self.use_nir_emb_proj = nir_emb_proj not in (None, 0)
        if self.use_nir_emb_proj:

            def create_proj_layer():
                return tf.keras.Sequential(
                    [
                        tf.keras.layers.Dense(nir_emb_proj, activation=activation),
                        tf.keras.layers.LayerNormalization(),
                    ]
                )

            self.nir_proj_layers: Dict | None = {
                k: create_proj_layer() for k in ("query", "listing", "title", "shop", "desckw")
            }
        else:
            self.nir_proj_layers = None

        self.call_fn_use_logits = call_fn_use_logits
        self.nir_emb_key = nir_emb_key
        self.nir_emb_fusion = nir_emb_fusion

        # Store these so model can be restored with get_config/from_config
        self.n_classes = n_classes
        self.hidden_sizes = hidden_sizes
        self.activation = activation
        self.nir_emb_proj = nir_emb_proj

    def score_query_listing_qltsd(
        self,
        query_emb: tf.Tensor,
        listing_emb: tf.Tensor,
        title_emb: tf.Tensor,
        shop_emb: tf.Tensor,
        desckw_emb: tf.Tensor,
        output_logits: bool,
    ):
        hadamard = query_emb * listing_emb
        hadamard_title = query_emb * title_emb
        hadamard_shop = query_emb * shop_emb
        hadamard_desckw = query_emb * desckw_emb

        if self.nir_emb_fusion == "lth":
            # Use both the hadamard product of <query,listing>
            # and <query, title>
            mlp_input = tf.concat([hadamard_title, hadamard], axis=1)
        elif self.nir_emb_fusion == "lh":
            # Use the hadamard product of <query,listing>
            mlp_input = hadamard
        elif self.nir_emb_fusion == "tslh":
            # Use the hadamard product of query and <title, shop, listing>
            mlp_input = tf.concat([hadamard_title, hadamard_shop, hadamard], axis=1)
        elif self.nir_emb_fusion == "tdh":
            # Use the hadamard product of query and <title, description keywords>
            mlp_input = tf.concat([hadamard_title, hadamard_desckw], axis=1)
        elif self.nir_emb_fusion == "tsdh":
            # Use the hadamard product of query and <title, shop, description keywords>
            mlp_input = tf.concat([hadamard_title, hadamard_shop, hadamard_desckw], axis=1)
        elif self.nir_emb_fusion == "tsdlh":
            # Use the hadamard product of query and <title, shop, description keywords, listing>
            mlp_input = tf.concat(
                [hadamard_title, hadamard_shop, hadamard_desckw, hadamard], axis=1
            )
        elif self.nir_emb_fusion == "alibaba":
            # Use embedding fusion similar to described in Alibaba's ReprBERT, section 3.2:
            # https://drive.google.com/file/d/1hvxPNZ1zx9H6TJZjHnnXP1kA--uI4yGX/view?usp=sharing
            mlp_input = tf.concat(
                [
                    tf.maximum(query_emb, title_emb),
                    query_emb - title_emb,
                    query_emb + title_emb,
                    tf.maximum(query_emb, shop_emb),
                    query_emb - shop_emb,
                    query_emb + shop_emb,
                    tf.maximum(query_emb, desckw_emb),
                    query_emb - desckw_emb,
                    query_emb + desckw_emb,
                ],
                axis=1,
            )
        elif self.nir_emb_fusion == "tdh_qsdot":
            # Use embedding fusion similar to described in Alibaba's ReprBERT, section 3.2:
            # https://drive.google.com/file/d/1hvxPNZ1zx9H6TJZjHnnXP1kA--uI4yGX/view?usp=sharing
            query_shop_dot = tf.reduce_sum(hadamard_shop, axis=1, keepdims=True)
            mlp_input = tf.concat([hadamard_title, hadamard_desckw, query_shop_dot], axis=1)
        elif self.nir_emb_fusion == "qlt":
            # Concat embeddings of query (q), listing (l), title (t), similar below
            mlp_input = tf.concat([query_emb, listing_emb, title_emb], axis=1)
        elif self.nir_emb_fusion == "qtsl":
            mlp_input = tf.concat([query_emb, title_emb, shop_emb, listing_emb], axis=1)
        elif self.nir_emb_fusion == "qtsd":
            mlp_input = tf.concat([query_emb, title_emb, shop_emb, desckw_emb], axis=1)
        elif self.nir_emb_fusion == "qtd":
            mlp_input = tf.concat([query_emb, title_emb, desckw_emb], axis=1)
        else:
            raise ValueError(f"Unsupported emb fusion {self.nir_emb_fusion}")

        logits = self.mlp(mlp_input)
        if output_logits:
            return logits
        else:
            return tf.nn.softmax(logits, axis=1)

    def get_input_emb(
        self, x, entity: Literal["query", "listing", "title", "shop", "desckw"], training=False
    ):
        if entity == "query":
            embs = self.nir_wrapper.embed_query_features(x, training=training)
        elif entity == "listing":
            embs = self.nir_wrapper.embed_listing_features(x, training=training, use_hqi=False)
        elif entity == "title":
            embs = self.nir_wrapper.embed_title_features_as_query(x, training=training)
        elif entity == "shop":
            embs = self.nir_wrapper.embed_shop_features_as_query(x, training=training)
        elif entity == "desckw":
            embs = self.nir_wrapper.embed_desckw_features_as_query(x, training=training)
        else:
            raise ValueError(f"Unsupported entity {entity}")

        emb = embs[self.nir_emb_key]

        proj_layer = (self.nir_proj_layers or {}).get(entity)
        if proj_layer is not None:
            emb = proj_layer(emb)
        return emb

    def call(self, query_listing_features, training=False):
        listing_emb = self.get_input_emb(query_listing_features, "listing", training)
        listing_title_emb = self.get_input_emb(query_listing_features, "title", training)
        query_emb = self.get_input_emb(query_listing_features, "query", training)
        shop_emb = self.get_input_emb(query_listing_features, "shop", training)
        desckw_emb = self.get_input_emb(query_listing_features, "desckw", training)
        return self.score_query_listing_qltsd(
            query_emb, listing_emb, listing_title_emb, shop_emb, desckw_emb, self.call_fn_use_logits
        )

    @tf.function(
        input_signature=[
            tf.TensorSpec(shape=[None], dtype=tf.string),
            tf.TensorSpec(shape=[None], dtype=tf.string),
            tf.TensorSpec(shape=[None], dtype=tf.string),
        ]
    )
    def embed_listings(self, title, tags, taxonomyPath):
        listing_input_features = self.nir_wrapper.preproc_listing(
            {
                "title": title,
                "tags": tags,
                "taxonomyPath": taxonomyPath,
                "clickTopQuery": None,
                "cartTopQuery": None,
                "purchaseTopQuery": None,
            }
        )

        listing_emb = self.get_input_emb(listing_input_features, "listing", training=False)
        title_emb = self.get_input_emb(listing_input_features, "title", training=False)
        return {"listing_emb": listing_emb, "title_emb": title_emb}

    @tf.function(input_signature=[tf.TensorSpec(shape=[None], dtype=tf.string)])
    def embed_queries(self, query):
        query_input_features = self.nir_wrapper.preproc_query(query)
        query_emb = self.get_input_emb(query_input_features, "query", training=False)
        return {"query_emb": query_emb}

    @tf.function(input_signature=[tf.TensorSpec(shape=[None], dtype=tf.string)])
    def embed_desckws(self, desckw):
        desckw_input_features = self.nir_wrapper.preproc_tags(desckw)
        desckw_input_features = {
            k.replace("tags", "desckw"): v for k, v in desckw_input_features.items()
        }
        emb = self.get_input_emb(desckw_input_features, "desckw", training=False)
        return {"desckw_emb": emb}

    def get_embedding_dim(self):
        proj_dim = self.nir_emb_proj or 0
        if proj_dim == 0:
            return self.nir_wrapper.nir_model.hidden_layer_sizes[-1]
        return proj_dim

    def export_model(self, export_path):
        embedding_dim = self.get_embedding_dim()

        @tf.function(
            input_signature=[
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
            ]
        )
        def score_query_listing_proba(query_emb, listing_emb, title_emb, shop_emb, desckw_emb):
            probas = self.score_query_listing_qltsd(
                query_emb=query_emb,
                listing_emb=listing_emb,
                title_emb=title_emb,
                shop_emb=shop_emb,
                desckw_emb=desckw_emb,
                output_logits=False,
            )
            return {"probas": probas}

        signatures = {
            "embed_listings": self.embed_listings.get_concrete_function(),
            "embed_queries": self.embed_queries.get_concrete_function(),
            "embed_desckws": self.embed_desckws.get_concrete_function(),
            "score_query_listing_proba": score_query_listing_proba.get_concrete_function(),
        }
        self.save(export_path, signatures=signatures, include_optimizer=False)

    def get_config(self):
        # Configuration includes all constructor arguments, which we assume are saved as instance attributes
        # except for `nir_wrapper`, which we save via its underlying paths
        init_sig = inspect.signature(SemrelModelServingV2.__init__)
        saved_args = set(init_sig.parameters) - {"self", "nir_wrapper"}
        config = {k: getattr(self, k) for k in saved_args}
        config["nir_model_path"] = self.nir_wrapper.nir_model_path
        config["nir_tft_path"] = self.nir_wrapper.nir_tft_path
        return config

    @classmethod
    def from_config(cls, config_dict: dict, **kwargs):
        nir_model_path = config_dict.pop("nir_model_path")
        nir_tft_path = config_dict.pop("nir_tft_path")
        nir_wrapper = NirWrapper.from_model_paths(nir_model_path, nir_tft_path)
        config_dict["nir_wrapper"] = nir_wrapper
        return SemrelModelServingV2(**config_dict)

    @staticmethod
    def load(model_path, unfuse_layernorm=False) -> "SemrelModelServingV2":
        model: SemrelModelServingV2 = tf.keras.models.load_model(
            model_path,
            compile=False,
            custom_objects={"SemrelModelServingV2": SemrelModelServingV2},
        )

        if unfuse_layernorm:
            for layer in model.mlp.layers:
                if isinstance(layer, tf.keras.layers.LayerNormalization):
                    logger.info("UNFUSING LAYER NORM MEOOOOOOWW!!!!")
                    layer._fused = False
        return model
    



class CachedSemrelServingV2(tf.keras.models.Model):
    def __init__(
        self,
        listing_ids: tf.Tensor,
        listing_emb: tf.Variable,
        title_emb: tf.Variable,
        desckw_emb: tf.Variable,
        shopname_semrel_emb: tf.Variable,
        shopname_nir_emb: tf.Variable,
        semrel_model: SemrelModelServingV2,
        nir_model,
    ):
        """
        :param listing_ids: 1-d tensor of integer listing IDs whose embeddings will be cached in the model
        :param listing_emb: 2-d tensor (N_LISTINGS x EMB_DIM) with listing embeddings for every listing
        :param title_emb: 2-d tensor (N_LISTINGS x EMB_DIM) with title embeddings for every listing
        :param desckw_emb: 2-d tensor (N_LISTINGS x EMB_DIM) with desc_ngram embeddings for every listing
        :param shopname_semrel_emb: 2-d tensor (N_LISTINGS x EMB_DIM) with shop name Semrel embeddings for every listing
        :param shopname_nir_emb: 2-d tensor (N_LISTINGS x EMB_DIM) with shop name NIR embeddings for every listing
        :param semrel_model: Trained semantic relevance model (used to get relevance score)
        :param nir_model: Neural IR model (used to get shop score)
        """
        super().__init__()
        self.listing_ids = listing_ids
        self.listing2idx = self.create_listing2idx_lookup(listing_ids)
        self.listing_embs = listing_emb
        self.title_embs = title_emb
        self.semrel_model = semrel_model
        self.nir_model = nir_model
        self.desckw_embs = desckw_emb
        self.shopname_semrel_embs = shopname_semrel_emb
        self.shopname_nir_embs = shopname_nir_emb
        logger.info("LOSE INHERITANCE FOR CACHE")

        mlp_layers = []
        mlp_layers.append(tf.keras.layers.Dense(256, activation="relu"))
        lnorm_layer = tf.keras.layers.LayerNormalization()
        lnorm_layer._fused = False
        mlp_layers.append(lnorm_layer)
        mlp_layers.append(tf.keras.layers.Dense(3))
        self.mlp_layers = tf.keras.Sequential(mlp_layers)

    @staticmethod
    def create_listing2idx_lookup(
        listing_ids: tf.Tensor, missing_value=-1
    ) -> tf.lookup.StaticHashTable:
        """
        :param listing_ids: int64 tensor of listing IDs
        :returns: A lookup table from listing ID to index, which returns -1 when a listing ID is missing
        """
        idx = tf.range(len(listing_ids), dtype=tf.int32)
        listing2idx_init = tf.lookup.KeyValueTensorInitializer(keys=listing_ids, values=idx)
        return tf.lookup.StaticHashTable(listing2idx_init, default_value=missing_value)

    def call(self, inputs: ModelInput):
        query = inputs["query"]
        listing_ids = inputs["listing_ids"]

        query_1d = query[None]
        semrel_query_emb = self.semrel_model.signatures["embed_queries"](query_1d)["query_emb"]
        nir_query_emb = self.nir_model.signatures["embed_raw_queries"](query_1d)["embedding"]

        # Look up embeddings for listings
        listing_idxs = self.listing2idx.lookup(listing_ids)
        listing_exists_mask = tf.not_equal(listing_idxs, -1)
        valid_idxs = tf.boolean_mask(listing_idxs, listing_exists_mask)
        valid_listing_ids = tf.boolean_mask(listing_ids, listing_exists_mask)
        missing_listing_ids = tf.boolean_mask(listing_ids, tf.logical_not(listing_exists_mask))

        embs = {
            "listing": tf.gather(self.listing_embs, valid_idxs),
            "title": tf.gather(self.title_embs, valid_idxs),
            "desckw": tf.gather(self.desckw_embs, valid_idxs),
            "shopname_semrel": tf.gather(self.shopname_semrel_embs, valid_idxs),
            "shopname_nir": tf.gather(self.shopname_nir_embs, valid_idxs),
        }
        for k in list(embs):
            # Cast back to float32 for calculation
            embs[k] = tf.cast(embs[k], tf.float32)

        # Concatenate query embedding with valid listing embeddings
        n_valid_listings = tf.shape(embs["listing"])[0]
        semrel_query_emb = tf.repeat(semrel_query_emb, repeats=n_valid_listings, axis=0)
        nir_query_emb = tf.repeat(nir_query_emb, repeats=n_valid_listings, axis=0)

        # Get model scores for each input
        # probas = self.semrel_model.signatures["score_query_listing_proba"](
        #     listing_emb=embs["listing"],
        #     query_emb=semrel_query_emb,
        #     desckw_emb=embs["desckw"],
        #     title_emb=embs["title"],
        #     shop_emb=embs["shopname_semrel"],
        # )["probas"]

        hadamard = semrel_query_emb * embs["listing"]
        hadamard_title = semrel_query_emb * embs["title"]
        hadamard_shop = semrel_query_emb * embs["shopname_semrel"]
        hadamard_desckw = semrel_query_emb * embs["desckw"]

        mlp_input = tf.concat([hadamard_title, hadamard_shop, hadamard_desckw, hadamard], axis=1)

        logger.info("Using new MLP layers")
        logits = self.mlp_layers(mlp_input)
        probas = tf.nn.softmax(logits, axis=1)

        # logger.info("USING RANDOM PROBAS + MLP INPUT")
        # probas = tf.reduce_sum(mlp_input) * tf.random.uniform(shape=[n_valid_listings, 3])

        # Score is probability of not irrelevant, so 1 minus probability of irrelevant
        scores = 1 - probas[:, 0]
        scores = tf.reshape(scores, [-1])

        # Get shop name scores
        shop_scores = tf.reduce_sum(nir_query_emb * embs["shopname_nir"], axis=1)

        return {
            "listing_ids": valid_listing_ids,
            "scores": scores,
            "shop_scores": shop_scores,
            "missing_listing_ids": missing_listing_ids,
            "scores_irrelevant": tf.reshape(probas[:, 0], [-1]),
            "scores_partial": tf.reshape(probas[:, 1], [-1]),
            "scores_relevant": tf.reshape(probas[:, 2], [-1]),
        }

    def create_export_signature(self):
        @tf.function(
            input_signature=[
                tf.TensorSpec(shape=[], dtype=tf.string),
                tf.TensorSpec(shape=[None], dtype=tf.int64),
            ]
        )
        def score_listings(query, listing_ids):
            return self({"query": query, "listing_ids": listing_ids})

        @tf.function(input_signature=[tf.TensorSpec(shape=[None], dtype=tf.string)])
        def score_examples_batch(tf_examples):
            # After this, listing_ids is a 2-d ragged tensor, each row is one example
            # query is 1-d tensor of strings
            parsed = tf.io.parse_example(
                tf_examples,
                {
                    "query": tf.io.FixedLenFeature([], tf.string),
                    "listing_ids": tf.io.RaggedFeature(tf.int64),
                },
            )

            # In output, all values are 2-d ragged tensors, each row has the listing_ids/scores/missing_listing_ids from one example
            output = tf.map_fn(
                self.call,
                parsed,
                fn_output_signature={
                    "listing_ids": tf.RaggedTensorSpec(shape=[None], dtype=tf.int64),
                    "scores": tf.RaggedTensorSpec(shape=[None], dtype=tf.float32),
                    "shop_scores": tf.RaggedTensorSpec(shape=[None], dtype=tf.float32),
                    "missing_listing_ids": tf.RaggedTensorSpec(shape=[None], dtype=tf.int64),
                    "scores_irrelevant": tf.RaggedTensorSpec(shape=[None], dtype=tf.float32),
                    "scores_partial": tf.RaggedTensorSpec(shape=[None], dtype=tf.float32),
                    "scores_relevant": tf.RaggedTensorSpec(shape=[None], dtype=tf.float32),
                },
            )

            # Concatenate the ragged rows by flattening the values
            flattened = {k: v.values for k, v in output.items()}
            return flattened

        signatures = {
            "score_listings": score_listings,
            "score_examples_batch": score_examples_batch,
        }
        return signatures

    def export_model(self, model_path, sample_query_listing_path):
        # Run dummy input through model, needed to get it build
        _ = self({"query": tf.constant("hi"), "listing_ids": tf.constant([42], tf.int64)})
        signatures = self.create_export_signature()
        self.save(model_path, signatures=signatures)
        warmup.save_warmup_files(model_path, sample_query_listing_path, logger)
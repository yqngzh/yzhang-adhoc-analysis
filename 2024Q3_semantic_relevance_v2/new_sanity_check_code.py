class SemrelModel(tf.keras.models.Model):
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
                k: create_proj_layer() for k in ("query", "listing", "title")
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

    def score_query_listing(
        self,
        query_emb: tf.Tensor,
        listing_emb: tf.Tensor,
        title_emb: tf.Tensor,
        output_logits: bool,
    ):
        hadamard = query_emb * listing_emb
        hadamard_title = query_emb * title_emb

        if self.nir_emb_fusion == "lth":
            # Use both the hadamard product of <query,listing>
            # and <query, title>
            mlp_input = tf.concat([hadamard_title, hadamard], axis=1)
        elif self.nir_emb_fusion == "lh":
            # Use the hadamard product of <query,listing>
            mlp_input = hadamard
        elif self.nir_emb_fusion == "alibaba":
            # Use embedding fusion similar to described in Alibaba's ReprBERT, section 3.2:
            # https://drive.google.com/file/d/1hvxPNZ1zx9H6TJZjHnnXP1kA--uI4yGX/view?usp=sharing
            mlp_input = tf.concat(
                [
                    tf.maximum(query_emb, listing_emb),
                    query_emb - listing_emb,
                    query_emb + listing_emb,
                    tf.maximum(query_emb, title_emb),
                    query_emb - title_emb,
                    query_emb + title_emb,
                ],
                axis=1,
            )
        elif self.nir_emb_fusion == "qlt":
            mlp_input = tf.concat([query_emb, listing_emb, title_emb], axis=1)
        else:
            raise ValueError(f"Unsupported emb fusion {self.nir_emb_fusion}")

        logits = self.mlp(mlp_input)
        if output_logits:
            return logits
        else:
            return tf.nn.softmax(logits, axis=1)

    def get_input_emb(self, x, entity: Literal["query", "listing", "title"], training=False):
        if entity == "query":
            embs = self.nir_wrapper.embed_query_features(x, training=training)
        elif entity == "listing":
            embs = self.nir_wrapper.embed_listing_features(x, training=training, use_hqi=False)
        elif entity == "title":
            embs = self.nir_wrapper.embed_title_features_as_query(x, training=training)
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
        return self.score_query_listing(
            query_emb, listing_emb, listing_title_emb, self.call_fn_use_logits
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
            ]
        )
        def score_query_listing_proba(query_emb, listing_emb, title_emb):
            probas = self.score_query_listing(
                query_emb, listing_emb, title_emb, output_logits=False
            )
            return {"probas": probas}

        signatures = {
            "embed_listings": self.embed_listings.get_concrete_function(),
            "embed_queries": self.embed_queries.get_concrete_function(),
            "score_query_listing_proba": score_query_listing_proba.get_concrete_function(),
        }
        self.save(export_path, signatures=signatures, include_optimizer=False)

    def get_config(self):
        # Configuration includes all constructor arguments, which we assume are saved as instance attributes
        # except for `nir_wrapper`, which we save via its underlying paths
        init_sig = inspect.signature(SemrelModel.__init__)
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
        return SemrelModel(**config_dict)

    @staticmethod
    def load(model_path, unfuse_layernorm=False) -> "SemrelModel":
        model: SemrelModel = tf.keras.models.load_model(
            model_path,
            compile=False,
            custom_objects={"SemrelModel": SemrelModel},
        )

        if unfuse_layernorm:
            for layer in model.mlp.layers:
                if isinstance(layer, tf.keras.layers.LayerNormalization):
                    layer._fused = False
        return model

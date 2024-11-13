class SemrelModelServingV2(SemrelModel):
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
        super().__init__(
            nir_wrapper=nir_wrapper,
            n_classes=n_classes,
            hidden_sizes=hidden_sizes,
            activation=activation,
            call_fn_use_logits=call_fn_use_logits,
            nir_emb_key=nir_emb_key,
            nir_emb_proj=nir_emb_proj,
            nir_emb_fusion=nir_emb_fusion,
        )

        if self.use_nir_emb_proj:

            def create_proj_layer():
                return tf.keras.Sequential(
                    [
                        tf.keras.layers.Dense(nir_emb_proj, activation=activation),
                        tf.keras.layers.LayerNormalization(),
                    ]
                )

            self.nir_proj_layers: Dict | None = {
                k: create_proj_layer() for k in ("query", "listing", "title", "shop")
            }
        else:
            self.nir_proj_layers = None

    def score_query_listing(
        self,
        query_emb: tf.Tensor,
        listing_emb: tf.Tensor,
        title_emb: tf.Tensor,
        shop_emb: tf.Tensor,
        output_logits: bool,
    ):
        hadamard = query_emb * listing_emb
        hadamard_title = query_emb * title_emb
        hadamard_shop = query_emb * shop_emb

        if self.nir_emb_fusion == "lth":
            # Use both the hadamard product of <query,listing>
            # and <query, title>
            mlp_input = tf.concat([hadamard_title, hadamard], axis=1)
        elif self.nir_emb_fusion == "lh":
            # Use the hadamard product of <query,listing>
            mlp_input = hadamard
        elif self.nir_emb_fusion == "tslh":
            mlp_input = tf.concat([hadamard_title, hadamard_shop, hadamard], axis=1)
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
                    tf.maximum(query_emb, shop_emb),
                    query_emb - shop_emb,
                    query_emb + shop_emb,
                ],
                axis=1,
            )
        elif self.nir_emb_fusion == "qlt":
            mlp_input = tf.concat([query_emb, listing_emb, title_emb], axis=1)
        elif self.nir_emb_fusion == "qtsl":
            mlp_input = tf.concat([query_emb, title_emb, shop_emb, listing_emb], axis=1)
        else:
            raise ValueError(f"Unsupported emb fusion {self.nir_emb_fusion}")

        logits = self.mlp(mlp_input)
        if output_logits:
            return logits
        else:
            return tf.nn.softmax(logits, axis=1)

    def get_input_emb(
        self, x, entity: Literal["query", "listing", "title", "shop"], training=False
    ):
        if entity == "query":
            embs = self.nir_wrapper.embed_query_features(x, training=training)
        elif entity == "listing":
            embs = self.nir_wrapper.embed_listing_features(x, training=training, use_hqi=False)
        elif entity == "title":
            embs = self.nir_wrapper.embed_title_features_as_query(x, training=training)
        elif entity == "shop":
            embs = self.nir_wrapper.embed_shop_features_as_query(x, training=training)
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
        return self.score_query_listing(
            query_emb, listing_emb, listing_title_emb, shop_emb, self.call_fn_use_logits
        )

    def export_model(self, export_path):
        embedding_dim = self.get_embedding_dim()

        @tf.function(
            input_signature=[
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
            ]
        )
        def score_query_listing_proba(query_emb, listing_emb, title_emb, shop_emb):
            probas = self.score_query_listing(
                query_emb, listing_emb, title_emb, shop_emb, output_logits=False
            )
            return {"probas": probas}

        signatures = {
            "embed_listings": self.embed_listings.get_concrete_function(),
            "embed_queries": self.embed_queries.get_concrete_function(),
            "score_query_listing_proba": score_query_listing_proba.get_concrete_function(),
        }
        self.save(export_path, signatures=signatures, include_optimizer=False)
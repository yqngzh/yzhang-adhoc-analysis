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
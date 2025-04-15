class VLLMCompletionsModelHandler(ModelHandler[str,
                                               PredictionResult,
                                               _VLLMModelServer]):
  def __init__(
      self,
      model_name: str,
      vllm_server_kwargs: Optional[Dict[str, str]] = None):
    """Implementation of the ModelHandler interface for vLLM using text as
    input.

    Example Usage::

      pcoll | RunInference(VLLMModelHandler(model_name='facebook/opt-125m'))

    Args:
      model_name: The vLLM model. See
        https://docs.vllm.ai/en/latest/models/supported_models.html for
        supported models.
      vllm_server_kwargs: Any additional kwargs to be passed into your vllm
        server when it is being created. Will be invoked using
        `python -m vllm.entrypoints.openai.api_serverv <beam provided args>
        <vllm_server_kwargs>`. For example, you could pass
        `{'echo': 'true'}` to prepend new messages with the previous message.
        For a list of possible kwargs, see
        https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#extra-parameters-for-completions-api
    """
    self._model_name = model_name
    self._vllm_server_kwargs: Dict[str, str] = vllm_server_kwargs or {}
    self._env_vars = {}

  def load_model(self) -> _VLLMModelServer:
    return _VLLMModelServer(self._model_name, self._vllm_server_kwargs)

  async def _async_run_inference(
      self,
      batch: Sequence[str],
      model: _VLLMModelServer,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    client = getAsyncVLLMClient(model.get_server_port())
    inference_args = inference_args or {}
    async_predictions = []
    for prompt in batch:
      try:
        completion = client.completions.create(
            model=self._model_name, prompt=prompt, **inference_args)
        async_predictions.append(completion)
      except Exception as e:
        model.check_connectivity()
        raise e

    predictions = []
    for p in async_predictions:
      try:
        predictions.append(await p)
      except Exception as e:
        model.check_connectivity()
        raise e

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def run_inference(
      self,
      batch: Sequence[str],
      model: _VLLMModelServer,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of text strings.

    Args:
      batch: A sequence of examples as text strings.
      model: A _VLLMModelServer containing info for connecting to the server.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return asyncio.run(self._async_run_inference(batch, model, inference_args))

  def share_model_across_processes(self) -> bool:
    return True
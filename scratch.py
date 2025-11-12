from enum import Enum
from typing import Dict, List, Tuple

import torch


class MetricSpace(Enum):
    """Metric space that the model embeds queries and listings into"""

    COSINE = "COSINE"
    L2 = "L2"
    MAXSIM = "MAXSIM"
    DOT = "DOT"


class ScoreSlice(Enum):
    LABELED = "LABELED"  # positive pairs
    IN_BATCH_NEG = "IN_BATCH_NEG"  # positive listings from other queries in batch
    RANDOM_NEG = "RANDOM_NEG"  # true random negatives
    HARD_NEG = "HARD_NEG"  # mined hard negatives (through megaminer)

    def is_negative(self):
        return self != ScoreSlice.LABELED

    def __lt__(self, other):
        # This is added so that ScoreSlice can be used in dictionary keys of nn layer subclasses
        # it throws upon layer construction otherwise
        # TODO: check if this is still needed
        return self.value < other.value


ScoreSlices = Dict[ScoreSlice, torch.Tensor]


def extract_score_slices(X: torch.Tensor, num_hard_negatives_per_query: int = 0) -> ScoreSlices:
    """
    Separate the distance matrix into components
    Symbol key:
         - P: positive batch size; number of positive query listing pairs in batch
         - N: number of sampled random negative listings, from original negative batch size, or after megaminer
     Args:
         X: torch tensor of shape [P, P + P * num_hard_negatives_per_query + N].
             the distance matrics between all queries and listings in batch
             columns: [ ...in batch neg..., ...extracted random hard negs by megaminer..., ...totally random negs...]
             It will be separated into the following components by this function
                - X_labeled     shape [P, 1],   has distances between the labeled (query, listing) pairs
                - X_inbatch_neg shape [P, P-1], has distances between each labeled query and listing
                - X_random_neg  shape [P, N]],  has distances beteween each labeled query and random listing
                - X_hard_neg    shape [P, P * num_hard_negatives_per_query],  has distances beetween each labeled
                                                                              query and hard mined listing
         num_hard_negatives_per_query: int. number of hard negatives mined per query by megaminer
     Returns:
         ScoreSlices
    """

    num_labeled = X.shape[0]  # P

    X_diag = torch.diagonal(X)
    X_labeled = X_diag[:, None]  # (P, 1)

    X_inbatch_raw = X[:, :num_labeled]
    mask = ~torch.eye(
        num_labeled, dtype=torch.bool, device=X.device
    )  # (P, P) mask, all values true, diagonal elements false
    X_inbatch_neg = X_inbatch_raw[mask].view(num_labeled, num_labeled - 1)  # (P, P-1), remove diagonal

    X_random_neg = X[:, num_labeled * (1 + num_hard_negatives_per_query) :]  # (P, N)

    score_slices: ScoreSlices = {
        ScoreSlice.LABELED: X_labeled,
        ScoreSlice.IN_BATCH_NEG: X_inbatch_neg,
        ScoreSlice.RANDOM_NEG: X_random_neg,
    }

    if num_hard_negatives_per_query > 0:
        score_slices[ScoreSlice.HARD_NEG] = X[
            :, num_labeled : num_labeled * (1 + num_hard_negatives_per_query)
        ]  # (P, P * num_hard_negatives_per_query)

    return score_slices


class MultipartHingeLoss(torch.nn.Module):
    """
    All negative slices use the same loss function: if similarity is too high (distance is too low),
       exceeding threshold `eps_negative`, there is a penalty as similarity gets even higher.
    The loss for positives is multi-part because it has different settings for different types of labels
      (no_event, click, cart, purchase, etc.). This is stored in `epsilons`. If similarity is too low (distance too high)
      than the configured threshold for a type, there is a penalty as similarity gets even lower.
    """

    def __init__(
        self,
        epsilons: List[Tuple[float, bool]],
        eps_negative: float,
        metric_space: MetricSpace,
        power_of_loss: int = 2,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.epsilons = epsilons
        self.eps_negative = eps_negative
        self.metric_space = metric_space
        self.power_of_loss = power_of_loss

    def rightmost_one_hot_encode_positive_counts(self, y_true: torch.Tensor, include_weight: bool = False):
        """Takes 2-D matrix of counts and one-hot encodes it using the rightmost positive count"""
        mask = y_true > 0

        # flip mask along the last dimension to find first True from the right
        flipped = torch.flip(mask, dims=[1])
        idx_from_right = flipped.float().argmax(dim=1)
        has_positive = mask.any(dim=1)
        rightmost_idx = (y_true.size(1) - 1) - idx_from_right

        out = torch.zeros_like(y_true, dtype=torch.float32)
        # fill 1 at rightmost index with positive counts, only for rows with positive counts
        out[has_positive, rightmost_idx[has_positive]] = 1.0

        if include_weight:
            out = out * y_true.float()
        return out

    def labeled_loss(self, labeled_scores: torch.Tensor, y_true: torch.Tensor):
        y_true_one_hot = self.rightmost_one_hot_encode_positive_counts(y_true)
        labeled_loss_parts = []
        for margin, is_positive_label in self.epsilons:
            is_similarity_metric = self.metric_space in (MetricSpace.COSINE, MetricSpace.MAXSIM, MetricSpace.DOT)
            sign = 1 if (is_positive_label ^ is_similarity_metric) else -1  # xor, true when different, false when same
            l_part = torch.clamp(sign * (labeled_scores - margin), min=0.0)

            l_part = torch.pow(l_part, self.power_of_loss)  # no op if power_of_loss = 1

            labeled_loss_parts.append(l_part)

        # n_queries X n_targets matrix with the losses for positive (q,l) pairs that would be seen
        #   for each target (interaction) type
        l_vec = torch.cat(labeled_loss_parts, dim=1)

        # Loss for positive pairs.
        # Multiply by one-hot encoding of interaction type to get appropriate loss for interaction,
        #   then sum everything up
        return torch.sum(l_vec * y_true_one_hot, dim=1)

    def negative_loss(self, negative_scores: torch.Tensor):
        # n_queries X (n_random_negatives) matrix of losses for random negatives
        sign = 1 if self.metric_space in (MetricSpace.COSINE, MetricSpace.MAXSIM, MetricSpace.DOT) else -1
        loss = torch.pow(torch.clamp(sign * (negative_scores - self.eps_negative), min=0.0), self.power_of_loss)
        return loss

    def forward(self, y_pred: ScoreSlices, y_true: torch.Tensor) -> ScoreSlices:
        losses = {ScoreSlice.LABELED: self.labeled_loss(y_pred[ScoreSlice.LABELED], y_true)}
        losses.update({slice: self.negative_loss(scores) for slice, scores in y_pred.items() if slice.is_negative()})
        return losses
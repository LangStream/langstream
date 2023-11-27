from typing import List, Dict, Tuple, Callable

import numpy as np

VectorType = List[float]

# distance definitions. These all work batched in the first argument.
def distance_dot_product(
    embedding_vectors: List[VectorType], reference_embedding_vector: VectorType
) -> List[float]:
    """
    Given a list [emb_i] and a reference rEmb vector,
    return a list [distance_i] where each distance is
        distance_i = distance(emb_i, rEmb)
    At the moment only the dot product is supported
    (which for unitary vectors is the cosine difference).

    Not particularly optimized.
    """
    v1s = np.array(embedding_vectors, dtype=float)
    v2 = np.array(reference_embedding_vector, dtype=float)
    return list(
        np.dot(
            v1s,
            v2.T,
        )
    )


def distance_cos_difference(
    embedding_vectors: List[VectorType], reference_embedding_vector: VectorType
) -> List[float]:
    v1s = np.array(embedding_vectors, dtype=float)
    v2 = np.array(reference_embedding_vector, dtype=float)
    return list(
        np.dot(
            v1s,
            v2.T,
        )
        / (np.linalg.norm(v1s, axis=1) * np.linalg.norm(v2))
    )


def distance_l1(
    embedding_vectors: List[VectorType], reference_embedding_vector: VectorType
) -> List[float]:
    v1s = np.array(embedding_vectors, dtype=float)
    v2 = np.array(reference_embedding_vector, dtype=float)
    return list(np.linalg.norm(v1s - v2, axis=1, ord=1))


def distance_l2(
    embedding_vectors: List[VectorType], reference_embedding_vector: VectorType
) -> List[float]:
    v1s = np.array(embedding_vectors, dtype=float)
    v2 = np.array(reference_embedding_vector, dtype=float)
    return list(np.linalg.norm(v1s - v2, axis=1, ord=2))


def distance_max(
    embedding_vectors: List[VectorType], reference_embedding_vector: VectorType
) -> List[float]:
    v1s = np.array(embedding_vectors, dtype=float)
    v2 = np.array(reference_embedding_vector, dtype=float)
    return list(np.linalg.norm(v1s - v2, axis=1, ord=np.inf))


# The tuple is:
#   (
#       function,
#       sorting 'reverse' argument, nearest-to-farthest
#   )
# (i.e. True means that:
#     - in that metric higher is closer and that
#     - cutoff should be metric > threshold)
distance_metrics: Dict[
    str, Tuple[Callable[[List[VectorType], VectorType], List[float]], bool]
] = {
    "cos": (
        distance_cos_difference,
        True,
    ),
    "dot": (
        distance_dot_product,
        True,
    ),
    "l1": (
        distance_l1,
        False,
    ),
    "l2": (
        distance_l2,
        False,
    ),
    "max": (
        distance_max,
        False,
    ),
}

from cassio.table.base_table import BaseTable
from cassio.table.mixins import (
    ClusteredMixin,
    MetadataMixin,
    VectorMixin,
    ElasticKeyMixin,
    #
    TypeNormalizerMixin,
)


class PlainCassandraTable(TypeNormalizerMixin, BaseTable):
    pass


class ClusteredCassandraTable(TypeNormalizerMixin, ClusteredMixin, BaseTable):
    clustered = True
    pass


class ClusteredMetadataCassandraTable(
    TypeNormalizerMixin, MetadataMixin, ClusteredMixin, BaseTable
):
    clustered = True
    pass


class MetadataCassandraTable(TypeNormalizerMixin, MetadataMixin, BaseTable):
    pass


class VectorCassandraTable(TypeNormalizerMixin, VectorMixin, BaseTable):
    pass


class ClusteredVectorCassandraTable(
    TypeNormalizerMixin, VectorMixin, ClusteredMixin, BaseTable
):
    clustered = True
    pass


class ClusteredMetadataVectorCassandraTable(
    TypeNormalizerMixin, MetadataMixin, ClusteredMixin, VectorMixin, BaseTable
):
    clustered = True
    pass


class MetadataVectorCassandraTable(
    TypeNormalizerMixin, MetadataMixin, VectorMixin, BaseTable
):
    pass


class ElasticCassandraTable(TypeNormalizerMixin, ElasticKeyMixin, BaseTable):
    elastic = True
    pass


class ClusteredElasticCassandraTable(
    TypeNormalizerMixin, ClusteredMixin, ElasticKeyMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ClusteredElasticMetadataCassandraTable(
    TypeNormalizerMixin, MetadataMixin, ElasticKeyMixin, ClusteredMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ElasticMetadataCassandraTable(
    TypeNormalizerMixin, MetadataMixin, ElasticKeyMixin, BaseTable
):
    elastic = True
    pass


class ElasticVectorCassandraTable(
    TypeNormalizerMixin, VectorMixin, ElasticKeyMixin, BaseTable
):
    elastic = True
    pass


class ClusteredElasticVectorCassandraTable(
    TypeNormalizerMixin, ClusteredMixin, ElasticKeyMixin, VectorMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ClusteredElasticMetadataVectorCassandraTable(
    TypeNormalizerMixin,
    MetadataMixin,
    ElasticKeyMixin,
    ClusteredMixin,
    VectorMixin,
    BaseTable,
):
    clustered = True
    elastic = True
    pass


class ElasticMetadataVectorCassandraTable(
    MetadataMixin, ElasticKeyMixin, VectorMixin, BaseTable
):
    elastic = True
    pass

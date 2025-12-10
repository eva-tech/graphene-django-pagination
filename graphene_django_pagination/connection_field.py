import re
import math
from functools import partial

from graphene import Int, String
from graphene_django.filter import DjangoFilterConnectionField
from graphene_django.utils import maybe_queryset
from django.core.paginator import Paginator
from logging import getLogger
from . import PaginationConnection, PageInfoExtra

MAX_LIMIT_TO_WARN = 1500


logger = getLogger(__name__)

class DjangoPaginationConnectionField(DjangoFilterConnectionField):
    def __init__(
        self,
        type,
        fields=None,
        order_by=None,
        extra_filter_meta=None,
        filterset_class=None,
        *args,
        **kwargs
    ):
        self._type = type
        self._fields = fields
        self._provided_filterset_class = filterset_class
        self._filterset_class = None
        self._extra_filter_meta = extra_filter_meta
        self._base_args = None

        kwargs.setdefault("limit", Int(description="Query limit"))
        kwargs.setdefault("offset", Int(description="Query offset"))
        kwargs.setdefault("ordering", String(description="Query order"))

        super(DjangoPaginationConnectionField, self).__init__(
            type,
            *args,
            **kwargs
        )

    @property
    def type(self):

        class NodeConnection(PaginationConnection):
            total_count = Int()

            class Meta:
                node = self._type
                name = '{}NodeConnection'.format(self._type._meta.name)

            def resolve_total_count(self, info, **kwargs):
                """Resolve the total count of items, using cache if available."""
                if hasattr(info.context, "_CachedDjangoPaginationField"):
                    return info.context._CachedDjangoPaginationField
                return self.iterable.count()

        return NodeConnection

    @classmethod
    def _resolve_connection(cls, connection, args, iterable, max_limit=None, info=None):
        iterable = maybe_queryset(iterable)

        ordering = args.get("ordering")

        if ordering:
            iterable = connection_from_list_ordering(iterable, ordering, connection)

        connection = connection_from_list_slice(
            iterable,
            args,
            connection_type=connection,
            pageinfo_type=PageInfoExtra,
            info=info,
        )
        connection.iterable = iterable

        return connection

    @classmethod
    def resolve_connection(cls, connection, args, iterable, max_limit=None):
        """Hacky way to add info context to the connection resolver."""
        return partial(
            cls._resolve_connection,
            connection,
            args,
            iterable,
            max_limit=max_limit,
        )

    @classmethod
    def connection_resolver(
        cls,
        resolver,
        connection,
        default_manager,
        queryset_resolver,
        max_limit,
        enforce_first_or_last,
        root,
        info,
        **args,
    ):
        """Resolve the connection, ensuring the info context is passed to partials."""
        res = super().connection_resolver(
            resolver,
            connection,
            default_manager,
            queryset_resolver,
            max_limit,
            enforce_first_or_last,
            root,
            info,
            **args,
        )
        if isinstance(res, partial):
            return res(info=info)
        return res


def connection_from_list_slice(
    list_slice, args=None, connection_type=None, pageinfo_type=None, info=None,
):
    args = args or {}
    limit = args.get("limit", None)
    offset = args.get("offset", 0)

    if limit is None:
        try:
            logger.error(f"QUERY_SIZE_TEST_WARNING: Unlimited query for query: {info.operation.name.value}")
        except:
            pass
        
        return connection_type(
            results=list_slice,
            page_info=pageinfo_type(
                has_previous_page=False,
                has_next_page=False
            )
        )
    else:
        assert isinstance(limit, int), "Limit must be of type int"
        assert limit > 0, "Limit must be positive integer greater than 0"

        paginator = Paginator(list_slice, limit)
        _slice = list_slice[offset:(offset+limit)]

        page_num = math.ceil(offset/limit) + 1
        page_num = (
            paginator.num_pages
            if page_num > paginator.num_pages
            else page_num
        )
        page = paginator.page(page_num)

        info.context._CachedDjangoPaginationField = paginator.count
        try:
            if paginator.count >= MAX_LIMIT_TO_WARN:
                logger.error(f"QUERY_SIZE_TEST_WARNING: Query returned {len(list_slice)} results, which is greater than {MAX_LIMIT_TO_WARN}. This may cause performance issues. Query: {info.operation.name.value}")
        except:
            pass
        return connection_type(
            results=_slice,
            page_info=pageinfo_type(
                has_previous_page=page.has_previous(),
                has_next_page=page.has_next()
            )
        )


def connection_from_list_ordering(items_list, ordering, connection):
    field, order = ordering.replace(' ', '').split(',')
    field = re.sub(r'(?<!^)(?=[A-Z])', '_', field).lower()
    order = '-' if order == 'desc' else ''

    if (connection
        and connection._meta
        and connection._meta.node
        and hasattr(connection._meta.node, 'ordering')
        and callable(getattr(connection._meta.node, 'ordering'))):
        return connection._meta.node.ordering(items_list, field, order)
    else:
        return items_list.order_by(f'{order}{field}')

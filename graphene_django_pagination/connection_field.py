import re
from functools import partial

from graphene import Int, String
from graphene_django.filter import DjangoFilterConnectionField
from graphene_django.utils import maybe_queryset
from django.core.paginator import Paginator
from django.db.models.query import QuerySet

from . import PaginationConnection, PageInfoExtra


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


def _store_result_ids_on_context(list_slice, result_list, info):
    """Store result IDs on context for DataLoader batching.
    
    After the queryset is evaluated, extract object IDs and store them
    on the request context. This enables DataLoaders to batch-fetch
    related data in a single query instead of N+1 queries.
    
    The IDs are stored as: info.context._<model_name>_result_ids
    Example: info.context._study_result_ids = [uuid1, uuid2, ...]
    """
    if not info or not result_list:
        return
    
    # Get model name from queryset
    if isinstance(list_slice, QuerySet):
        model_name = list_slice.model.__name__.lower()
    elif result_list and hasattr(result_list[0], '_meta'):
        model_name = result_list[0]._meta.model.__name__.lower()
    else:
        return
    
    # Extract IDs from already-fetched objects (in-memory, no DB query)
    try:
        ids = [obj.id for obj in result_list if hasattr(obj, 'id')]
        if ids:
            setattr(info.context, f"_{model_name}_result_ids", ids)
    except (AttributeError, TypeError):
        pass  # Silently fail if objects don't have IDs


def connection_from_list_slice(
    list_slice,
    args=None,
    connection_type=None,
    pageinfo_type=None,
    info=None,
):
    args = args or {}
    limit = args.get("limit", None)
    offset = args.get("offset", 0)

    if limit is None:
        # Evaluate queryset and store IDs for DataLoaders
        result_list = list(list_slice) if isinstance(list_slice, QuerySet) else list_slice
        _store_result_ids_on_context(list_slice, result_list, info)
        
        return connection_type(
            results=result_list,
            page_info=pageinfo_type(
                has_previous_page=False,
                has_next_page=False
            )
        )
    else:
        assert isinstance(limit, int), "Limit must be of type int"
        assert limit > 0, "Limit must be positive integer greater than 0"

        # Fetch the requested slice
        _slice = list_slice[offset:(offset+limit)]
        _slice_list = list(_slice)
        actual_count = len(_slice_list)
        
        # Store IDs for DataLoaders (in-memory extraction, no extra query)
        _store_result_ids_on_context(list_slice, _slice_list, info)

        # Optimization: skip COUNT query when we can determine we're on the last page:
        # 1. offset=0 and got 0 items → empty dataset, total=0
        # 2. got at least 1 item but < limit → last page, total=offset+actual_count
        if (actual_count == 0 and offset == 0) or (0 < actual_count < limit):
            total_count = offset + actual_count
            has_next_page = False
            has_previous_page = offset > 0

            info.context._CachedDjangoPaginationField = total_count

            return connection_type(
                results=_slice_list,
                page_info=pageinfo_type(
                    has_previous_page=has_previous_page,
                    has_next_page=has_next_page
                )
            )
        else:
            # We got exactly 'limit' items, so we need to use the paginator
            # to determine if there are more pages (requires COUNT query)
            paginator = Paginator(list_slice, limit)
            total_count = paginator.count

            # Calculate has_previous/has_next based on offset, not page numbers
            # since offsets don't necessarily align with page boundaries
            has_previous_page = offset > 0
            has_next_page = (offset + limit) < total_count

            info.context._CachedDjangoPaginationField = total_count

            return connection_type(
                results=_slice_list,
                page_info=pageinfo_type(
                    has_previous_page=has_previous_page,
                    has_next_page=has_next_page
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

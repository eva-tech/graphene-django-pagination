import re
from functools import partial

from django.core.paginator import Paginator
from graphene import Int, String
from graphene_django.filter import DjangoFilterConnectionField
from graphene_django.settings import graphene_settings
from graphene_django.utils import maybe_queryset

from . import PageInfoExtra, PaginationConnection


class DjangoPaginationConnectionField(DjangoFilterConnectionField):
    def __init__(
        self,
        type,
        fields=None,
        order_by=None,
        extra_filter_meta=None,
        filterset_class=None,
        hard_limit=None,
        *args,
        **kwargs,
    ):
        self._type = type
        self._fields = fields
        self._provided_filterset_class = filterset_class
        self._filterset_class = None
        self._extra_filter_meta = extra_filter_meta
        self._base_args = None
        self._hard_limit = hard_limit or graphene_settings.RELAY_CONNECTION_MAX_LIMIT

        kwargs.setdefault("limit", Int(description="Query limit"))
        kwargs.setdefault("offset", Int(description="Query offset"))
        kwargs.setdefault("ordering", String(description="Query order"))

        super(DjangoPaginationConnectionField, self).__init__(type, *args, **kwargs)

    @property
    def type(self):
        class NodeConnection(PaginationConnection):
            total_count = Int()

            class Meta:
                node = self._type
                name = "{}NodeConnection".format(self._type._meta.name)

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
            max_limit=max_limit,
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
    list_slice,
    args=None,
    connection_type=None,
    pageinfo_type=None,
    info=None,
    max_limit=None,
):
    args = args or {}
    limit = args.get("limit", None)
    offset = args.get("offset", 0)

    # Enforce max_limit if set
    if max_limit is not None:
        if limit is None:
            limit = max_limit
        else:
            limit = min(limit, max_limit)

    if limit is None:
        return connection_type(
            results=list_slice,
            page_info=pageinfo_type(has_previous_page=False, has_next_page=False),
        )
    else:
        assert isinstance(limit, int), "Limit must be of type int"
        assert limit > 0, "Limit must be positive integer greater than 0"

        # Fetch the requested slice
        _slice = list_slice[offset : (offset + limit)]
        _slice_list = list(_slice)
        actual_count = len(_slice_list)

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
                    has_previous_page=has_previous_page, has_next_page=has_next_page
                ),
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
                    has_previous_page=has_previous_page, has_next_page=has_next_page
                ),
            )


def connection_from_list_ordering(items_list, ordering, connection):
    field, order = ordering.replace(" ", "").split(",")
    field = re.sub(r"(?<!^)(?=[A-Z])", "_", field).lower()
    order = "-" if order == "desc" else ""

    if (
        connection
        and connection._meta
        and connection._meta.node
        and hasattr(connection._meta.node, "ordering")
        and callable(getattr(connection._meta.node, "ordering"))
    ):
        return connection._meta.node.ordering(items_list, field, order)
    else:
        return items_list.order_by(f"{order}{field}")

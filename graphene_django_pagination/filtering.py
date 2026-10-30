"""Filter arguments built the graphene-django 2.15 way.

graphene-django 3 types filter arguments from the model field: UUID foreign keys
become ``UUID`` instead of ``ID``, and declared ``in``/``range`` filters lose their
list type, and form ``DecimalField`` filters become ``Decimal`` instead of
``Float``. graphene-django 3 also stopped turning declared ``in``/``range``
filters into plain filters, so they got a list where they expect CSV text. Clients generate their types from the schema, so this keeps the 2.15
arguments. Copied from graphene-django 2.15.0 ``filter/utils.py`` with ``six``
removed.
"""

from django import forms
from django_filters.filters import BaseCSVFilter, Filter
from django_filters.utils import get_model_field
from graphene import Float, List
from graphene_django.forms.converter import convert_form_field as convert_v3_form_field


def convert_form_field(form_field):
    """Convert like graphene-django 2.15, where decimals were floats."""
    if isinstance(form_field, forms.DecimalField):
        return Float(description=form_field.help_text, required=form_field.required)
    return convert_v3_form_field(form_field)


def get_filtering_args_from_filterset(filterset_class, type):
    """Inspect a FilterSet and produce the arguments to pass to a Graphene Field."""
    args = {}
    model = filterset_class._meta.model
    for name, filter_field in filterset_class.base_filters.items():
        form_field = None
        filter_type = filter_field.lookup_expr

        if name in filterset_class.declared_filters:
            # Get the filter field from the explicitly declared filter
            form_field = filter_field.field
            field = convert_form_field(form_field)
        else:
            # Get the filter field with no explicit type declaration
            model_field = get_model_field(model, filter_field.field_name)
            if filter_type != "isnull" and hasattr(model_field, "formfield"):
                form_field = model_field.formfield(
                    required=filter_field.extra.get("required", False)
                )

            # Fallback to field defined on filter if we can't get it from the
            # model field
            if not form_field:
                form_field = filter_field.field

            field = convert_form_field(form_field)

        if filter_type in ["in", "range"]:
            # Replace CSV filters (`in`, `range`) argument type to be a list of
            # the same type as the field.
            field = List(field.get_type())

        field_type = field.Argument()
        field_type.description = filter_field.label
        args[name] = field_type

    return args


def replace_declared_csv_filters(filterset_class):
    """Turn declared ``in``/``range`` CSV filters into plain filters, like 2.15.

    Their arguments are lists, so the filter receives the list as is instead of
    splitting a comma separated string. graphene-django 3 only does this for the
    filters it generates.
    """
    for name, filter_field in list(filterset_class.base_filters.items()):
        if name not in filterset_class.declared_filters:
            continue
        if filter_field.lookup_expr not in ["in", "range"]:
            continue
        if not isinstance(filter_field, BaseCSVFilter):
            continue
        filterset_class.base_filters[name] = Filter(
            field_name=filter_field.field_name,
            lookup_expr=filter_field.lookup_expr,
            label=filter_field.label,
            method=filter_field.method,
            exclude=filter_field.exclude,
            **filter_field.extra,
        )

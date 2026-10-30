import uuid

import django_filters
from graphene import ObjectType, Schema
from graphene_django import DjangoObjectType
from django.db import models
from django.db import connection

from graphene_django_pagination.connection_field import DjangoPaginationConnectionField



class TestItem(models.Model):
    name = models.CharField(max_length=100)
    value = models.IntegerField()
    
    class Meta:
        app_label = 'test_app'


class TestOwner(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(max_length=100)

    class Meta:
        app_label = 'test_app'


class TestProduct(models.Model):
    owner = models.ForeignKey(TestOwner, on_delete=models.CASCADE)
    name = models.CharField(max_length=100)
    value = models.IntegerField()
    price = models.DecimalField(max_digits=8, decimal_places=2)

    class Meta:
        app_label = 'test_app'

# Create the table (simple approach)
with connection.schema_editor() as schema_editor:
    for model in (TestItem, TestOwner, TestProduct):
        try:
            schema_editor.create_model(model)
        except:
            pass  # Table might already exist


class TestItemType(DjangoObjectType):
    class Meta:
        model = TestItem
        fields = ('id', 'name', 'value')
        filter_fields = {
            'name': ['exact', 'icontains'],
            'value': ['exact', 'gte', 'lte'],
        }


class TestItemLimitedType(DjangoObjectType):
    class Meta:
        model = TestItem
        name = 'TestItemLimited'
        fields = ('id', 'name', 'value')
        filter_fields = {
            'name': ['exact', 'icontains'],
            'value': ['exact', 'gte', 'lte'],
        }


class NumberRangeFilter(django_filters.BaseRangeFilter, django_filters.NumberFilter):
    pass


class TestProductFilterSet(django_filters.FilterSet):
    """Declared filters shaped like the ones evapacs-back writes."""

    value_in = django_filters.BaseInFilter(field_name="value", lookup_expr="in")
    value_range = NumberRangeFilter(field_name="value", lookup_expr="range")
    name_in = django_filters.BaseInFilter(method="filter_name_in")
    price_max = django_filters.NumberFilter(field_name="price", lookup_expr="lte")

    class Meta:
        model = TestProduct
        fields = {
            'owner': ['exact'],
            'price': ['exact'],
        }

    def filter_name_in(self, queryset, name, value):
        return queryset.filter(name__in=value)


class TestOwnerType(DjangoObjectType):
    class Meta:
        model = TestOwner
        fields = ('id', 'name')


class TestProductType(DjangoObjectType):
    class Meta:
        model = TestProduct
        fields = ('id', 'name', 'value', 'price')
        filterset_class = TestProductFilterSet


class Query(ObjectType):
    items = DjangoPaginationConnectionField(TestItemType)
    items_limited = DjangoPaginationConnectionField(TestItemLimitedType, max_limit=3)
    products = DjangoPaginationConnectionField(TestProductType)
    
    def resolve_items(self, info, **kwargs):
        return TestItem.objects.all()

    def resolve_items_limited(self, info, **kwargs):
        return TestItem.objects.all()

    def resolve_products(self, info, **kwargs):
        return TestProduct.objects.all()


schema = Schema(query=Query)

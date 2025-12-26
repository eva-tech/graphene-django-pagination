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

# Create the table (simple approach)
with connection.schema_editor() as schema_editor:
    try:
        schema_editor.create_model(TestItem)
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


class Query(ObjectType):
    items = DjangoPaginationConnectionField(TestItemType)
    items_limited = DjangoPaginationConnectionField(TestItemLimitedType, max_limit=3)
    
    def resolve_items(self, info, **kwargs):
        return TestItem.objects.all()

    def resolve_items_limited(self, info, **kwargs):
        return TestItem.objects.all()


schema = Schema(query=Query)

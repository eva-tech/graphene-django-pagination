# Changelog

## Unreleased

- The minimum django-filter version is 2.3.0, the first one that imports on Django 3.2 and later.
- Tests pin the filter argument types and run on the release B versions.

## 2.0.0

- Supports graphene 3 and graphene-django 3 only (graphene-django >=3.2.3,<4). Projects on graphene 2 stay on 1.1.5.
- Filter arguments keep the graphene-django 2.15 types, so the GraphQL schema does not change: a foreign key filter is `ID`, declared `in`/`range` filters are lists, and `DecimalField` filters are `Float`.

"""Route groups mounted by ``build_app``.

One module per cohesive group of endpoints. Each exposes
``build_router(...) -> APIRouter``, taking the ``ApiContext`` unless the group
needs less (``meta`` takes only the static directory). Registration order
inside a module is preserved by FastAPI, which matters where a literal path has
to be declared before a parameterised one.
"""

"""Route groups mounted by ``build_app``.

One module per cohesive group of endpoints. Each exposes
``build_router(ctx: ApiContext) -> APIRouter``; registration order inside a
module is preserved by FastAPI, which matters where a literal path has to be
declared before a parameterised one.
"""

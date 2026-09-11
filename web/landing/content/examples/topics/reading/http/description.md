Extract data from a paginated HTTP API. The paginator walks `?per_page=2&page=1,2,3...` and stops once the `items` path of a response comes back empty, so the whole collection arrives as a single DataFrame.

Responses come from a `MockHttpClient` because the playground runs PHP compiled to WebAssembly, which has no network access. Swap it for any PSR-18 client and the rest of the pipeline stays the same.

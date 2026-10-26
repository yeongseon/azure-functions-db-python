# Binding wrapper simplification

All five public APIs are retained; they address distinct workflows:

| API | Call flow (before and after) | Ownership |
| --- | --- | --- |
| `input` | bind host args → resolve PK/query params → read → close reader → validate model → handler | reader per invocation; default shared provider |
| `output` | bind host args → inject module-scoped `DbOut` → handler calls `.set()` → create/write/close writer | provider managed; no wrapper-owned per-call resource |
| `inject_reader` | acquire reader → bind host args → inject client/proxy → handler → close in finally | private engine without supplied provider |
| `inject_writer` | acquire writer → bind host args → inject client/proxy → handler → close in finally | private engine without supplied provider |
| `trigger` | bind timer/host args → poll/lease/fetch/normalize → handler → checkpoint/release | pseudo trigger over a real timer; sync only |

## Removed duplication

Previously `_wrap_handler` had four complete branches: managed/unmanaged ×
sync/async. Each repeated host binding, injection argument construction and
signature/metadata finalization. The shared `injected_call` resource scope now
owns acquisition, binding, conversion and cleanup; only the final sync versus
awaited handler call differs. The four branches become two invocation wrappers.

The resource scope preserves a subtle existing distinction: an unmanaged
wrapper binds before acquisition, while a managed wrapper acquires first and
releases even if host binding or argument conversion fails. Exceptions propagate;
cleanup is not suppressed. Tracebacks may include the private context-manager
frame; exception types and handler results are unchanged.

No new injection framework, base class or decorator variant is introduced.
Input remains separate because it resolves user callables before threaded I/O
and closes the reader before handing data to the handler. Trigger remains
separate because it controls acknowledgment, not ordinary resource injection.
Async transaction proxies still pin transaction work to one thread; this change
does not move acquisition/cleanup onto different threads or alter their code.

## Characterization and verification

`test_wrapper_lifecycle_characterization.py` was run before the refactor: 16
combinations lock sync/async, managed/unmanaged, binding/conversion/handler
failure and success, including hidden injected parameter names. The full suite
also retains decorator composition/order and host signature tests, real SQLite
binding tests, and async transaction commit/rollback/thread-affinity tests.

Before: `1026 passed, 87 skipped, 11 deselected, 23 warnings in 48.05s`.
After: `1026 passed, 87 skipped, 11 deselected, 23 warnings in 42.25s`.
These timings are run evidence, not a performance claim. `make check-all` and
`make build` passed on Python 3.11. Remote DB/Azurite cases skipped locally.

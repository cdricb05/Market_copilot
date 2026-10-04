# Wireframe — "Create Paper Orders" fails closed (R100 repair, defect D31)

Scope: one control, `#pd-act-generate`, in the Paper Desk action row. The layout does not change, and no control is added.

## States (1920×1080, Paper Desk action row)

```
┌ Paper Desk ─────────────────────────────── [LIVE] [NO ORDERS] [MANUAL REVIEW] ┐
│ NEXT MANUAL ACTION  <backend next_required_action>                              │
│ [Refresh] [Preview] [Submit (disabled unless open orders)] [Cancel ...]         │
│   Create Paper Orders: NOT RENDERED in every state below                        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

| `/v1/paper-desk/status` | `/v1/alpha-book/status` | `#pd-act-generate` before | after |
|---|---|---|---|
| unavailable | any | hidden, disabled (R84) | hidden, disabled (unchanged) |
| loaded | **unavailable (null)** | **VISIBLE + ENABLED** (fail-open) | **hidden + disabled**, title says the status is unavailable |
| loaded | loaded, `bootstrap_order_path_open === false` | hidden, disabled | unchanged |
| loaded | loaded, path open | hidden (the alpha workflow governs) | hidden, disabled unless the backend explicitly says the path is open |

## Acceptance criteria

1. The markup default stays `disabled` and `style="display:none"` (the R84 test pins this).
2. If `/v1/alpha-book/status` returns non-OK, or the request times out, the control is hidden **and** disabled. An HTTP error that has a JSON body reaches the renderer as an object (`_mhzGet`), so "loaded" means the backend's boolean `bootstrap_order_path_open` is present. This was found in the first Playwright run.
3. The control is enabled only when the alpha-book status loaded and `bootstrap_order_path_open === true`, and even then it stays hidden while the alpha workflow governs. No state renders an enabled Create Orders control.
4. No `alert()` or `confirm()` is added. Safety badges are unchanged.
5. Playwright check at 1920×1080 with `/v1/alpha-book/status` forced to fail: `#pd-act-generate` has `display: none` and `disabled = true`.

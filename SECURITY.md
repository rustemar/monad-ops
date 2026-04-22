# Security Policy

## Reporting a vulnerability

If you find a security issue in `monad-ops`, please email
**security@rustemar.dev**.

Include:
- A clear description of the issue and what it lets an attacker do.
- Steps to reproduce (or a PoC).
- The commit hash / deployment URL you tested against.

Please do **not** open a public GitHub issue for security reports.

## Scope

In scope:
- The code in this repository.
- The deployed instance at `https://ops.rustemar.dev`.

Out of scope:
- Third-party services (Cloudflare, GitHub, Monad RPC endpoints).
- Denial-of-service via simple volume (the service is behind Cloudflare
  rate-limiting).
- Missing security headers already documented as edge-only
  (e.g. CSP on Cloudflare's 429/TRACE default responses).

## Response

- Acknowledgement within 72 hours.
- Initial assessment within 7 days.
- Fixes go out as normal commits to `main`. For issues that affect
  the public dashboard, the fix is deployed to `ops.rustemar.dev`
  before the commit is made public.

## Preferred languages

English or Russian.

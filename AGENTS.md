# PMKit Agent Guidance

## Effect Library Reference

The Effect library source is available locally at `./.repos/effect` as a read-only reference checkout. This directory is ignored by git and resolves to an external checkout at `/Users/yovanoc/.local/share/opencode/repos/github.com/Effect-TS/effect`. It is never committed to the repository.

### Using Effect Source

- **Read-only reference**: `./.repos/effect` is a read-only local checkout. Never edit or commit changes to files under `.repos/effect/`.
- **Canonical documentation**: When working with Effect, prefer `./.repos/effect/LLMS.md` and source code under `./.repos/effect/packages/` as the authoritative reference.
- **No imports from vendored source**: Do not import from `./.repos/effect` in application code. The local checkout is for reference and research only.
- **Package dependencies**: Effect packages are installed via npm/package.json as normal dependencies. Use those installed packages in application code, not the local checkout.

### Effect Patterns

When implementing Effect code in PMKit:

1. Consult `./.repos/effect/LLMS.md` for canonical Effect patterns and best practices.
2. Review `./.repos/effect/packages/effect/src/` for implementation examples and API signatures.
3. Follow the Effect skill guidance in the agent configuration for services, layers, error handling, and testing.

### Local Checkout Setup

The `.repos/effect` directory is a symlink to an external checkout and is ignored by git. To set it up locally:

```bash
ln -s /Users/yovanoc/.local/share/opencode/repos/github.com/Effect-TS/effect .repos/effect
```

The external checkout is maintained independently and never committed to this repository.

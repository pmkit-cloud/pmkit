# PMKit Agent Guidance

## Effect Library Reference

The Effect library source is vendored at `./.repos/effect` as a squashed git subtree from https://github.com/Effect-TS/effect.git (main branch).

### Using Effect Source

- **Read-only reference**: `./.repos/effect` is a read-only vendored copy. Never edit or commit changes to files under `.repos/effect/`.
- **Canonical documentation**: When working with Effect, prefer `./.repos/effect/LLMS.md` and source code under `./.repos/effect/packages/` as the authoritative reference.
- **No imports from vendored source**: Do not import from `./.repos/effect` in application code. The vendored source is for reference and research only.
- **Package dependencies**: Effect packages are installed via npm/package.json as normal dependencies. Use those installed packages in application code, not the vendored source.

### Effect Patterns

When implementing Effect code in PMKit:

1. Consult `./.repos/effect/LLMS.md` for canonical Effect patterns and best practices.
2. Review `./.repos/effect/packages/effect/src/` for implementation examples and API signatures.
3. Follow the Effect skill guidance in the agent configuration for services, layers, error handling, and testing.

### Subtree Maintenance

The subtree was added with:
```bash
git subtree add --prefix=.repos/effect https://github.com/Effect-TS/effect.git main --squash
```

To update the Effect source in the future:
```bash
git subtree pull --prefix=.repos/effect https://github.com/Effect-TS/effect.git main --squash
```

Do not push changes to the subtree. If updates are needed, coordinate with the Effect team upstream.

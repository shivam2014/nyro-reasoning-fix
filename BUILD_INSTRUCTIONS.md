# Nyro Build Instructions

## Quick Build

```bash
cd /Users/shivam94/nyro-mod/nyro-src
make build
```

This will:
1. Build the webui (`webui/dist/`)
2. Build the Tauri desktop app
3. Bundle `Nyro.app` to `target/release/bundle/macos/Nyro.app`

## Install to Applications

```bash
rm -rf /Applications/Nyro.app
cp -R target/release/bundle/macos/Nyro.app /Applications/
```

## Important: Always use `make build`, not `cargo build`

**The white screen issue is caused by using `cargo build --release` directly.**

### Why `make build` is required

The Tauri app embeds the webui (frontend) at compile time using `rust-embed`. The `make build` target runs `webui-build` first, which builds the frontend with `pnpm build`. This ensures the `webui/dist/` folder has the latest assets before the Rust binary is compiled.

If you run `cargo build --release` directly:
- The webui may not be built (or may be stale)
- The binary embeds old/missing frontend assets
- The Tauri webview shows a white screen because it can't load the frontend

### The correct build flow

```
make build
  └── webui-build (pnpm build)
  └── cargo tauri build (embeds webui/dist/ into binary)
  └── bundle (creates Nyro.app)
```

### If you already ran `cargo build`

Always run `make build` instead, or manually build the webui first:

```bash
cd webui && pnpm build && cd ..
cargo build --release
```

## Debugging White Screen

If the Nyro app shows a white screen after building:

1. **Check the webui was built:**
   ```bash
   ls webui/dist/index.html
   ls webui/dist/assets/
   ```

2. **Rebuild with `make build`:**
   ```bash
   make build
   ```

3. **Reinstall:**
   ```bash
   rm -rf /Applications/Nyro.app
   cp -R target/release/bundle/macos/Nyro.app /Applications/
   ```

4. **Relaunch:**
   ```bash
   kill $(pgrep -a nyro 2>/dev/null | awk '{print $1}') 2>/dev/null
   open /Applications/Nyro.app
   ```

## Code Changes

When making code changes:

1. **Rust changes only:** `make build` (webui is cached)
2. **Webui changes:** `make build` (rebuilds webui + binary)
3. **Both:** `make build` (rebuilds everything)

## Troubleshooting

### Proxy not running after launch

Check if the proxy is listening:
```bash
curl http://127.0.0.1:19530/v1/models
```

### Models not showing correct context windows

The `nyro-models.json` file is generated when you click "Sync" in the Nyro GUI. After code changes that affect model capabilities, click Sync to regenerate it.

### App crashes on launch

Check the Tauri logs:
```bash
find ~/Library/Logs -name "*nyro*" -o -name "*Nyro*" 2>/dev/null
```

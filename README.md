# Walmart Order Dashboard

A desktop app that syncs Walmart order emails from Gmail, parses them, and displays everything in a local dashboard. Built with Tauri (Rust backend + TypeScript frontend).

## What it does

- Connects to Gmail accounts via OAuth2 and pulls Walmart order/shipping/delivery emails
- Parses order details, line items, prices, and tracking numbers from email HTML
- Tracks shipments via 17track API
- Stores everything locally in SQLite
- Provides a dashboard UI with order browsing, search, filtering, analytics, and multi-account support

## Prerequisites

- Rust toolchain
- Node.js
- A Google Cloud project with Gmail API enabled and an OAuth2 `client_secret.json`

## Setup

```
npm install
```

Place your `client_secret.json` in the project root.

## Usage

**Desktop app (Tauri):**
```
npm run tauri:dev
```

**CLI:**
```
cargo run --bin walmart-cli -- sync
cargo run --bin walmart-cli -- process
cargo run --bin walmart-cli -- track
```

## Project structure

```
src/           Rust library (parsing, DB, auth, tracking, ingestion)
src-cli/       CLI binary
src-tauri/     Tauri app backend
frontend/      TypeScript UI (Vite + Tailwind)
migrations/    SQLite schema migrations
```

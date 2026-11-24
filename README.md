# 📝 0x00 Community Blogs

English | [简体中文](README.zh-CN.md)

Blog posts of the 0x00 Community, written in [MDX](https://mdxjs.com/).

## 📁 Layout

| Path | What lives there |
| --- | --- |
| `blogs/<slug>/index.<lang>.mdx` | One post per folder, one file per language (`index.en.mdx`, `index.zh-CN.mdx`); `<slug>` is kebab-case (`a-z`, `0-9`, `-`) |
| `blogs/<slug>/assets/` | Images / videos used by that post, referenced with relative paths |
| `blogs/<slug>/refs/` | Source files embedded in the post with Obsidian syntax: `![[refs/relay.rs]]` |
| `authors/<id>.yml` | One file per author: `name`, `email`, `github`, `bio` |
| `check/` | Check script run by CI on every pull request |

## 👤 Authors

Before your first post, add `authors/<id>.yml` (see [`authors/jabberwocky238.yml`](authors/jabberwocky238.yml)):

```yaml
name: Your Name
email: you@example.com     # or a list of addresses
github: https://github.com/your-github-id
bio: |
  A few words about yourself.
```

## ✍️ Writing a post

Create `blogs/my-post/index.en.mdx` (and `index.zh-CN.mdx` for a Chinese version):

```mdx
---
title: My post
date: 2026-11-24
authors: [your-author-id]  # must match authors/<id>.yml
tags: [rust, gc]          # optional
summary: One sentence shown in listings.
lang: en                  # en | zh-CN
status: draft             # draft | staged | release | archive
---

Markdown (with GFM tables, task lists…) plus JSX when you need it.
```

## ✅ Checking locally

```sh
cd check
bun install
bun run check
```

The check validates author files, that every post author exists, folder names, required frontmatter fields (including `status`), that post files are named `index.<lang>.mdx` with a matching `lang`,
that relative links, images and `![[refs/...]]` embeds exist inside the post folder, that every file in `refs/` is embedded, and that the MDX compiles.

## 🤝 Contributing

`main` is protected — open a pull request; it can be merged once the `check` workflow passes.

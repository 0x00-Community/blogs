# 📝 0x00 Community Blogs

English | [简体中文](README.zh-CN.md)

Blog posts of the 0x00 Community, written in [MDX](https://mdxjs.com/).

## 📁 Layout

| Path | What lives there |
| --- | --- |
| `blogs/<slug>/index.mdx` | One post per folder; `<slug>` is kebab-case (`a-z`, `0-9`, `-`) |
| `blogs/<slug>/assets/` | Images / videos used by that post, referenced with relative paths |
| `check/` | Check script run by CI on every pull request |

## ✍️ Writing a post

Create `blogs/my-post/index.mdx`:

```mdx
---
title: My post
date: 2026-11-24
authors: [your-github-id]
tags: [rust, gc]          # optional
summary: One sentence shown in listings.
lang: en                  # en | zh-CN
draft: false              # optional
---

Markdown (with GFM tables, task lists…) plus JSX when you need it.
```

## ✅ Checking locally

```sh
cd check
bun install
bun run check
```

The check validates folder names, required frontmatter fields, that every post is `.mdx`,
that relative links and images exist inside the post folder, and that the MDX compiles.

## 🤝 Contributing

`main` is protected — open a pull request; it can be merged once the `check` workflow passes.

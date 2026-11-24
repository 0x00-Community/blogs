# 📝 0x00 社区博客

[English](README.md) | 简体中文

0x00 社区的博客文章，统一使用 [MDX](https://mdxjs.com/) 编写。

## 📁 目录结构

| 路径 | 内容 |
| --- | --- |
| `blogs/<slug>/index.<lang>.mdx` | 一篇文章一个文件夹，每种语言一个文件（`index.zh-CN.mdx`、`index.en.mdx`）；`<slug>` 用短横线小写（`a-z`、`0-9`、`-`） |
| `blogs/<slug>/assets/` | 该文章用到的图片 / 视频，用相对路径引用 |
| `blogs/<slug>/refs/` | 文章引用的源码文件，用 Obsidian 语法嵌入：`![[refs/relay.rs]]` |
| `authors/<id>.yml` | 一位作者一个文件：`name`、`email`、`github`、`bio` |
| `check/` | 检查脚本，合并前在本地运行 |

## 👤 作者

第一次投稿前先新建 `authors/<id>.yml`（样例见 [`authors/jabberwocky238.yml`](authors/jabberwocky238.yml)）：

```yaml
name: 你的名字
email: you@example.com     # 多个邮箱写成列表
github: https://github.com/你的-github-id
bio: |
  一段自我介绍。
```

## ✍️ 写一篇文章

新建 `blogs/my-post/index.zh-CN.mdx`（英文版另建 `index.en.mdx`）：

```mdx
---
title: 文章标题
date: 2026-11-24
authors: [你的作者-id]    # 对应 authors/<id>.yml
tags: [rust, gc]          # 可选
summary: 列表里显示的一句话简介。
lang: zh-CN               # en | zh-CN
status: draft             # draft | staged | release | archive
---

正文是 Markdown（支持 GFM 表格、任务列表等），需要时可以写 JSX。
```

## ✅ 本地检查

```sh
cd check
bun install
bun run check
```

检查内容：作者文件格式、文章作者必须在 authors/ 中存在、文件夹命名、frontmatter 必填字段（含 `status`）、文章文件名必须是 `index.<lang>.mdx` 且与 `lang` 一致、相对链接、图片和 `![[refs/...]]` 嵌入都在文章文件夹内且存在、`refs/` 里每个文件都被引用、MDX 能正常编译。

## 🤝 参与贡献

`main` 分支受保护，请提交 PR；合并前在本地运行检查。

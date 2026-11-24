import { compile } from "@mdx-js/mdx";
import remarkFrontmatter from "remark-frontmatter";
import remarkGfm from "remark-gfm";
import { parse as parseYaml } from "yaml";
import { readdir, readFile, stat } from "node:fs/promises";
import { join, relative, resolve, dirname } from "node:path";

const ROOT = resolve(import.meta.dir, "..");
const BLOGS = join(ROOT, "blogs");
const AUTHORS = join(ROOT, "authors");
const AUTHOR_FIELDS = ["name", "email", "github", "bio"] as const;
const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const GITHUB = /^https:\/\/github\.com\/[A-Za-z0-9-]+\/?$/;
const authorIds = new Set<string>();
const ENTRY = "index.mdx";
const SLUG = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const LANGS = ["en", "zh-CN"] as const;
const ASSETS = /\.(png|jpe?g|gif|webp|avif|svg|mp4|webm)$/i;
const REQUIRED = ["title", "date", "authors", "summary", "lang"] as const;

type Problem = { file: string; message: string };
const problems: Problem[] = [];
const report = (file: string, message: string) =>
  problems.push({ file: relative(ROOT, file), message });

const isNonEmptyString = (v: unknown): v is string =>
  typeof v === "string" && v.trim().length > 0;

function checkFrontmatter(file: string, source: string) {
  const match = source.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n/);
  if (!match) return report(file, "missing YAML frontmatter at top of file");
  let fm: Record<string, unknown>;
  try {
    fm = parseYaml(match[1]) ?? {};
  } catch (e) {
    return report(file, `invalid YAML frontmatter: ${(e as Error).message}`);
  }
  for (const key of REQUIRED) if (!(key in fm)) report(file, `frontmatter: missing "${key}"`);
  if ("title" in fm && !isNonEmptyString(fm.title)) report(file, "frontmatter: title must be a non-empty string");
  if ("summary" in fm && !isNonEmptyString(fm.summary)) report(file, "frontmatter: summary must be a non-empty string");
  if ("date" in fm) {
    const d = String(fm.date instanceof Date ? fm.date.toISOString().slice(0, 10) : fm.date);
    if (!DATE.test(d) || Number.isNaN(Date.parse(d))) report(file, `frontmatter: date must be YYYY-MM-DD, got "${d}"`);
  }
  if ("authors" in fm) {
    if (!(Array.isArray(fm.authors) && fm.authors.length > 0 && fm.authors.every(isNonEmptyString)))
      report(file, "frontmatter: authors must be a non-empty list of author ids");
    else
      for (const a of fm.authors)
        if (!authorIds.has(a)) report(file, `frontmatter: unknown author "${a}" — add authors/${a}.yml`);
  }
  if ("tags" in fm && !(Array.isArray(fm.tags) && fm.tags.every(isNonEmptyString)))
    report(file, "frontmatter: tags must be a list of strings");
  if ("lang" in fm && !LANGS.includes(fm.lang as (typeof LANGS)[number]))
    report(file, `frontmatter: lang must be one of ${LANGS.join(", ")}`);
  if ("draft" in fm && typeof fm.draft !== "boolean") report(file, "frontmatter: draft must be true or false");
}

async function exists(path: string) {
  return stat(path).then(() => true, () => false);
}

async function checkLocalRefs(file: string, source: string) {
  const refs = [
    ...source.matchAll(/!?\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)/g),
    ...source.matchAll(/\b(?:src|href)=["']([^"']+)["']/g),
  ].map((m) => m[1]);
  for (const ref of refs) {
    if (/^(?:[a-z]+:|#|\/\/)/i.test(ref)) continue;
    if (ref.startsWith("/")) {
      report(file, `absolute local path "${ref}" — use a path relative to the post`);
      continue;
    }
    const target = resolve(dirname(file), ref.split(/[?#]/)[0]);
    if (!target.startsWith(dirname(file))) report(file, `"${ref}" points outside the post folder`);
    else if (!(await exists(target))) report(file, `"${ref}" does not exist`);
  }
}

async function checkPost(dir: string) {
  const slug = dir.split("/").pop()!;
  if (!SLUG.test(slug)) report(dir, "folder name must be a kebab-case slug (a-z, 0-9, -)");
  const entries = await readdir(dir, { withFileTypes: true });
  const file = join(dir, ENTRY);
  if (!entries.some((e) => e.isFile() && e.name === ENTRY)) return report(dir, `missing ${ENTRY}`);
  for (const e of entries) {
    if (e.name === ENTRY) continue;
    if (e.isDirectory()) {
      if (e.name !== "assets") report(join(dir, e.name), 'only an "assets" sub-folder is allowed');
      else
        for (const a of await readdir(join(dir, e.name), { recursive: true }))
          if (!ASSETS.test(a) && !(await stat(join(dir, e.name, a))).isDirectory())
            report(join(dir, e.name, a), "unsupported asset type");
    } else if (e.name.endsWith(".md")) report(join(dir, e.name), "posts must be .mdx, not .md");
    else if (!ASSETS.test(e.name)) report(join(dir, e.name), "unexpected file; put images/videos in assets/");
  }
  const source = await readFile(file, "utf8");
  checkFrontmatter(file, source);
  await checkLocalRefs(file, source);
  try {
    await compile({ value: source, path: file }, { remarkPlugins: [remarkFrontmatter, remarkGfm] });
  } catch (e) {
    report(file, `MDX compile error: ${(e as Error).message}`);
  }
}

for (const e of await readdir(AUTHORS, { withFileTypes: true })) {
  const file = join(AUTHORS, e.name);
  const id = e.name.replace(/\.yml$/, "");
  if (!e.isFile() || !e.name.endsWith(".yml") || !SLUG.test(id)) {
    report(file, "authors/ only holds <id>.yml files with a lowercase kebab-case id");
    continue;
  }
  let a: Record<string, unknown>;
  try {
    a = parseYaml(await readFile(file, "utf8")) ?? {};
  } catch (err) {
    report(file, `invalid YAML: ${(err as Error).message}`);
    continue;
  }
  for (const k of AUTHOR_FIELDS) if (!isNonEmptyString(a[k])) report(file, `missing or empty "${k}"`);
  for (const k of Object.keys(a)) if (!(AUTHOR_FIELDS as readonly string[]).includes(k)) report(file, `unknown field "${k}"`);
  if (isNonEmptyString(a.email) && !EMAIL.test(a.email)) report(file, `invalid email "${a.email}"`);
  if (isNonEmptyString(a.github) && !GITHUB.test(a.github)) report(file, "github must be https://github.com/<user>");
  authorIds.add(id);
}

for (const e of await readdir(BLOGS, { withFileTypes: true })) {
  if (e.isDirectory()) await checkPost(join(BLOGS, e.name));
  else if (e.name !== ".gitkeep") report(join(BLOGS, e.name), "each post lives in its own folder: blogs/<slug>/index.mdx");
}

if (problems.length) {
  for (const p of problems) console.error(`✗ ${p.file}: ${p.message}`);
  console.error(`\n${problems.length} problem(s) found.`);
  process.exit(1);
}
console.log("✓ all posts passed");

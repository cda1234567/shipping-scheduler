import { spawnSync } from "node:child_process";
import { mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative, resolve } from "node:path";

async function listJsFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await listJsFiles(fullPath));
    } else if (entry.isFile() && entry.name.endsWith(".js")) {
      files.push(fullPath);
    }
  }
  return files;
}

function runNodeCheck(filePath, label) {
  const result = spawnSync(process.execPath, ["--check", filePath], {
    encoding: "utf8",
  });
  if (result.status !== 0) {
    const output = `${result.stdout || ""}${result.stderr || ""}`.trim();
    return `${label}\n${output}`;
  }
  return null;
}

async function checkInlineModules(rootDir) {
  const htmlPath = resolve(rootDir, "static", "index.html");
  const html = await readFile(htmlPath, "utf8");
  const modules = [...html.matchAll(/<script\s+type="module">([\s\S]*?)<\/script>/g)];
  if (!modules.length) {
    return { checked: 0, failures: [] };
  }

  const tempDir = await mkdtemp(join(tmpdir(), "opentext-js-check-"));
  const failures = [];

  try {
    for (const [index, match] of modules.entries()) {
      const tempFile = join(tempDir, `index-inline-${index + 1}.mjs`);
      await writeFile(tempFile, match[1], "utf8");
      const failure = runNodeCheck(tempFile, `${relative(rootDir, htmlPath)} <script type="module"> #${index + 1}`);
      if (failure) failures.push(failure);
    }
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }

  return { checked: modules.length, failures };
}

async function main() {
  const rootDir = process.cwd();
  const jsFiles = await listJsFiles(resolve(rootDir, "static", "modules"));
  const failures = [];

  for (const filePath of jsFiles) {
    const failure = runNodeCheck(filePath, relative(rootDir, filePath));
    if (failure) failures.push(failure);
  }

  const inlineResult = await checkInlineModules(rootDir);
  failures.push(...inlineResult.failures);

  if (failures.length) {
    console.error(failures.join("\n\n"));
    process.exit(1);
  }

  console.log(`JS syntax OK (${jsFiles.length} files + ${inlineResult.checked} inline modules)`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

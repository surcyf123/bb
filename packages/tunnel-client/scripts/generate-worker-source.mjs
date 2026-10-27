import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

const packageRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const outputPath = path.join(
  packageRoot,
  "src",
  "generated",
  "tunnel-worker-source.generated.ts",
);

const result = await build({
  entryPoints: [path.join(packageRoot, "src", "worker-main.ts")],
  bundle: true,
  format: "cjs",
  platform: "node",
  target: "node22",
  minify: true,
  write: false,
  logLevel: "warning",
  conditions: ["source"],
  external: ["bufferutil", "utf-8-validate"],
});
const source = result.outputFiles[0]?.text;
if (source === undefined) throw new Error("tunnel worker bundle is empty");

const content = `export const TUNNEL_WORKER_SOURCE = ${JSON.stringify(source)};\n`;
const current = await readFile(outputPath, "utf8").catch(() => null);
if (current !== content) {
  await mkdir(path.dirname(outputPath), { recursive: true });
  const temporaryPath = `${outputPath}.${process.pid}.tmp`;
  await writeFile(temporaryPath, content, "utf8");
  await rename(temporaryPath, outputPath);
}

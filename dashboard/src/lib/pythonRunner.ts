import path from "node:path";
import { spawn } from "node:child_process";

function resolvePythonExecutable(): string {
  if (process.env.PYTHON_BIN) {
    return process.env.PYTHON_BIN;
  }

  const venvPython = path.resolve(process.cwd(), "../.venv/bin/python");
  return venvPython;
}

function extractJsonPayload(rawOutput: string): string {
  const trimmed = rawOutput.trim();
  if (!trimmed) {
    throw new Error("Python query returned empty output");
  }

  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    return trimmed;
  }

  const objStart = trimmed.indexOf("{");
  const objEnd = trimmed.lastIndexOf("}");
  if (objStart !== -1 && objEnd > objStart) {
    return trimmed.slice(objStart, objEnd + 1);
  }

  const arrStart = trimmed.indexOf("[");
  const arrEnd = trimmed.lastIndexOf("]");
  if (arrStart !== -1 && arrEnd > arrStart) {
    return trimmed.slice(arrStart, arrEnd + 1);
  }

  throw new Error(`Python query did not return JSON payload: ${trimmed}`);
}

export function runS3Query(args: string[]): Promise<string> {
  const scriptPath = path.resolve(process.cwd(), "scripts/s3_query.py");
  const pythonBin = resolvePythonExecutable();

  return new Promise((resolve, reject) => {
    const child = spawn(pythonBin, [scriptPath, ...args], {
      env: process.env,
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });

    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (code === 0) {
        resolve(extractJsonPayload(stdout));
        return;
      }

      reject(new Error(`Python query failed with code ${code}: ${stderr || stdout}`));
    });
  });
}

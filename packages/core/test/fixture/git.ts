import { execFile, spawn } from "child_process"
import fs from "fs/promises"
import path from "path"
import { promisify } from "util"
import { pathToFileURL } from "url"
import { Repository } from "@opencode-ai/core/repository"

const exec = promisify(execFile)

export async function gitRemote(root: string, input: { feature?: { name: string; content: string } } = {}) {
  const origin = path.join(root, "origin.git")
  const source = path.join(root, "source")
  if (input.feature) {
    await git(root, "init", "--bare", "--initial-branch=main", origin)
    await fastImport(origin, input.feature)
    return fixture(root, source, origin)
  }
  await git(root, "init", "--bare", origin)
  await git(root, "init", source)
  await git(source, "config", "user.email", "test@example.com")
  await git(source, "config", "user.name", "Test")
  await fs.writeFile(path.join(source, "README.md"), "one\n")
  await git(source, "add", "README.md")
  await git(source, "commit", "-m", "initial")
  await git(source, "branch", "-M", "main")
  await git(source, "remote", "add", "origin", pathToFileURL(origin).href)
  await git(source, "push", "-u", "origin", "main")
  await git(root, "--git-dir", origin, "symbolic-ref", "HEAD", "refs/heads/main")
  return fixture(root, source, origin)
}

function fixture(root: string, source: string, origin: string) {
  return {
    root,
    source,
    remote: pathToFileURL(origin).href,
    reference: { ...Repository.parseRemote("owner/repo"), remote: pathToFileURL(origin).href },
  }
}

async function fastImport(origin: string, feature: { name: string; content: string }) {
  if (feature.name.includes("\n")) throw new Error("Fixture branch names cannot contain newlines")
  const data = (value: string) => `data ${Buffer.byteLength(value)}\n${value}\n`
  const identity = "Test <test@example.com> 1700000000 +0000"
  const stream = [
    "blob\nmark :1\n",
    data("one\n"),
    "commit refs/heads/main\nmark :2\n",
    `author ${identity}\ncommitter ${identity}\n`,
    data("initial"),
    "M 100644 :1 README.md\n\n",
    "blob\nmark :3\n",
    data(feature.content),
    `commit refs/heads/${feature.name}\nmark :4\nauthor ${identity}\ncommitter ${identity}\n`,
    data(feature.name),
    "from :2\nM 100644 :3 README.md\n\n",
  ].join("")
  await new Promise<void>((resolve, reject) => {
    const child = spawn("git", ["fast-import", "--quiet"], { cwd: origin, stdio: ["pipe", "ignore", "pipe"] })
    const errors: Buffer[] = []
    child.stderr.on("data", (chunk) => errors.push(chunk))
    child.on("error", reject)
    child.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`git fast-import failed (${code}): ${Buffer.concat(errors)}`)),
    )
    child.stdin.end(stream)
  })
}

export async function commit(source: string, content: string, message: string) {
  await fs.writeFile(path.join(source, "README.md"), content)
  await git(source, "add", "README.md")
  await git(source, "commit", "-m", message)
  await git(source, "push")
}

export async function branch(source: string, name: string, content: string) {
  await git(source, "checkout", "-b", name)
  await fs.writeFile(path.join(source, "README.md"), content)
  await git(source, "add", "README.md")
  await git(source, "commit", "-m", name)
  await git(source, "push", "-u", "origin", name)
}

export async function git(cwd: string, ...args: string[]) {
  await exec("git", args, { cwd })
}

import * as fs from 'node:fs'
import { constants } from 'node:fs'
import { platform as osPlatform } from 'node:os'
import { format, debuglog } from 'node:util'

/** @typedef {import('fs')} FSModule */

const debug = debuglog?.('gfs4') || ((...args) => {
  if (/\bgfs4\b/i.test(process.env.NODE_DEBUG || '')) {
    console.error('GFS4:', format(...args).replace(/\n/g, '\nGFS4: '))
  }
})

/** @type {[Function, Array, Error, number?, number?][]} */
const queue = []

let retryTimer = null

/**
 * Enqueue a failed call and schedule retry.
 * @param {[Function, Array, Error, number?, number?]} entry
 */
const enqueue = (entry) => {
  debug('ENQUEUE', entry[0].name, entry[1])
  queue.push(entry)
  scheduleRetry()
}

/**
 * Schedule a microtask to run the retry loop.
 */
const scheduleRetry = () => {
  if (retryTimer) return
  retryTimer = setTimeout(async () => {
    retryTimer = null
    for await (const _ of retryQueue()) {}
    if (queue.length) scheduleRetry()
  }, 0)
}

/**
 * Reset all retry timestamps.
 */
const resetQueue = () => {
  const now = Date.now()
  for (const job of queue) {
    if (job.length > 2) {
      job[3] = now
      job[4] = now
    }
  }
  scheduleRetry()
}

/**
 * Retry EMFILE/ENFILE calls in a controlled loop.
 * @returns {AsyncGenerator<void>}
 */
async function* retryQueue() {
  while (queue.length) {
    const [fn, args, err, startTime = Date.now(), lastTime = Date.now()] = queue.shift()
    const now = Date.now()
    const sinceAttempt = now - lastTime
    const sinceStart = Math.max(lastTime - startTime, 1)
    const delay = Math.min(sinceStart * 1.2, 100)

    if (now - startTime >= 60_000) {
      debug('TIMEOUT', fn.name, args)
      const cb = args.pop()
      if (typeof cb === 'function') cb.call(null, err)
      continue
    }

    if (sinceAttempt >= delay) {
      debug('RETRY', fn.name, args)
      yield fn(...args, startTime)
    } else {
      queue.push([fn, args, err, startTime, now])
    }
  }
}

/**
 * Wrap an async fs method to support EMFILE/ENFILE retry logic.
 * @template T
 * @param {(cb: (...args: any[]) => void) => void} fn
 * @returns {(...args: any[]) => void}
 */
const wrapWithRetry = (fn) => (...args) => {
  const cb = typeof args.at(-1) === 'function' ? args.pop() : undefined
  const attempt = (startTime = Date.now()) =>
    fn(...args, (err, ...results) => {
      if (err && (err.code === 'EMFILE' || err.code === 'ENFILE')) {
        enqueue([attempt, [...args, cb], err, startTime, Date.now()])
      } else {
        cb?.(err, ...results)
      }
    })
  attempt()
}

/**
 * Patch a provided fs module with retry and polyfill support.
 * @param {FSModule} fs
 * @param {string} [platform]
 */
const patchFs = (fs, platform = osPlatform()) => {
  // --- EMFILE/ENFILE Retry Patch ---
  for (const key of ['open', 'readFile', 'writeFile', 'appendFile', 'readdir']) {
    if (typeof fs[key] === 'function') {
      fs[key] = wrapWithRetry(fs[key].bind(fs))
    }
  }

  // --- Polyfills Section ---

  if ('O_SYMLINK' in constants && /^v0\.6\.[0-2]|^v0\.5\./.test(process.version)) {
    fs.lchmod = (path, mode, cb) => {
      fs.open(path, constants.O_WRONLY | constants.O_SYMLINK, mode, (err, fd) => {
        if (err) return cb?.(err)
        fs.fchmod(fd, mode, (err2) => fs.close(fd, (err3) => cb?.(err2 || err3)))
      })
    }
    fs.lchmodSync = (path, mode) => {
      const fd = fs.openSync(path, constants.O_WRONLY | constants.O_SYMLINK, mode)
      try {
        return fs.fchmodSync(fd, mode)
      } finally {
        fs.closeSync(fd)
      }
    }
  }

  if (!fs.lutimes) {
    if ('O_SYMLINK' in constants && fs.futimes) {
      fs.lutimes = (path, atime, mtime, cb) => {
        fs.open(path, constants.O_SYMLINK, (err, fd) => {
          if (err) return cb?.(err)
          fs.futimes(fd, atime, mtime, (err2) => fs.close(fd, (err3) => cb?.(err2 || err3)))
        })
      }
      fs.lutimesSync = (path, atime, mtime) => {
        const fd = fs.openSync(path, constants.O_SYMLINK)
        try {
          return fs.futimesSync(fd, atime, mtime)
        } finally {
          fs.closeSync(fd)
        }
      }
    } else {
      fs.lutimes = (_a, _b, _c, cb) => cb?.()
      fs.lutimesSync = () => {}
    }
  }

  const isIgnorableChownErr = (err) => {
    if (!err) return true
    if (err.code === 'ENOSYS') return true
    if (!process.getuid || process.getuid() !== 0) {
      return err.code === 'EPERM' || err.code === 'EINVAL'
    }
    return false
  }

  const chownFix = (orig) => orig && function (target, uid, gid, cb) {
    orig.call(this, target, uid, gid, (err, ...rest) => {
      if (isIgnorableChownErr(err)) err = null
      cb?.(err, ...rest)
    })
  }

  const chmodFix = (orig) => orig && function (target, mode, cb) {
    orig.call(this, target, mode, (err, ...rest) => {
      if (isIgnorableChownErr(err)) err = null
      cb?.(err, ...rest)
    })
  }

  const chownFixSync = (orig) => orig && function (target, uid, gid) {
    try {
      return orig.call(this, target, uid, gid)
    } catch (err) {
      if (!isIgnorableChownErr(err)) throw err
    }
  }

  const chmodFixSync = (orig) => orig && function (target, mode) {
    try {
      return orig.call(this, target, mode)
    } catch (err) {
      if (!isIgnorableChownErr(err)) throw err
    }
  }

  const statFix = (orig) => orig && function (target, options, cb) {
    if (typeof options === 'function') {
      cb = options
      options = undefined
    }
    orig.call(this, target, options, (err, stats) => {
      if (stats?.uid < 0) stats.uid += 2 ** 32
      if (stats?.gid < 0) stats.gid += 2 ** 32
      cb?.(err, stats)
    })
  }

  const statFixSync = (orig) => orig && function (target, options) {
    const stats = orig.call(this, target, options)
    if (stats?.uid < 0) stats.uid += 2 ** 32
    if (stats?.gid < 0) stats.gid += 2 ** 32
    return stats
  }

  fs.chown = chownFix(fs.chown)
  fs.fchown = chownFix(fs.fchown)
  fs.lchown = chownFix(fs.lchown)
  fs.chmod = chmodFix(fs.chmod)
  fs.fchmod = chmodFix(fs.fchmod)
  fs.lchmod = chmodFix(fs.lchmod)

  fs.chownSync = chownFixSync(fs.chownSync)
  fs.fchownSync = chownFixSync(fs.fchownSync)
  fs.lchownSync = chownFixSync(fs.lchownSync)
  fs.chmodSync = chmodFixSync(fs.chmodSync)
  fs.fchmodSync = chmodFixSync(fs.fchmodSync)
  fs.lchmodSync = chmodFixSync(fs.lchmodSync)

  fs.stat = statFix(fs.stat)
  fs.fstat = statFix(fs.fstat)
  fs.lstat = statFix(fs.lstat)

  fs.statSync = statFixSync(fs.statSync)
  fs.fstatSync = statFixSync(fs.fstatSync)
  fs.lstatSync = statFixSync(fs.lstatSync)

  if (fs.chmod && !fs.lchmod) {
    fs.lchmod = (_p, _m, cb) => cb?.()
    fs.lchmodSync = () => {}
  }
  if (fs.chown && !fs.lchown) {
    fs.lchown = (_p, _u, _g, cb) => cb?.()
    fs.lchownSync = () => {}
  }

  if (platform === 'win32') {
    const origRename = fs.rename?.bind(fs)
    if (typeof origRename === 'function') {
      fs.rename = (from, to, cb) => {
        const start = Date.now()
        let backoff = 0
        const retry = (err) => {
          if (!err || Date.now() - start >= 60_000) return cb?.(err)
          setTimeout(() => {
            fs.stat(to, (statErr) => {
              if (statErr?.code === 'ENOENT') {
                origRename(from, to, retry)
              } else {
                cb?.(err)
              }
            })
          }, backoff)
          backoff = Math.min(backoff + 10, 100)
        }
        origRename(from, to, retry)
      }
    }
  }

  const origRead = fs.read?.bind(fs)
  if (typeof origRead === 'function') {
    fs.read = (fd, buffer, offset, length, position, cb) => {
      let attempts = 0
      const retryingCb = (err, ...res) => {
        if (err?.code === 'EAGAIN' && attempts++ < 10) {
          return origRead(fd, buffer, offset, length, position, retryingCb)
        }
        cb?.(err, ...res)
      }
      return origRead(fd, buffer, offset, length, position, retryingCb)
    }
  }

  const origReadSync = fs.readSync?.bind(fs)
  if (typeof origReadSync === 'function') {
    fs.readSync = (fd, buffer, offset, length, position) => {
      let attempts = 0
      while (true) {
        try {
          return origReadSync(fd, buffer, offset, length, position)
        } catch (err) {
          if (err.code === 'EAGAIN' && attempts++ < 10) continue
          throw err
        }
      }
    }
  }
}

export default patchFs
export { patchFs, resetQueue }

import fs from 'node:fs'
import path from 'node:path'
import { mkdirsSync } from '../mkdirs.js'
import { utimesMillisSync } from '../util/utimes.js'
import * as stat from '../util/stat.js'

/**
 * @typedef {Object} CopyOptions
 * @property {boolean} [clobber]
 * @property {boolean} [overwrite]
 * @property {boolean} [errorOnExist]
 * @property {boolean} [preserveTimestamps]
 * @property {boolean} [dereference]
 * @property {(src: string, dest: string) => boolean} [filter]
 */

/**
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions | ((src: string, dest: string) => boolean)} [opts]
 */
const copySync = (src, dest, opts) => {
  if (typeof opts === 'function') {
    opts = { filter: opts }
  }

  opts = opts || {}
  opts.clobber = 'clobber' in opts ? !!opts.clobber : true
  opts.overwrite = 'overwrite' in opts ? !!opts.overwrite : opts.clobber

  if (opts.preserveTimestamps && process.arch === 'ia32') {
    process.emitWarning(
      'Using the preserveTimestamps option in 32-bit node is not recommended;\n\n' +
      '\tsee https://github.com/jprichardson/node-fs-extra/issues/269',
      'Warning', 'fs-extra-WARN0002'
    )
  }

  const { srcStat, destStat } = stat.checkPathsSync(src, dest, 'copy', opts)
  stat.checkParentPathsSync(src, srcStat, dest, 'copy')
  if (opts.filter && !opts.filter(src, dest)) return

  const destParent = path.dirname(dest)
  if (!fs.existsSync(destParent)) mkdirsSync(destParent)
  return getStats(destStat, src, dest, opts)
}

/**
 * @param {fs.Stats | undefined} destStat
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const getStats = (destStat, src, dest, opts) => {
  const statSync = opts.dereference ? fs.statSync : fs.lstatSync
  const srcStat = statSync(src)

  if (srcStat.isDirectory()) return onDir(srcStat, destStat, src, dest, opts)
  if (srcStat.isFile() || srcStat.isCharacterDevice() || srcStat.isBlockDevice()) {
    return onFile(srcStat, destStat, src, dest, opts)
  }
  if (srcStat.isSymbolicLink()) return onLink(destStat, src, dest, opts)
  if (srcStat.isSocket()) throw new Error(`Cannot copy a socket file: ${src}`)
  if (srcStat.isFIFO()) throw new Error(`Cannot copy a FIFO pipe: ${src}`)

  throw new Error(`Unknown file: ${src}`)
}

/**
 * @param {fs.Stats} srcStat
 * @param {fs.Stats | undefined} destStat
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const onFile = (srcStat, destStat, src, dest, opts) => {
  if (!destStat) return copyFile(srcStat, src, dest, opts)
  return mayCopyFile(srcStat, src, dest, opts)
}

/**
 * @param {fs.Stats} srcStat
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const mayCopyFile = (srcStat, src, dest, opts) => {
  if (opts.overwrite) {
    fs.unlinkSync(dest)
    return copyFile(srcStat, src, dest, opts)
  } else if (opts.errorOnExist) {
    throw new Error(`'${dest}' already exists`)
  }
}

/**
 * @param {fs.Stats} srcStat
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const copyFile = (srcStat, src, dest, opts) => {
  fs.copyFileSync(src, dest)
  if (opts.preserveTimestamps) handleTimestamps(srcStat.mode, src, dest)
  return setDestMode(dest, srcStat.mode)
}

/**
 * @param {number} srcMode
 * @param {string} src
 * @param {string} dest
 */
const handleTimestamps = (srcMode, src, dest) => {
  if (fileIsNotWritable(srcMode)) makeFileWritable(dest, srcMode)
  return setDestTimestamps(src, dest)
}

/**
 * @param {number} srcMode
 * @returns {boolean}
 */
const fileIsNotWritable = (srcMode) => {
  return (srcMode & 0o200) === 0
}

/**
 * @param {string} dest
 * @param {number} srcMode
 */
const makeFileWritable = (dest, srcMode) => {
  return setDestMode(dest, srcMode | 0o200)
}

/**
 * @param {string} dest
 * @param {number} srcMode
 */
const setDestMode = (dest, srcMode) => {
  return fs.chmodSync(dest, srcMode)
}

/**
 * @param {string} src
 * @param {string} dest
 */
const setDestTimestamps = (src, dest) => {
  const updatedSrcStat = fs.statSync(src)
  return utimesMillisSync(dest, updatedSrcStat.atime, updatedSrcStat.mtime)
}

/**
 * @param {fs.Stats} srcStat
 * @param {fs.Stats | undefined} destStat
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const onDir = (srcStat, destStat, src, dest, opts) => {
  if (!destStat) return mkDirAndCopy(srcStat.mode, src, dest, opts)
  return copyDir(src, dest, opts)
}

/**
 * @param {number} srcMode
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const mkDirAndCopy = (srcMode, src, dest, opts) => {
  fs.mkdirSync(dest)
  copyDir(src, dest, opts)
  return setDestMode(dest, srcMode)
}

/**
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const copyDir = (src, dest, opts) => {
  const dir = fs.opendirSync(src)

  try {
    let dirent
    while ((dirent = dir.readSync()) !== null) {
      copyDirItem(dirent.name, src, dest, opts)
    }
  } finally {
    dir.closeSync()
  }
}

/**
 * @param {string} item
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const copyDirItem = (item, src, dest, opts) => {
  const srcItem = path.join(src, item)
  const destItem = path.join(dest, item)
  if (opts.filter && !opts.filter(srcItem, destItem)) return
  const { destStat } = stat.checkPathsSync(srcItem, destItem, 'copy', opts)
  return getStats(destStat, srcItem, destItem, opts)
}

/**
 * @param {fs.Stats | undefined} destStat
 * @param {string} src
 * @param {string} dest
 * @param {CopyOptions} opts
 */
const onLink = (destStat, src, dest, opts) => {
  let resolvedSrc = fs.readlinkSync(src)
  if (opts.dereference) {
    resolvedSrc = path.resolve(process.cwd(), resolvedSrc)
  }

  if (!destStat) {
    return fs.symlinkSync(resolvedSrc, dest)
  } else {
    let resolvedDest
    try {
      resolvedDest = fs.readlinkSync(dest)
    } catch (err) {
      if (err.code === 'EINVAL' || err.code === 'UNKNOWN') {
        return fs.symlinkSync(resolvedSrc, dest)
      }
      throw err
    }

    if (opts.dereference) {
      resolvedDest = path.resolve(process.cwd(), resolvedDest)
    }

    if (stat.isSrcSubdir(resolvedSrc, resolvedDest)) {
      throw new Error(`Cannot copy '${resolvedSrc}' to a subdirectory of itself, '${resolvedDest}'.`)
    }

    if (stat.isSrcSubdir(resolvedDest, resolvedSrc)) {
      throw new Error(`Cannot overwrite '${resolvedDest}' with '${resolvedSrc}'.`)
    }

    return copyLink(resolvedSrc, dest)
  }
}

/**
 * @param {string} resolvedSrc
 * @param {string} dest
 */
const copyLink = (resolvedSrc, dest) => {
  fs.unlinkSync(dest)
  return fs.symlinkSync(resolvedSrc, dest)
}

export {
  copySync,
  getStats,
  onFile,
  mayCopyFile,
  copyFile,
  handleTimestamps,
  fileIsNotWritable,
  makeFileWritable,
  setDestMode,
  setDestTimestamps,
  onDir,
  mkDirAndCopy,
  copyDir,
  copyDirItem,
  onLink,
  copyLink
}

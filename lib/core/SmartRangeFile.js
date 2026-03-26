/**
 * SmartRangeFile - 智能 Range 请求文件类
 *
 * 功能：
 * 1. HTTP Range 请求分块获取，提供 slice(start, end) → { arrayBuffer() }
 * 2. 小文件（≤ threshold）直接整体获取，避免分段开销
 * 3. 批量防抖：多个 slice() 请求在 batchDelay ms 内合并为最少 Range 请求
 * 4. 智能预取：小请求（< prefetchThreshold）对齐 chunkSize 边界向后预取；
 *             大请求不预取；单次请求不超过 maxRequestSize
 * 5. 区间缓存：已获取数据写入有序缓存；新请求先查缓存，命中直接返回；
 *             相邻缓存块自动合并，避免碎片
 */
export class SmartRangeFile {
  /**
   * @param {string}   url                          - 文件 URL
   * @param {number}   size                         - 文件总字节数
   * @param {object}  [options]
   * @param {number}  [options.threshold=2097152]   - 小文件阈值，默认 2 MB
   * @param {number}  [options.batchDelay=10]       - 批量合并防抖延迟，ms
   * @param {number}  [options.chunkSize=524288]    - 预取对齐单元，默认 512 KB
   * @param {number}  [options.prefetchThreshold=131072] - 小请求判定阈值，默认 128 KB
   * @param {number}  [options.maxRequestSize=524288]    - 单次最大请求字节数，默认 512 KB
   * @param {function}[options.fetcher]             - 自定义 fetcher(url, start, end)=>Promise<ArrayBuffer>
   */
  constructor(url, size, options = {}) {
    this.url  = url;
    this.size = size;

    const {
      threshold         = 2   * 1024 * 1024, // 2 MB
      batchDelay        = 10,
      chunkSize         = 512 * 1024,         // 512 KB
      prefetchThreshold = 128 * 1024,         // 128 KB
      maxRequestSize    = 512 * 1024,         // 512 KB
      fetcher           = null,
    } = options;

    this.threshold         = threshold;
    this.batchDelay        = batchDelay;
    this.chunkSize         = chunkSize;
    this.prefetchThreshold = prefetchThreshold;
    this.maxRequestSize    = maxRequestSize;
    this.fetcher           = fetcher ?? this._defaultFetcher.bind(this);

    // ── 小文件整体缓存 ──
    this._wholeFilePromise = null;

    // ── 批量队列 ──
    // entry: { start, end, resolve, reject }
    this._pendingQueue = [];
    this._batchTimer   = null;

    // ── inflight 去重 ──
    // key = `${start}-${end}` → Promise<ArrayBuffer>
    this._inflightMap = new Map();

    // ── 区间缓存 ──
    // 有序数组，元素：{ start, end, buffer: ArrayBuffer }
    // 不变量：按 start 升序，相邻块不重叠也不紧邻（紧邻会被合并）
    this._cache = [];
  }

  // ═══════════════════════════════════════════════════════════════
  // 公开 API
  // ═══════════════════════════════════════════════════════════════

  /**
   * @param {number}  start
   * @param {number} [end]   - 不包含，省略则到文件末尾；支持负数索引
   * @returns {{ arrayBuffer: () => Promise<ArrayBuffer> }}
   */
  slice(start, end) {
    const { normStart, normEnd } = this._normalize(start, end);
    return { arrayBuffer: () => this._requestChunk(normStart, normEnd) };
  }

  // ═══════════════════════════════════════════════════════════════
  // 内部核心
  // ═══════════════════════════════════════════════════════════════

  /** 路由：空区间 / 小文件 / 大文件批量 */
  _requestChunk(start, end) {
    if (start >= end) return Promise.resolve(new ArrayBuffer(0));

    // 路径 A：小文件
    if (this.size <= this.threshold) {
      return this._getWholeFile().then((buf) => buf.slice(start, end));
    }

    // 路径 B：大文件——先查缓存，命中则直接返回，否则入队
    const cached = this._cacheRead(start, end);
    if (cached) return Promise.resolve(cached);

    return new Promise((resolve, reject) => {
      this._pendingQueue.push({ start, end, resolve, reject });
      this._scheduleBatch();
    });
  }

  // ───────────────────────────────────────────────────────────────
  // 批量处理
  // ───────────────────────────────────────────────────────────────

  _scheduleBatch() {
    if (this._batchTimer !== null) return;
    this._batchTimer = setTimeout(() => {
      this._batchTimer = null;
      this._flushBatch();
    }, this.batchDelay);
  }

  async _flushBatch() {
    if (this._pendingQueue.length === 0) return;

    const queue = this._pendingQueue;
    this._pendingQueue = [];

    // 1. 过滤：再次检查缓存（batchDelay 期间可能有其他批次写入缓存）
    const stillNeeded = [];
    for (const entry of queue) {
      const cached = this._cacheRead(entry.start, entry.end);
      if (cached) {
        entry.resolve(cached);
      } else {
        stillNeeded.push(entry);
      }
    }
    if (stillNeeded.length === 0) return;

    // 2. 合并请求区间
    const merged = this._mergeRanges(
      stillNeeded.map((e) => ({ start: e.start, end: e.end }))
    );

    // 3. 对每个合并区间：预取扩展 → 切掉缓存已有部分 → 限制最大大小
    //    产出若干实际需要 fetch 的子区间
    const fetchJobs = []; // { fetchStart, fetchEnd, coversStart, coversEnd }
    for (const range of merged) {
      const expanded  = this._applyPrefetch(range.start, range.end);
      const subRanges = this._subtractCache(expanded.start, expanded.end);
      for (const sub of subRanges) {
        // 限制单次请求大小，超大区间拆分
        const chunks = this._splitByMaxSize(sub.start, sub.end);
        for (const chunk of chunks) {
          fetchJobs.push({
            fetchStart:  chunk.start,
            fetchEnd:    chunk.end,
            // 记录这次 fetch 覆盖的"逻辑范围"（含预取）供缓存写入
            coversStart: chunk.start,
            coversEnd:   chunk.end,
          });
        }
      }
    }

    // 4. 并行发起 fetch，写入缓存
    await Promise.all(
      fetchJobs.map(async ({ fetchStart, fetchEnd }) => {
        const key = `${fetchStart}-${fetchEnd}`;
        let promise = this._inflightMap.get(key);
        if (!promise) {
          promise = this.fetcher(this.url, fetchStart, fetchEnd).then(
            (buf) => {
              this._cacheWrite(fetchStart, fetchEnd, buf);
              return buf;
            }
          ).finally(() => {
            this._inflightMap.delete(key);
          });
          this._inflightMap.set(key, promise);
        }
        // 等待完成（不在这里处理错误，错误在分发阶段处理）
        try { await promise; } catch (_) { /* 分发时处理 */ }
      })
    );

    // 5. 从缓存读取数据分发给各 entry
    for (const entry of stillNeeded) {
      const cached = this._cacheRead(entry.start, entry.end);
      if (cached) {
        entry.resolve(cached);
      } else {
        entry.reject(new Error(
          `SmartRangeFile: fetch failed for [${entry.start}, ${entry.end})`
        ));
      }
    }
  }

  // ───────────────────────────────────────────────────────────────
  // 预取 & 大小限制
  // ───────────────────────────────────────────────────────────────

  /**
   * 对合并后的逻辑请求区间应用预取策略，返回扩展后的区间。
   *
   * 规则：
   * - 请求大小 < prefetchThreshold（默认 128 KB）→ 小请求
   *   将 end 向后对齐到下一个 chunkSize 边界
   * - 请求大小 ≥ prefetchThreshold → 大请求，不预取
   *
   * @param {number} start
   * @param {number} end
   * @returns {{ start: number, end: number }}
   */
  _applyPrefetch(start, end) {
    const reqSize = end - start;
    if (reqSize >= this.prefetchThreshold) {
      // 大请求：不预取
      return { start, end };
    }

    // 小请求：end 对齐到下一个 chunkSize 边界
    const chunkIndex  = Math.floor(end / this.chunkSize);
    const nextBoundary = (chunkIndex + 1) * this.chunkSize;
    const prefetchEnd  = Math.min(nextBoundary, this.size);

    return { start, end: prefetchEnd };
  }

  /**
   * 把超过 maxRequestSize 的区间按 maxRequestSize 切分
   * @param {number} start
   * @param {number} end
   * @returns {{ start: number, end: number }[]}
   */
  _splitByMaxSize(start, end) {
    const result = [];
    let cur = start;
    while (cur < end) {
      const next = Math.min(cur + this.maxRequestSize, end);
      result.push({ start: cur, end: next });
      cur = next;
    }
    return result;
  }

  // ═══════════════════════════════════════════════════════════════
  // 缓存
  // ═══════════════════════════════════════════════════════════════

  /**
   * 查询缓存：如果 [start, end) 完全被某个缓存块覆盖，返回对应 ArrayBuffer 切片。
   * 否则返回 null。
   *
   * 注意：当前实现只处理"单块完全覆盖"的情况。
   * 跨多个缓存块的拼接见 _cacheReadMulti（如需可扩展）。
   *
   * @param {number} start
   * @param {number} end
   * @returns {ArrayBuffer|null}
   */
  _cacheRead(start, end) {
    // 二分找第一个可能覆盖 start 的块
    let lo = 0, hi = this._cache.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const blk  = this._cache[mid];
      if (blk.end <= start) {
        lo = mid + 1;
      } else if (blk.start > start) {
        hi = mid - 1;
      } else {
        // blk.start <= start < blk.end
        // 检查是否完全覆盖
        if (blk.end >= end) {
          const offset = start - blk.start;
          const length = end - start;
          return blk.buffer.slice(offset, offset + length);
        }
        // 当前块不够长——尝试拼接后续块
        return this._cacheReadMulti(start, end, mid);
      }
    }
    return null;
  }

  /**
   * 从 idx 块开始，尝试拼接连续缓存块以覆盖 [start, end)。
   * 若中间有缺口则返回 null。
   */
  _cacheReadMulti(start, end, idx) {
    const out    = new Uint8Array(end - start);
    let   cursor = start;

    for (let i = idx; i < this._cache.length && cursor < end; i++) {
      const blk = this._cache[i];
      if (blk.start > cursor) return null; // 有缺口
      const copyFrom = cursor - blk.start;
      const copyEnd  = Math.min(blk.end, end);
      const copyLen  = copyEnd - cursor;
      out.set(new Uint8Array(blk.buffer, copyFrom, copyLen), cursor - start);
      cursor = copyEnd;
    }

    return cursor >= end ? out.buffer : null;
  }

  /**
   * 写入缓存：把 [start, end) 对应的 buffer 插入有序列表，并合并相邻/重叠块。
   * @param {number} start
   * @param {number} end
   * @param {ArrayBuffer} buffer
   */
  _cacheWrite(start, end, buffer) {
    const newEntry = { start, end, buffer };

    // 找插入位置（保持 start 升序）
    let insertIdx = this._cache.length;
    for (let i = 0; i < this._cache.length; i++) {
      if (this._cache[i].start >= start) { insertIdx = i; break; }
    }
    this._cache.splice(insertIdx, 0, newEntry);

    // 合并：向左检查
    this._mergeCache(insertIdx);
  }

  /**
   * 以 idx 为中心，向左右合并重叠或相邻的缓存块。
   * "相邻"定义：blk[i].end === blk[i+1].start（字节紧挨着）
   */
  _mergeCache(idx) {
    // 向左找第一个与 idx 重叠/相邻的块
    let left = idx;
    while (left > 0 && this._cache[left - 1].end >= this._cache[left].start) {
      left--;
    }

    // 从 left 开始向右合并所有重叠/相邻块
    let right = left;
    while (
      right + 1 < this._cache.length &&
      this._cache[right].end >= this._cache[right + 1].start
    ) {
      right++;
    }

    if (right === left) return; // 没有需要合并的

    // 合并 [left, right] 范围内的所有块
    const mergedStart = this._cache[left].start;
    const mergedEnd   = this._cache[right].end;
    const mergedLen   = mergedEnd - mergedStart;
    const mergedBuf   = new Uint8Array(mergedLen);

    for (let i = left; i <= right; i++) {
      const blk    = this._cache[i];
      const offset = blk.start - mergedStart;
      mergedBuf.set(new Uint8Array(blk.buffer), offset);
    }

    // 替换
    this._cache.splice(left, right - left + 1, {
      start:  mergedStart,
      end:    mergedEnd,
      buffer: mergedBuf.buffer,
    });
  }

  /**
   * 从 [start, end) 中减去缓存已有区间，返回需要实际 fetch 的子区间列表。
   * @param {number} start
   * @param {number} end
   * @returns {{ start: number, end: number }[]}
   */
  _subtractCache(start, end) {
    const result = [];
    let   cursor = start;

    for (const blk of this._cache) {
      if (blk.start >= end) break;       // 超出右边界
      if (blk.end   <= cursor) continue; // 在左边界之前

      if (blk.start > cursor) {
        // cursor → blk.start 这段缺失，需要 fetch
        result.push({ start: cursor, end: blk.start });
      }
      cursor = Math.max(cursor, blk.end);
    }

    if (cursor < end) {
      result.push({ start: cursor, end });
    }

    return result;
  }

  // ═══════════════════════════════════════════════════════════════
  // 通用工具
  // ═══════════════════════════════════════════════════════════════

  /** 合并重叠或相邻的区间（排序 + 线性扫描） */
  _mergeRanges(ranges) {
    if (ranges.length === 0) return [];
    const sorted = [...ranges].sort((a, b) => a.start - b.start || a.end - b.end);
    const merged = [{ ...sorted[0] }];
    for (let i = 1; i < sorted.length; i++) {
      const cur  = sorted[i];
      const last = merged[merged.length - 1];
      if (cur.start <= last.end) {
        last.end = Math.max(last.end, cur.end);
      } else {
        merged.push({ ...cur });
      }
    }
    return merged;
  }

  /** 小文件：整体获取（只请求一次） */
  _getWholeFile() {
    if (!this._wholeFilePromise) {
      this._wholeFilePromise = this.fetcher(this.url, 0, this.size);
    }
    return this._wholeFilePromise;
  }

  /** 规范化 slice 边界（Blob.slice 语义：负数、超界、end<start） */
  _normalize(start, end) {
    const size = this.size;
    const normStart = start < 0 ? Math.max(0, size + start) : Math.min(start, size);
    let normEnd   = end === undefined
      ? size
      : end < 0
        ? Math.max(0, size + end)
        : Math.min(end, size);
    if (normEnd < normStart) normEnd = normStart;
    return { normStart, normEnd };
  }

  /** 默认 fetcher：浏览器 fetch + Range 头 */
  async _defaultFetcher(url, start, end) {
    const res = await fetch(url, {
      headers: { Range: `bytes=${start}-${end - 1}` },
    });
    if (!res.ok && res.status !== 206) {
      throw new Error(
        `SmartRangeFile fetch failed: ${res.status} ${res.statusText} ` +
        `for [${start}, ${end})`
      );
    }
    return res.arrayBuffer();
  }
}


/**
 * 支持 HTTP Range 请求的文件类
 * 优先使用 GM_xmlhttpRequest（Tampermonkey/Greasemonkey 环境）
 * 回退到标准 fetch API
 * 用于配合 mobi.js 实现按需分块加载
 * 支持请求合并和智能预取
 */

/**
 * 检查当前环境是否支持 GM_xmlhttpRequest
 * @returns {boolean}
 */
export const hasGM = () => typeof GM_xmlhttpRequest !== 'undefined'

/**
 * URL 映射缓存，用于存储重定向后的最终 URL
 * @type {Map<string, string>}
 */
const urlMap = new Map()

/**
 * 统一的 GM_xmlhttpRequest 封装
 * @param {Object} options - 请求选项
 * @param {string} options.method - 请求方法
 * @param {string} options.url - 请求 URL
 * @param {Object} options.headers - 请求头
 * @param {string} options.responseType - 响应类型
 * @param {Function} options.onload - 加载回调
 * @param {Function} options.onerror - 错误回调
 * @param {Function} options.ontimeout - 超时回调
 * @returns {Promise<response>}
 */
export const GM_request = (url, options = {}) => {
    const { method = 'GET', headers = {}, responseType } = options
    const finalUrl = urlMap.get(url) || url

    return new Promise((resolve, reject) => {
        GM_xmlhttpRequest({
            method,
            url: finalUrl,
            headers: {
                'User-Agent': 'pan.baidu.com',
                ...headers,
            },
            responseType,
            onload: (response) => {
                if (response.finalUrl && response.finalUrl !== finalUrl) {
                    urlMap.set(url, response.finalUrl)
                }
                resolve(response)
            },
            onerror: (error) => reject(error),
            ontimeout: () => reject(new Error('GM_xmlhttpRequest timeout')),
        })
    })
}

/**
 * 使用 GM_xmlhttpRequest 发起 Range 请求
 * @param {string} url - 请求地址
 * @param {number} start - 字节起始位置
 * @param {number} end - 字节结束位置（不包含）
 * @returns {Promise<ArrayBuffer>}
 */
export const gmRangeRequest = async (url, start, end) => {
    const response = await GM_request(url, {
        headers: {
            'Range': `bytes=${start}-${end - 1}`,
        },
        responseType: 'arraybuffer',
    })

    if (response.status >= 200 && response.status < 300) {
        return response.response
    } else {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }
}

/**
 * 使用 fetch API 发起 Range 请求
 * @param {string} url - 请求地址
 * @param {number} start - 字节起始位置
 * @param {number} end - 字节结束位置（不包含）
 * @returns {Promise<ArrayBuffer>}
 */
export const fetchRangeRequest = async (url, start, end) => {
    const response = await fetch(url, {
        headers: {
            'Range': `bytes=${start}-${end - 1}`,
        },
    })
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }
    return response.arrayBuffer()
}

export const fetcher = hasGM()
  ? (url, s, e) => gmRangeRequest(url, s, e)
  : (url, s, e) => fetchRangeRequest(url, s, e)


/**
 * 通过 HEAD 请求获取文件大小，如果 403 则通过 Range 0-0 请求获取
 * @param {string} url - 文件 URL
 * @returns {Promise<number>} 文件大小（字节）
 */
export const getFileSize = async (url) => {
    if (hasGM()) {
        const response = await GM_request(url, { method: 'HEAD' })

        if (response.status >= 200 && response.status < 300) {
            const contentLength = response.responseHeaders
                .split('\n')
                .find(line => line.toLowerCase().startsWith('content-length:'))
            if (contentLength) {
                return parseInt(contentLength.split(':')[1].trim(), 10)
            } else {
                throw new Error('Content-Length header not found')
            }
        } else if (response.status === 403) {
            const rangeResponse = await GM_request(url, {
                headers: { 'Range': 'bytes=0-0' },
            })
            if (rangeResponse.status === 206) {
                const contentRange = rangeResponse.responseHeaders
                    .split('\n')
                    .find(line => line.toLowerCase().startsWith('content-range:'))
                if (contentRange) {
                    const match = contentRange.match(/bytes\s+\d+-(\d+)\/(\d+)/)
                    if (match) {
                        const fileSize = parseInt(match[2], 10)
                        console.debug(`[range-file] 403 时通过 Range 0-0 获取文件大小: ${fileSize}`)
                        return fileSize
                    }
                }
            }
            throw new Error(`HTTP ${response.status}: ${response.statusText}`)
        } else {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`)
        }
    } else {
        const finalUrl = urlMap.get(url) || url
        const response = await fetch(finalUrl, { method: 'HEAD' })
        if (response.status === 403) {
            const rangeResponse = await fetch(finalUrl, {
                headers: { 'Range': 'bytes=0-0' },
            })
            if (rangeResponse.status === 206) {
                const contentRange = rangeResponse.headers.get('content-range')
                if (contentRange) {
                    const match = contentRange.match(/bytes\s+\d+-(\d+)\/(\d+)/)
                    if (match) {
                        const fileSize = parseInt(match[2], 10)
                        console.debug(`[range-file] 403 时通过 Range 0-0 获取文件大小: ${fileSize}`)
                        return fileSize
                    }
                }
            }
            throw new Error(`HTTP ${response.status}: ${response.statusText}`)
        }
        const contentLength = response.headers.get('content-length')
        if (contentLength) {
            return parseInt(contentLength, 10)
        }
        throw new Error('Content-Length header not found')
    }
}

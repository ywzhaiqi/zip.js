//#region src/SmartRangeFile.ts
var SmartRangeFile = class {
	/**
	* @param url - 文件 URL
	* @param size - 文件总字节数
	* @param options - 配置选项
	*/
	constructor(url, size, options = {}) {
		this._wholeFilePromise = null;
		this._pendingQueue = [];
		this._batchTimer = null;
		this._inflightMap = /* @__PURE__ */ new Map();
		this._cache = [];
		this._cacheSize = 0;
		this.url = url;
		this.size = size;
		const { threshold = 2 * 1024 * 1024, batchDelay = 10, chunkSize = 512 * 1024, prefetchThreshold = 128 * 1024, maxRequestSize = 512 * 1024, maxCacheBytes = 50 * 1024 * 1024, fetcher = null } = options;
		this.threshold = threshold;
		this.batchDelay = batchDelay;
		this.chunkSize = chunkSize;
		this.prefetchThreshold = prefetchThreshold;
		this.maxRequestSize = maxRequestSize;
		this.maxCacheBytes = maxCacheBytes;
		this.fetcher = fetcher ?? this._defaultFetcher.bind(this);
	}
	/**
	* 切片获取文件数据
	* @param start - 起始位置
	* @param end - 结束位置（不包含），省略则到文件末尾；支持负数索引
	* @returns SliceResult
	*/
	slice(start, end) {
		const { normStart, normEnd } = this._normalize(start, end);
		return { arrayBuffer: () => this._requestChunk(normStart, normEnd) };
	}
	/**
	* 路由：空区间 / 小文件 / 大文件批量
	* @param start - 起始位置
	* @param end - 结束位置
	* @returns Promise<ArrayBuffer>
	*/
	async _requestChunk(start, end) {
		if (start >= end) return Promise.resolve(/* @__PURE__ */ new ArrayBuffer(0));
		if (this.size <= this.threshold) return (await this._getWholeFile()).slice(start, end);
		const cached = this._cacheRead(start, end);
		if (cached) return Promise.resolve(cached);
		return new Promise((resolve, reject) => {
			this._pendingQueue.push({
				start,
				end,
				resolve,
				reject
			});
			this._scheduleBatch();
		});
	}
	/**
	* 调度批量处理
	*/
	_scheduleBatch() {
		if (this._batchTimer !== null) return;
		this._batchTimer = setTimeout(() => {
			this._batchTimer = null;
			this._flushBatch();
		}, this.batchDelay);
	}
	/**
	* 执行批量请求
	*/
	async _flushBatch() {
		if (this._pendingQueue.length === 0) return;
		const queue = this._pendingQueue;
		this._pendingQueue = [];
		const stillNeeded = [];
		for (const entry of queue) {
			const cached = this._cacheRead(entry.start, entry.end);
			if (cached) entry.resolve(cached);
			else stillNeeded.push(entry);
		}
		if (stillNeeded.length === 0) return;
		const merged = this._mergeRanges(stillNeeded.map((e) => ({
			start: e.start,
			end: e.end
		})));
		const fetchJobs = [];
		for (const range of merged) {
			const expanded = this._applyPrefetch(range.start, range.end);
			const subRanges = this._subtractCache(expanded.start, expanded.end);
			for (const sub of subRanges) {
				const chunks = this._splitByMaxSize(sub.start, sub.end);
				for (const chunk of chunks) fetchJobs.push({
					fetchStart: chunk.start,
					fetchEnd: chunk.end,
					coversStart: chunk.start,
					coversEnd: chunk.end
				});
			}
		}
		await Promise.all(fetchJobs.map(async ({ fetchStart, fetchEnd }) => {
			const key = `${fetchStart}-${fetchEnd}`;
			let promise = this._inflightMap.get(key);
			if (!promise) {
				promise = this.fetcher(this.url, fetchStart, fetchEnd).then((buf) => {
					this._cacheWrite(fetchStart, fetchEnd, buf);
					return buf;
				}).finally(() => {
					this._inflightMap.delete(key);
				});
				this._inflightMap.set(key, promise);
			}
			try {
				await promise;
			} catch (_) {}
		}));
		for (const entry of stillNeeded) {
			const cached = this._cacheRead(entry.start, entry.end);
			if (cached) entry.resolve(cached);
			else entry.reject(/* @__PURE__ */ new Error(`SmartRangeFile: fetch failed for [${entry.start}, ${entry.end})`));
		}
	}
	/**
	* 对合并后的逻辑请求区间应用预取策略
	* @param start - 起始位置
	* @param end - 结束位置
	* @returns 扩展后的区间
	*/
	_applyPrefetch(start, end) {
		if (end - start >= this.prefetchThreshold) return {
			start,
			end
		};
		const nextBoundary = (Math.floor(end / this.chunkSize) + 1) * this.chunkSize;
		return {
			start,
			end: Math.min(nextBoundary, this.size)
		};
	}
	/**
	* 把超过 maxRequestSize 的区间按 maxRequestSize 切分
	* @param start - 起始位置
	* @param end - 结束位置
	* @returns 切分后的区间数组
	*/
	_splitByMaxSize(start, end) {
		const result = [];
		let cur = start;
		while (cur < end) {
			const next = Math.min(cur + this.maxRequestSize, end);
			result.push({
				start: cur,
				end: next
			});
			cur = next;
		}
		return result;
	}
	/**
	* 查询缓存：如果 [start, end) 完全被某个缓存块覆盖，返回对应 ArrayBuffer 切片
	* @param start - 起始位置
	* @param end - 结束位置
	* @returns ArrayBuffer 或 null
	*/
	_cacheRead(start, end) {
		let lo = 0, hi = this._cache.length - 1;
		while (lo <= hi) {
			const mid = lo + hi >> 1;
			const blk = this._cache[mid];
			if (blk.end <= start) lo = mid + 1;
			else if (blk.start > start) hi = mid - 1;
			else {
				if (blk.end >= end) {
					const offset = start - blk.start;
					const length = end - start;
					return blk.buffer.slice(offset, offset + length);
				}
				return this._cacheReadMulti(start, end, mid);
			}
		}
		return null;
	}
	/**
	* 从 idx 块开始，尝试拼接连续缓存块以覆盖 [start, end)
	* @param start - 起始位置
	* @param end - 结束位置
	* @param idx - 起始块索引
	* @returns ArrayBuffer 或 null
	*/
	_cacheReadMulti(start, end, idx) {
		const out = new Uint8Array(end - start);
		let cursor = start;
		for (let i = idx; i < this._cache.length && cursor < end; i++) {
			const blk = this._cache[i];
			if (blk.start > cursor) return null;
			const copyFrom = cursor - blk.start;
			const copyEnd = Math.min(blk.end, end);
			const copyLen = copyEnd - cursor;
			out.set(new Uint8Array(blk.buffer, copyFrom, copyLen), cursor - start);
			cursor = copyEnd;
		}
		return cursor >= end ? out.buffer : null;
	}
	/**
	* 写入缓存：把 [start, end) 对应的 buffer 插入有序列表，并合并相邻/重叠块
	* @param start - 起始位置
	* @param end - 结束位置
	* @param buffer - 数据缓冲区
	*/
	_cacheWrite(start, end, buffer) {
		const newEntry = {
			start,
			end,
			buffer
		};
		let insertIdx = this._cache.length;
		for (let i = 0; i < this._cache.length; i++) if (this._cache[i].start >= start) {
			insertIdx = i;
			break;
		}
		this._cache.splice(insertIdx, 0, newEntry);
		this._cacheSize += buffer.byteLength;
		this._mergeCache(insertIdx);
		this._evictIfNeeded();
	}
	/**
	* 检查并执行缓存淘汰，确保缓存不超过 maxCacheBytes
	*/
	_evictIfNeeded() {
		if (this.maxCacheBytes <= 0) return;
		while (this._cacheSize > this.maxCacheBytes && this._cache.length > 0) {
			const evicted = this._cache.shift();
			this._cacheSize -= evicted.buffer.byteLength;
		}
	}
	/**
	* 以 idx 为中心，向左右合并重叠或相邻的缓存块
	* @param idx - 中心块索引
	*/
	_mergeCache(idx) {
		let left = idx;
		while (left > 0 && this._cache[left - 1].end >= this._cache[left].start) left--;
		let right = left;
		while (right + 1 < this._cache.length && this._cache[right].end >= this._cache[right + 1].start) right++;
		if (right === left) return;
		const mergedStart = this._cache[left].start;
		const mergedEnd = this._cache[right].end;
		const mergedLen = mergedEnd - mergedStart;
		const mergedBuf = new Uint8Array(mergedLen);
		let removedSize = 0;
		for (let i = left; i <= right; i++) {
			const blk = this._cache[i];
			const offset = blk.start - mergedStart;
			mergedBuf.set(new Uint8Array(blk.buffer), offset);
			removedSize += blk.buffer.byteLength;
		}
		this._cache.splice(left, right - left + 1, {
			start: mergedStart,
			end: mergedEnd,
			buffer: mergedBuf.buffer
		});
		this._cacheSize = this._cacheSize - removedSize + mergedBuf.byteLength;
	}
	/**
	* 从 [start, end) 中减去缓存已有区间，返回需要实际 fetch 的子区间列表
	* @param start - 起始位置
	* @param end - 结束位置
	* @returns 需要获取的子区间列表
	*/
	_subtractCache(start, end) {
		const result = [];
		let cursor = start;
		for (const blk of this._cache) {
			if (blk.start >= end) break;
			if (blk.end <= cursor) continue;
			if (blk.start > cursor) result.push({
				start: cursor,
				end: blk.start
			});
			cursor = Math.max(cursor, blk.end);
		}
		if (cursor < end) result.push({
			start: cursor,
			end
		});
		return result;
	}
	/**
	* 合并重叠或相邻的区间
	* @param ranges - 区间数组
	* @returns 合并后的区间数组
	*/
	_mergeRanges(ranges) {
		if (ranges.length === 0) return [];
		const sorted = [...ranges].sort((a, b) => a.start - b.start || a.end - b.end);
		const merged = [{ ...sorted[0] }];
		for (let i = 1; i < sorted.length; i++) {
			const cur = sorted[i];
			const last = merged[merged.length - 1];
			if (cur.start <= last.end) last.end = Math.max(last.end, cur.end);
			else merged.push({ ...cur });
		}
		return merged;
	}
	/**
	* 小文件：整体获取（只请求一次）
	* @returns Promise<ArrayBuffer>
	*/
	_getWholeFile() {
		if (!this._wholeFilePromise) this._wholeFilePromise = this.fetcher(this.url, 0, this.size);
		return this._wholeFilePromise;
	}
	/**
	* 规范化 slice 边界（Blob.slice 语义：负数、超界、end<start）
	* @param start - 起始位置
	* @param end - 结束位置
	* @returns 规范化后的边界
	*/
	_normalize(start, end) {
		const size = this.size;
		const normStart = start < 0 ? Math.max(0, size + start) : Math.min(start, size);
		let normEnd = end === void 0 ? size : end < 0 ? Math.max(0, size + end) : Math.min(end, size);
		if (normEnd < normStart) normEnd = normStart;
		return {
			normStart,
			normEnd
		};
	}
	/**
	* 默认 fetcher：浏览器 fetch + Range 头
	* @param url - 请求 URL
	* @param start - 起始位置
	* @param end - 结束位置
	* @returns Promise<ArrayBuffer>
	*/
	async _defaultFetcher(url, start, end) {
		const res = await fetch(url, { headers: { Range: `bytes=${start}-${end - 1}` } });
		if (!res.ok && res.status !== 206) throw new Error(`SmartRangeFile fetch failed: ${res.status} ${res.statusText} for [${start}, ${end})`);
		return res.arrayBuffer();
	}
};
//#endregion
//#region src/gm.ts
/**
* 检查当前环境是否支持 GM_xmlhttpRequest
* @returns 是否支持 GM_xmlhttpRequest
*/
const hasGM = () => typeof GM_xmlhttpRequest !== "undefined";
/**
* URL 映射缓存，用于存储重定向后的最终 URL
*/
const urlMap = /* @__PURE__ */ new Map();
/**
* 获取 URL 映射缓存
* @returns URL 映射 Map
*/
const getUrlMap = () => urlMap;
/**
* 统一的 GM_xmlhttpRequest 封装
* @param url - 请求 URL
* @param options - 请求选项
* @returns Promise<GMResponse>
*/
const GM_request = (url, options = {}) => {
	const { method = "GET", headers = {}, responseType } = options;
	const finalUrl = urlMap.get(url) || url;
	return new Promise((resolve, reject) => {
		GM_xmlhttpRequest({
			method,
			url: finalUrl,
			headers: { ...headers },
			responseType,
			onload: (response) => {
				if (response.finalUrl && response.finalUrl !== finalUrl) urlMap.set(url, response.finalUrl);
				resolve(response);
			},
			onerror: (error) => reject(error),
			ontimeout: () => reject(/* @__PURE__ */ new Error("GM_xmlhttpRequest timeout"))
		});
	});
};
/**
* 使用 GM_xmlhttpRequest 发起 Range 请求
* @param url - 请求地址
* @param start - 字节起始位置
* @param end - 字节结束位置（不包含）
* @param headers - 可选的自定义请求头
* @returns Promise<ArrayBuffer>
*/
const gmRangeRequest = async (url, start, end, headers) => {
	const response = await GM_request(url, {
		headers: {
			Range: `bytes=${start}-${end - 1}`,
			...headers
		},
		responseType: "arraybuffer"
	});
	if (response.status >= 200 && response.status < 300) return response.response;
	else throw new Error(`HTTP ${response.status}: ${response.statusText}`);
};
/**
* 通过 GM_xmlhttpRequest 获取文件大小
* 可能不准确，有些服务器会返回 200 但内容是 JSON 错误信息
* @param url - 文件 URL
* @param headers - 可选的自定义请求头
* @returns 文件大小（字节）
*/
const gmGetFileSize = async (url, headers) => {
	const rangeResponse = await GM_request(url, { headers: {
		Range: "bytes=0-0",
		...headers
	} });
	if (rangeResponse.status === 206) {
		const contentRange = rangeResponse.responseHeaders.split("\n").find((line) => line.toLowerCase().startsWith("content-range:"));
		if (contentRange) {
			const match = contentRange.match(/bytes\s+\d+-(\d+)\/(\d+)/);
			if (match) {
				const fileSize = parseInt(match[2], 10);
				console.debug(`[range-file] 通过 Range 0-0 获取文件大小: ${fileSize}`);
				return fileSize;
			}
		}
	}
	const response = await GM_request(url, {
		method: "HEAD",
		headers
	});
	if (response.status >= 200 && response.status < 300) {
		const contentLength = response.responseHeaders.split("\n").find((line) => line.toLowerCase().startsWith("content-length:"));
		if (contentLength) return parseInt(contentLength.split(":")[1].trim(), 10);
		else throw new Error("Content-Length header not found");
	} else throw new Error(`HTTP ${response.status}: ${response.statusText}`);
};
//#endregion
//#region src/fetch.ts
/**
* Fetch API 相关功能
* 用于标准浏览器环境
*/
/**
* 使用 fetch API 发起 Range 请求
* @param url - 请求地址
* @param start - 字节起始位置
* @param end - 字节结束位置（不包含）
* @param headers - 可选的自定义请求头
* @returns Promise<ArrayBuffer>
*/
const fetchRangeRequest = async (url, start, end, headers) => {
	const response = await fetch(url, { headers: {
		Range: `bytes=${start}-${end - 1}`,
		...headers
	} });
	if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
	return response.arrayBuffer();
};
/**
* 通过 fetch API 获取文件大小
* @param url - 文件 URL
* @param headers - 可选的自定义请求头
* @returns 文件大小（字节）
*/
const fetchGetFileSize = async (url, headers) => {
	const finalUrl = getUrlMap().get(url) || url;
	const rangeResponse = await fetch(finalUrl, { headers: {
		Range: "bytes=0-0",
		...headers
	} });
	if (rangeResponse.status === 206) {
		const contentRange = rangeResponse.headers.get("content-range");
		if (contentRange) {
			const match = contentRange.match(/bytes\s+\d+-(\d+)\/(\d+)/);
			if (match) {
				const fileSize = parseInt(match[2], 10);
				console.debug(`[range-file] 通过 Range 0-0 获取文件大小: ${fileSize}`);
				return fileSize;
			}
		}
	}
	const response = await fetch(finalUrl, {
		method: "HEAD",
		headers
	});
	if (response.status >= 200 && response.status < 300) {
		const contentLength = response.headers.get("content-length");
		if (contentLength) return parseInt(contentLength, 10);
		throw new Error("Content-Length header not found");
	} else throw new Error(`HTTP ${response.status}: ${response.statusText}`);
};
//#endregion
//#region src/utils.ts
/**
* 工具函数
*/
/**
* 创建统一的 Range 请求 fetcher
* 优先使用 GM_xmlhttpRequest，回退到标准 fetch API
* @param headers - 可选的自定义请求头
* @returns fetcher 函数
*/
const createFetcher = (headers) => hasGM() ? (url, start, end) => gmRangeRequest(url, start, end, headers) : (url, start, end) => fetchRangeRequest(url, start, end, headers);
/**
* 统一的 Range 请求 fetcher（无自定义 headers）
* 优先使用 GM_xmlhttpRequest，回退到标准 fetch API
* @param url - 请求地址
* @param start - 字节起始位置
* @param end - 字节结束位置（不包含）
* @returns Promise<ArrayBuffer>
* @deprecated 请使用 createFetcher() 创建带自定义 headers 的 fetcher
*/
const fetcher = createFetcher();
/**
* 通过 HEAD 请求获取文件大小，如果 403 则通过 Range 0-0 请求获取
* @param url - 文件 URL
* @param headers - 可选的自定义请求头
* @returns 文件大小（字节）
*/
const getFileSize = async (url, headers) => {
	if (hasGM()) return gmGetFileSize(url, headers);
	else return fetchGetFileSize(url, headers);
};
//#endregion
export { GM_request, SmartRangeFile, createFetcher, fetchGetFileSize, fetchRangeRequest, fetcher, getFileSize, getUrlMap, gmGetFileSize, gmRangeRequest, hasGM };

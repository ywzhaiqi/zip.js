import { SmartRangeFile, getFileSize, createFetcher } from './smart-range-file.mjs'

const headers = {
  'User-Agent': 'pan.baidu.com',
}

async function getUrlFileSize(url) {
  return await getFileSize(url, headers)
}

const customFetcher = createFetcher(headers)

export { SmartRangeFile, customFetcher, getUrlFileSize }
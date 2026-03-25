# Introduction

zip.js is a JavaScript open-source library (BSD-3-Clause license) for
compressing and decompressing zip files. It has been designed to handle large amounts
of data. It supports notably multi-core compression, native compression with
compression streams, archives larger than 4GB with Zip64, split zip files, data
encryption, and Deflate64 decompression.

# Demo

See https://gildas-lormeau.github.io/zip-manager

# Documentation

See here for more info: https://gildas-lormeau.github.io/zip.js/

# New Features

## GM_xmlhttpRequest 支持

在油猴脚本（Tampermonkey / Violentmonkey）中运行时，可以使用 `useGM` 选项通过 `GM_xmlhttpRequest` 读取远程 ZIP 文件，解决跨域问题。

```js
import { ZipReader, HttpReader } from '@zip.js/zip.js';

// 使用 GM_xmlhttpRequest 读取远程 ZIP 文件
const reader = new HttpReader('https://example.com/file.zip', { 
  useGM: true,
  useRangeHeader: true,
  // forceRangeRequests: true,  // 跳过检查 acceptRanges bytes
  // 自定义请求头
  headers: {
    'User-Agent': 'zip.js/1.0.0'
  }
});
const zipReader = new ZipReader(reader);
const entries = await zipReader.getEntries();
await zipReader.close();
```

**选项说明：**
- `useGM: true` - 强制使用 GM_xmlhttpRequest
- `useRangeHeader: true` - 使用 Range 请求，支持断点续传和大文件

**注意：** 需要在油猴脚本环境中运行，或者自行注入 `GM_xmlhttpRequest` 到页面中。

## getMultipleData

```js
import { ZipReader, HttpRangeReader, Uint8ArrayWriter } from '@zip.js/zip.js';
 
// 打开远程 ZIP 文件
const reader = new HttpRangeReader('https://example.com/large.zip');
const zipReader = new ZipReader(reader);
const entries = await zipReader.getEntries();
 
// 选择要下载的文件
const targetFiles = entries.filter(e => 
  !e.directory && e.filename.endsWith('.jpg')
);
 
// 批量获取（自动合并 Range 请求）
const results = await zipReader.getMultipleData(
  targetFiles,
  () => new Uint8ArrayWriter(),
  {
    mergeThreshold: 2 * 1024 * 1024,  // 2MB 间隙内合并
    maxRangeSize: 100 * 1024 * 1024,   // 单次最大 100MB
    onProgress: (done, total) => {
      console.log(`进度: ${done}/${total}`);
    }
  }
);
 
// 处理结果
for (const [entry, data] of results) {
  console.log(`${entry.filename}: ${data.byteLength} bytes`);
  // 可以保存到本地等操作
}
 
await zipReader.close();
```

# Examples

## Hello world

```js
import {
  BlobReader,
  BlobWriter,
  TextReader,
  TextWriter,
  ZipReader,
  ZipWriter
} from "@zip-js/zip-js";
// Prefix "@zip-js/zip-js" with "jsr:" for Deno

// ----
// Write the zip file
// ----

// Creates a BlobWriter object where the zip content will be written.
const zipFileWriter = new BlobWriter();
// Creates a TextReader object storing the text of the entry to add in the zip
// (i.e. "Hello world!").
const helloWorldReader = new TextReader("Hello world!");

// Creates a ZipWriter object writing data via `zipFileWriter`, adds the entry
// "hello.txt" containing the text "Hello world!" via `helloWorldReader`, and
// closes the writer.
const zipWriter = new ZipWriter(zipFileWriter);
await zipWriter.add("hello.txt", helloWorldReader);
await zipWriter.close();

// Retrieves the Blob object containing the zip content into `zipFileBlob`. It
// is also returned by zipWriter.close() for more convenience.
const zipFileBlob = await zipFileWriter.getData();

// ----
// Read the zip file
// ----

// Creates a BlobReader object used to read `zipFileBlob`.
const zipFileReader = new BlobReader(zipFileBlob);
// Creates a TextWriter object where the content of the first entry in the zip
// will be written.
const helloWorldWriter = new TextWriter();

// Creates a ZipReader object reading the zip content via `zipFileReader`,
// retrieves metadata (name, dates, etc.) of the first entry, retrieves its
// content via `helloWorldWriter`, and closes the reader.
const zipReader = new ZipReader(zipFileReader);
const firstEntry = (await zipReader.getEntries()).shift();
const helloWorldText = await firstEntry.getData(helloWorldWriter);
await zipReader.close();

// Displays "Hello world!".
console.log(helloWorldText);
```

Run the code on JSFiddle: https://jsfiddle.net/tm9fhvab/

## Hello world with Streams

```js
import {
  BlobReader,
  ZipReader,
  ZipWriter
} from "@zip-js/zip-js";
// Prefix "@zip-js/zip-js" with "jsr:" for Deno

// ----
// Write the zip file
// ----

// Creates a TransformStream object, the zip content will be written in the
// `writable` property.
const zipFileStream = new TransformStream();
// Creates a Promise object resolved to the zip content returned as a Blob
// object retrieved from `zipFileStream.readable`.
const zipFileBlobPromise = new Response(zipFileStream.readable).blob();
// Creates a ReadableStream object storing the text of the entry to add in the
// zip (i.e. "Hello world!").
const helloWorldReadable = new Blob(["Hello world!"]).stream();

// Creates a ZipWriter object writing data into `zipFileStream.writable`, adds
// the entry "hello.txt" containing the text "Hello world!" retrieved from
// `helloWorldReadable`, and closes the writer.
const zipWriter = new ZipWriter(zipFileStream.writable);
await zipWriter.add("hello.txt", helloWorldReadable);
await zipWriter.close();

// Retrieves the Blob object containing the zip content into `zipFileBlob`.
const zipFileBlob = await zipFileBlobPromise;

// ----
// Read the zip file
// ----

// Creates a BlobReader object used to read `zipFileBlob`.
const zipFileReader = new BlobReader(zipFileBlob);
// Creates a TransformStream object, the content of the first entry in the zip
// will be written in the `writable` property.
const helloWorldStream = new TransformStream();
// Creates a Promise object resolved to the content of the first entry returned
// as text from `helloWorldStream.readable`.
const helloWorldTextPromise = new Response(helloWorldStream.readable).text();

// Creates a ZipReader object reading the zip content via `zipFileReader`,
// retrieves metadata (name, dates, etc.) of the first entry, retrieves its
// content into `helloWorldStream.writable`, and closes the reader.
const zipReader = new ZipReader(zipFileReader);
const firstEntry = (await zipReader.getEntries()).shift();
await firstEntry.getData(helloWorldStream.writable);
await zipReader.close();

// Displays "Hello world!".
const helloWorldText = await helloWorldTextPromise;
console.log(helloWorldText);
```

Run the code on JSFiddle: https://jsfiddle.net/aw3d6f4o/

## Adding concurrently multiple entries in a zip file

```js
import {
  BlobWriter,
  HttpReader,
  TextReader,
  ZipWriter,
} from "@zip-js/zip-js";
// Prefix "@zip-js/zip-js" with "jsr:" for Deno

const README_URL = "https://unpkg.com/@zip.js/zip.js/README.md";
getZipFileBlob()
  .then(downloadFile);

async function getZipFileBlob() {
  const zipWriter = new ZipWriter(new BlobWriter("application/zip"));
  await Promise.all([
    zipWriter.add("hello.txt", new TextReader("Hello world!")),
    zipWriter.add("README.md", new HttpReader(README_URL)),
  ]);
  return zipWriter.close();
}

function downloadFile(blob) {
  document.body.appendChild(Object.assign(document.createElement("a"), {
    download: "hello.zip",
    href: URL.createObjectURL(blob),
    textContent: "Download zip file",
  }));
}
```

Run the code on Plunker: https://plnkr.co/edit/4sVljNIpqSUE9HCA?preview

## Tests

See https://github.com/gildas-lormeau/zip.js/tree/master/tests/all

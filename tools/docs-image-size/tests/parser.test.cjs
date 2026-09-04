'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { test, after } = require('node:test');

const baseline = process.env.IMAGE_SIZE_UPSTREAM;
const runtime = baseline ? path.join(baseline, 'dist') : path.join(__dirname, '..', 'lib');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'membrane-image-parsers-'));
after(() => fs.rmSync(temp, { recursive: true, force: true }));
const worker = `
const fs = require('node:fs');
const { pathToFileURL } = require('node:url');
(async () => {
  const [entry, mode, format, file] = process.argv.slice(1);
  const mod = entry.endsWith('.mjs') ? await import(pathToFileURL(entry).href) : require(entry);
  try {
    let result;
    if (mode === 'fromFile') result = await mod.imageSizeFromFile(file);
    else {
      const input = fs.readFileSync(file);
      if (mode === 'leaf') result = mod[format].calculate(input);
      else if (mode === 'handlers') result = mod.typeHandlers.get(format.toLowerCase()).calculate(input);
      else result = mod.imageSize(input);
    }
    process.stdout.write(JSON.stringify({ ok: true, result }));
  } catch (error) { process.stdout.write(JSON.stringify({ ok: false, error: error.message })); }
})().catch(error => { console.error(error); process.exitCode = 1; });
`;
function parse(input, entry, mode = 'imageSize', format = '') {
  const file = path.join(temp, 'input');
  fs.writeFileSync(file, input);
  const child = spawnSync(process.execPath, ['-e', worker, path.join(runtime, entry), mode, format, file], {
    encoding: 'utf8', timeout: baseline ? 300 : 2000, killSignal: 'SIGKILL', maxBuffer: 1024 * 1024
  });
  assert.equal(child.error, undefined, `${entry}: parser failed to terminate: ${child.error}`);
  assert.equal(child.status, 0, `${entry}: ${child.stderr}`);
  return JSON.parse(child.stdout);
}
function box(name, body = Buffer.alloc(0), declaredSize = body.length + 8) {
  const header = Buffer.alloc(8);
  header.writeUInt32BE(declaredSize, 0);
  header.write(name, 4, 'ascii');
  return Buffer.concat([header, body]);
}
function icns(entryLength = 8) {
  const data = Buffer.alloc(16);
  data.write('icns', 0); data.writeUInt32BE(16, 4);
  data.write('ic07', 8); data.writeUInt32BE(entryLength, 12);
  return data;
}
function jxl(entryLength = 12) {
  return Buffer.concat([
    box('JXL ', Buffer.from([13, 10, 135, 10])),
    box('ftyp', Buffer.from('jxl \0\0\0\0jxl ', 'binary')),
    box('jxlp', Buffer.alloc(4), entryLength)
  ]);
}
function heif(entryLength = 20) {
  const dimensions = Buffer.alloc(12);
  dimensions.writeUInt32BE(123, 4); dimensions.writeUInt32BE(456, 8);
  return Buffer.concat([
    box('ftyp', Buffer.from('heic\0\0\0\0heic', 'binary')),
    box('meta', Buffer.concat([Buffer.alloc(4), box('iprp', box('ipco', box('ispe', dimensions, entryLength)))]))
  ]);
}
function entries(format) {
  if (baseline) return [['index.cjs', 'imageSize']];
  return ['cjs', 'mjs'].flatMap(ext => [
    [`index.${ext}`, 'imageSize'], [`lookup.${ext}`, 'imageSize'],
    [`fromFile.${ext}`, 'fromFile'], [`types/index.${ext}`, 'handlers'],
    [`types/${format.toLowerCase()}.${ext}`, 'leaf']
  ]);
}
for (const [format, make, oversized, undersized] of [
  ['ICNS', icns, 17, 7], ['JXL', jxl, 1000, 7], ['HEIF', heif, 1000, 7]
]) {
  for (const [name, length] of [['zero', 0], ['undersized', undersized], ['oversized', oversized]]) {
    test(`rejects ${name} ${format} entries without hanging`, () => {
      for (const [entry, mode] of entries(format)) {
        assert.equal(parse(make(length), entry, mode, format).ok, false, `${entry} accepted malformed ${format}`);
      }
    });
  }
}
test('rejects truncated ICNS entry headers and undersized JXL/HEIF payload headers', () => {
  for (const [format, input] of [['ICNS', icns().subarray(0, 12)], ['JXL', jxl(8)], ['HEIF', heif(12)]]) {
    for (const [entry, mode] of entries(format)) assert.equal(parse(input, entry, mode, format).ok, false, entry);
  }
});
test('HEIF child boxes cannot borrow dimensions outside their declared parent', () => {
  const dimensions = Buffer.alloc(12); dimensions.writeUInt32BE(123, 4); dimensions.writeUInt32BE(456, 8);
  const input = Buffer.concat([
    box('ftyp', Buffer.from('heic\0\0\0\0heic', 'binary')),
    box('meta', Buffer.concat([Buffer.alloc(4), box('iprp', Buffer.concat([box('ipco'), box('ispe', dimensions)]))]))
  ]);
  for (const [entry, mode] of entries('HEIF')) assert.equal(parse(input, entry, mode, 'HEIF').ok, false, entry);
});
const images = [
  ['sample.icns', 'ICNS', 128, 128], ['sample.heic', 'HEIF', 123, 456],
  ['sample.avif', 'HEIF', 123, 456], ['sample.jxl', 'JXL', 123, 456],
  ['1x2-flipped-big-endian.jpg', 'JPG', 1, 2]
];
for (const [file, format, width, height] of images) {
  test(`preserves upstream valid ${file} dimensions through CJS, ESM and fromFile`, () => {
    const input = fs.readFileSync(path.join(__dirname, 'fixtures', file));
    for (const [entry, mode] of entries(format)) {
      const output = parse(input, entry, mode, format);
      assert.equal(output.ok, true, `${entry}: ${output.error}`);
      const leafIcon = format === 'ICNS' && ['leaf', 'handlers'].includes(mode);
      assert.equal(output.result.width, leafIcon ? 16 : width, entry);
      assert.equal(output.result.height, leafIcon ? 16 : height, entry);
    }
  });
}
test('preserves PNG and SVG dimensions', () => {
  const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a5WQAAAAASUVORK5CYII=', 'base64');
  const svg = Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="24" height="32"></svg>');
  for (const [input, width, height] of [[png, 1, 1], [svg, 24, 32]]) {
    for (const ext of ['cjs', 'mjs']) for (const [entry, mode] of [[`index.${ext}`, 'imageSize'], [`fromFile.${ext}`, 'fromFile']]) {
      const output = parse(input, entry, mode);
      assert.equal(output.ok, true, output.error);
      assert.equal(output.result.width, width); assert.equal(output.result.height, height);
    }
  }
});
test('fromFile preserves large ICNS prefix sizing and JXL with trailing boxes', () => {
  const icon = Buffer.alloc(600 * 1024);
  icon.write('icns', 0); icon.writeUInt32BE(icon.length, 4);
  icon.write('ic07', 8); icon.writeUInt32BE(icon.length - 8, 12);
  const jxlData = Buffer.concat([fs.readFileSync(path.join(__dirname, 'fixtures', 'sample.jxl')), box('free', Buffer.alloc(600 * 1024))]);
  for (const ext of ['cjs', 'mjs']) {
    assert.equal(parse(icon, `fromFile.${ext}`, 'fromFile').result.width, 128);
    const result = parse(jxlData, `fromFile.${ext}`, 'fromFile');
    assert.equal(result.ok, true, result.error); assert.equal(result.result.width, 123); assert.equal(result.result.height, 456);
  }
});
test('retains upstream unsupported oversized JXL codestream prefix behavior', () => {
  const data = fs.readFileSync(path.join(__dirname, 'fixtures', 'sample.jxl'));
  const input = Buffer.concat([data, Buffer.alloc(600 * 1024)]);
  let offset = 0;
  while (offset < data.length) {
    const size = data.readUInt32BE(offset);
    if (['jxlc', 'jxlp'].includes(data.toString('ascii', offset + 4, offset + 8))) {
      input.writeUInt32BE(input.length - offset, offset);
      break;
    }
    offset += size;
  }
  assert.ok(offset < data.length, 'fixture must contain a codestream box');
  for (const ext of ['cjs', 'mjs']) assert.equal(parse(input, `fromFile.${ext}`, 'fromFile').ok, false);
});

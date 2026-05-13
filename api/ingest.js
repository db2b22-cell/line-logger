// LINE Logger — receives forwarded events from maintenance-dept
// Handles: text, image, file messages → Google Drive

const CHANNEL_ACCESS_TOKEN = 'zAxex+H02fBeebm6uRsJz4gYYxWk7Jxpxa+w2Hzc5XYLEFBxT1CCXT/IFkC+TYb8GkSV3IfYCXntYMZiQ6t0j7+JKpF5Lq2mGXNszncGzw/rE6xOdsnYVA7P+wFbt/c7/v8hHXXE1IAYyp+i86mUOgdB04t89/1O/w1cDnyilFU=';
const GDRIVE_CONNECTION_ID = '9d9d8ae1-7cff-44ca-b29c-3e95f9aaac7e';
const INGEST_SECRET = process.env.INGEST_SECRET || 'b652ad7b9fbc9b175a3f6c1c99406333';

const MEMBERS = [
  { id: 11, names: ['ปราโมทย์', 'ไพรวรรณ์', 'Pramot'] },
  { id: 12, names: ['สกล', 'กิจเจริญ', 'nouvo'] },
  { id: 13, names: ['บัณฑิต', 'นิลอ่อน', 'plug'] },
  { id: 7,  names: ['วิทยา', 'แพงศรี', 'หมี'] },
  { id: 4,  names: ['ศิริชัย', 'แสงวงศ์', 'aek', 'เอก'] },
  { id: 6,  names: ['อุดมชัย', 'ทศรักษา', 'Ly'] },
  { id: 9,  names: ['สนธยา', 'โจ้', 'โจ้ชาวดี'] },
  { id: 1,  names: ['ภัททิยา', 'แพท์พิพัฒน์', 'Noomotasa'] },
  { id: 2,  names: ['วันชนะ', 'ฟอร์ด'] },
  { id: 3,  names: ['วัชรินทร์', 'วงษ์ตุรัณต์', 'Beerkujiki'] },
  { id: 5,  names: ['สมชาย', 'ลิขิต', 'ลิขิตอณิเนยฬ์'] },
  { id: 8,  names: ['พิพัฒน์พล', 'เบล', 'เบลดับปลู'] },
  { id: 10, names: ['สุทิน', 'รอดยิ้ม'] },
  { id: 14, names: ['ณัฐพงษ์', 'ยะล้อม', 'ไมค์', 'nattapong'] },
];

const SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbwtslkpUh2oUtcgwE8ToA_tCueY_FHRXFepEyxIlsWap8X4YABgvJPab9dJX7C8ToZ7/exec';

// ---- Google Drive helpers ----
const folderIdCache = {};

const gdriveHeaders = () => ({
  'Authorization': `Bearer ${process.env.MATON_API_KEY}`,
  'Maton-Connection': GDRIVE_CONNECTION_ID,
});

async function gdriveSearch(q, fields = 'files(id,createdTime)') {
  const params = new URLSearchParams({ q, fields, spaces: 'drive' });
  const res = await fetch(`https://api.maton.ai/google-drive/drive/v3/files?${params}`, {
    headers: gdriveHeaders(),
  });
  if (!res.ok) return [];
  const data = await res.json();
  return data.files || [];
}

async function gdriveGetOrCreateFolder(name, parentId) {
  const key = `${parentId}::${name}`;
  if (folderIdCache[key]) return folderIdCache[key];
  const q = `name='${name}' and mimeType='application/vnd.google-apps.folder' and '${parentId}' in parents and trashed=false`;
  const files = await gdriveSearch(q);
  if (files.length > 0) {
    const winner = files.sort((a, b) => a.createdTime.localeCompare(b.createdTime))[0];
    folderIdCache[key] = winner.id;
    return winner.id;
  }
  await fetch('https://api.maton.ai/google-drive/drive/v3/files', {
    method: 'POST',
    headers: { ...gdriveHeaders(), 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] }),
  });
  const files2 = await gdriveSearch(q);
  if (!files2.length) throw new Error(`Cannot create folder ${name}`);
  const winner2 = files2.sort((a, b) => a.createdTime.localeCompare(b.createdTime))[0];
  folderIdCache[key] = winner2.id;
  return winner2.id;
}

async function gdriveGetRootId() {
  if (folderIdCache['__root__']) return folderIdCache['__root__'];
  const q = `name='Makatoon' and mimeType='application/vnd.google-apps.folder' and 'root' in parents and trashed=false`;
  const files = await gdriveSearch(q);
  if (files.length > 0) {
    const winner = files.sort((a, b) => a.createdTime.localeCompare(b.createdTime))[0];
    folderIdCache['__root__'] = winner.id;
    return winner.id;
  }
  await fetch('https://api.maton.ai/google-drive/drive/v3/files', {
    method: 'POST',
    headers: { ...gdriveHeaders(), 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'Makatoon', mimeType: 'application/vnd.google-apps.folder' }),
  });
  const files2 = await gdriveSearch(q);
  if (!files2.length) throw new Error('Cannot create Makatoon root folder');
  const winner2 = files2.sort((a, b) => a.createdTime.localeCompare(b.createdTime))[0];
  folderIdCache['__root__'] = winner2.id;
  return winner2.id;
}

async function gdriveReadText(fileId) {
  const res = await fetch(`https://api.maton.ai/google-drive/drive/v3/files/${fileId}?alt=media`, {
    headers: gdriveHeaders(),
  });
  if (!res.ok) return null;
  return await res.text();
}

function buildMultipartText(metadata, textContent) {
  const boundary = 'gdrive_boundary_maton';
  const body = [
    `--${boundary}`,
    'Content-Type: application/json; charset=UTF-8',
    '',
    JSON.stringify(metadata),
    `--${boundary}`,
    'Content-Type: text/plain; charset=UTF-8',
    '',
    textContent,
    `--${boundary}--`,
  ].join('\r\n');
  return { body, contentType: `multipart/related; boundary=${boundary}` };
}

async function gdriveWriteText(name, folderId, content, existingId = null) {
  const { body, contentType } = buildMultipartText(
    existingId ? {} : { name, parents: [folderId] },
    content
  );
  const url = existingId
    ? `https://api.maton.ai/google-drive/upload/drive/v3/files/${existingId}?uploadType=multipart`
    : 'https://api.maton.ai/google-drive/upload/drive/v3/files?uploadType=multipart';
  const method = existingId ? 'PATCH' : 'POST';
  const res = await fetch(url, {
    method,
    headers: { ...gdriveHeaders(), 'Content-Type': contentType },
    body,
  });
  if (!res.ok) {
    const err = await res.text().catch(() => '');
    throw new Error(`gdriveWriteText failed: ${res.status} ${err.slice(0, 100)}`);
  }
  return await res.json();
}

async function gdriveUploadBinary(name, folderId, buffer, mimeType) {
  const metaRes = await fetch('https://api.maton.ai/google-drive/drive/v3/files', {
    method: 'POST',
    headers: { ...gdriveHeaders(), 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, parents: [folderId] }),
  });
  if (!metaRes.ok) throw new Error(`Create metadata failed: ${metaRes.status}`);
  const { id: fileId } = await metaRes.json();
  const upRes = await fetch(`https://api.maton.ai/google-drive/upload/drive/v3/files/${fileId}?uploadType=media`, {
    method: 'PATCH',
    headers: { ...gdriveHeaders(), 'Content-Type': mimeType },
    body: buffer,
  });
  if (!upRes.ok) throw new Error(`Upload binary failed: ${upRes.status}`);
  return fileId;
}

// ---- Core logging ----
async function appendToLog(groupName, dateStr, htmlBlock) {
  const rootId = await gdriveGetRootId();
  const logsId = await gdriveGetOrCreateFolder('LINE-Logs', rootId);
  const groupFolderId = await gdriveGetOrCreateFolder(groupName, logsId);
  const fileName = `${dateStr}.md`;
  const files = await gdriveSearch(`name='${fileName}' and '${groupFolderId}' in parents and trashed=false`, 'files(id)');
  let existing = '<div class="lc">\n';
  let fileId = null;
  if (files.length > 0) {
    fileId = files[0].id;
    const content = await gdriveReadText(fileId);
    if (content) existing = content;
  }
  await gdriveWriteText(fileName, groupFolderId, existing + htmlBlock, fileId);
  console.log(`[GDRIVE] Log: ${groupName}/${dateStr}`);
}

// ---- Utilities ----
function getThaiNow() {
  const now = new Date();
  const thNow = new Date(now.getTime() + 7 * 60 * 60 * 1000);
  return {
    dateStr: thNow.toISOString().slice(0, 10),
    timeStr: thNow.toISOString().slice(11, 16),
  };
}

function getExtension(contentType, fileName) {
  if (fileName) {
    const dot = fileName.lastIndexOf('.');
    if (dot !== -1) return fileName.slice(dot);
  }
  const map = {
    'image/jpeg': '.jpg', 'image/png': '.png', 'image/gif': '.gif', 'image/webp': '.webp',
    'application/pdf': '.pdf', 'application/msword': '.doc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
    'application/vnd.ms-excel': '.xls',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
    'audio/m4a': '.m4a', 'audio/mpeg': '.mp3',
  };
  return map[contentType] || '.bin';
}

function findMemberByName(name) {
  if (!name) return null;
  for (const m of MEMBERS) {
    if (m.names.some(n => name.includes(n) || n.includes(name))) return m;
  }
  return null;
}

async function getLineDisplayName(userId, source) {
  try {
    let url;
    if (source && source.type === 'group') {
      url = `https://api.line.me/v2/bot/group/${source.groupId}/member/${userId}`;
    } else if (source && source.type === 'room') {
      url = `https://api.line.me/v2/bot/room/${source.roomId}/member/${userId}`;
    } else {
      url = `https://api.line.me/v2/bot/profile/${userId}`;
    }
    const res = await fetch(url, { headers: { 'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}` } });
    const data = await res.json();
    return data.displayName || null;
  } catch (e) {
    return null;
  }
}

async function getMemberIdFromSheets(userId) {
  try {
    const res = await fetch(`${SCRIPT_URL}?action=getUser&userId=${encodeURIComponent(userId)}`);
    const data = await res.json();
    return data.memberId ? +data.memberId : null;
  } catch (e) {
    return null;
  }
}

async function ensureProfilePic(userId, senderName, source) {
  try {
    const rootId = await gdriveGetRootId();
    const profilesId = await gdriveGetOrCreateFolder('LINE-Profiles', rootId);
    const picName = `${senderName}.jpg`;
    const existing = await gdriveSearch(`name='${picName}' and '${profilesId}' in parents and trashed=false`, 'files(id)');
    if (existing.length > 0) return;
    let profileUrl;
    if (source && source.type === 'group') {
      profileUrl = `https://api.line.me/v2/bot/group/${source.groupId}/member/${userId}`;
    } else {
      profileUrl = `https://api.line.me/v2/bot/profile/${userId}`;
    }
    const profile = await fetch(profileUrl, { headers: { 'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}` } });
    if (!profile.ok) return;
    const { pictureUrl } = await profile.json();
    if (!pictureUrl) return;
    const imgRes = await fetch(pictureUrl);
    if (!imgRes.ok) return;
    const buf = await imgRes.arrayBuffer();
    await gdriveUploadBinary(picName, profilesId, buf, 'image/jpeg');
    console.log(`[PROFILE] Saved ${senderName}`);
  } catch (e) {
    console.error(`[PROFILE] Error: ${e.message}`);
  }
}

async function processEvent(event) {
  if (event.type !== 'message') return null;
  const msgType = event.message.type;
  const messageId = event.message.id;
  const userId = event.source.userId;
  const source = event.source;
  const groupName = event.groupName || 'unknown';

  if (msgType === 'video') return null;

  if (msgType === 'image') {
    return await buildMediaEntry(messageId, 'image', null, userId, source, groupName);
  }
  if (msgType === 'file') {
    return await buildMediaEntry(messageId, 'file', event.message.fileName || null, userId, source, groupName);
  }
  if (msgType !== 'text') return null;

  let memberId = await getMemberIdFromSheets(userId).catch(() => null);
  if (!memberId) {
    const dn = await getLineDisplayName(userId, source).catch(() => null);
    if (dn) { const m = findMemberByName(dn); if (m) memberId = m.id; }
  }
  const member = MEMBERS.find(m => m.id === memberId);
  const senderName = member ? member.names[0] : userId;
  const text = event.message.text.trim();

  const { dateStr, timeStr } = getThaiNow();
  const av = `LINE-Profiles/${senderName}.jpg`;
  if (userId) ensureProfilePic(userId, senderName, source).catch(() => {});
  const html = `<div class="msg"><div class="mh"><img class="av" src="${av}"><b class="nm">${senderName}</b><span class="ts">${timeStr}</span></div><span class="ct">${text}</span></div>\n`;
  return { groupName, dateStr, html };
}

async function buildMediaEntry(messageId, messageType, fileName, userId, source, groupName) {
  const { dateStr, timeStr } = getThaiNow();
  let memberId = await getMemberIdFromSheets(userId).catch(() => null);
  if (!memberId) {
    const dn = await getLineDisplayName(userId, source).catch(() => null);
    if (dn) { const m = findMemberByName(dn); if (m) memberId = m.id; }
  }
  const member = MEMBERS.find(m => m.id === memberId);
  const senderName = member ? member.names[0] : 'unknown';
  const av = `LINE-Profiles/${senderName}.jpg`;
  if (userId) ensureProfilePic(userId, senderName, source).catch(() => {});

  const lineRes = await fetch(`https://api-data.line.me/v2/bot/message/${messageId}/content`, {
    headers: { 'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}` }
  });
  if (!lineRes.ok) {
    console.error(`[LINE] Content fetch failed: ${lineRes.status} msgId=${messageId}`);
    return null;
  }
  const contentType = lineRes.headers.get('content-type') || 'application/octet-stream';
  const ext = getExtension(contentType, fileName);
  const finalFileName = fileName || `${messageType}_${messageId}${ext}`;
  const buffer = await lineRes.arrayBuffer();

  const rootId = await gdriveGetRootId();
  const mediaRootId = await gdriveGetOrCreateFolder('LINE-Media', rootId);
  const mediaGroupId = await gdriveGetOrCreateFolder(groupName, mediaRootId);
  const mediaDayId = await gdriveGetOrCreateFolder(dateStr, mediaGroupId);

  let html;
  try {
    await gdriveUploadBinary(finalFileName, mediaDayId, buffer, contentType);
    html = `<div class="msg"><div class="mh"><img class="av" src="${av}"><b class="nm">${senderName}</b><span class="ts">${timeStr}</span></div><img src="LINE-Media/${groupName}/${dateStr}/${finalFileName}" class="ci"></div>\n`;
    console.log(`[GDRIVE] Media: ${finalFileName} in [${groupName}]`);
  } catch (e) {
    html = `<div class="msg"><div class="mh"><b class="nm">${senderName}</b><span class="ts">${timeStr}</span></div><span class="ct">[upload failed: ${e.message.slice(0,60)}]</span></div>\n`;
    console.error(`[GDRIVE] Upload error: ${e.message}`);
  }
  return { groupName, dateStr, html };
}

// ---- Main handler ----
export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const auth = req.headers['x-ingest-secret'];
  if (auth !== INGEST_SECRET) {
    console.error('[INGEST] Unauthorized');
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const body = req.body || {};
  const events = body.events || [];

  const entries = await Promise.all(events.map(e => processEvent(e).catch(err => {
    console.error('[EVENT] Error:', err.message);
    return null;
  })));

  const batches = {};
  for (const entry of entries.filter(Boolean)) {
    const key = `${entry.groupName}:::${entry.dateStr}`;
    if (!batches[key]) batches[key] = { groupName: entry.groupName, dateStr: entry.dateStr, html: '' };
    batches[key].html += entry.html;
  }
  const driveErrors = [];
  for (const b of Object.values(batches)) {
    try {
      await appendToLog(b.groupName, b.dateStr, b.html);
    } catch (e) {
      driveErrors.push(e.message);
    }
  }

  const envCheck = process.env.MATON_API_KEY ? `key_len:${process.env.MATON_API_KEY.length}` : 'NO_KEY';
  return res.status(200).json({ success: true, debug: envCheck, batchCount: Object.keys(batches).length, driveErrors });
}

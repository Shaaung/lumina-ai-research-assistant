/** Document indexing worker. Run separately with `npm run worker`. */
import { hostname } from 'node:os';
import { GridFSBucket, ObjectId, type Collection } from 'mongodb';
import pino from 'pino';
import { getDocument } from 'pdfjs-dist/legacy/build/pdf.mjs';
import { env, secrets } from './env.js';
import { db } from './db.js';

const log = pino({ level: env.logLevel });
const workerId = `${hostname()}:${process.pid}`;
const STALE_AFTER_MS = 5 * 60 * 1000;
const MAX_ATTEMPTS = 3;
const CHUNK_CHARS = 1400;
const CHUNK_OVERLAP = 220;

type JobDoc = {
  _id: string;
  kind: 'index_document';
  status: 'pending' | 'running' | 'done' | 'failed';
  payload: { docId: string; spaceId: string; fileId: string; mimeType: string };
  userId: string;
  claimedAt?: Date;
  workerId?: string;
  attempts: number;
  error?: string;
  createdAt: Date;
};
type DocumentDoc = {
  _id: string;
  spaceId: string;
  userId: string;
  title: string;
  mimeType: string;
  bytes: number;
  status: 'pending' | 'parsing' | 'embedding' | 'indexed' | 'failed';
  pct: number;
  pages?: number;
  chunks?: number;
  error?: string;
  fileId: string;
  createdAt: Date;
};
type Locator = { page?: number; heading?: string; line?: number };
type ParsedChunk = { text: string; locator: Locator; ord: number };
type ChunkDoc = ParsedChunk & {
  _id: string;
  docId: string;
  spaceId: string;
  userId: string;
  embedding: number[];
  createdAt: Date;
};

const wait = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function downloadGridFsFile(fileId: string): Promise<Buffer> {
  const database = await db();
  const bucket = new GridFSBucket(database, { bucketName: 'uploads' });
  const pieces: Buffer[] = [];
  for await (const piece of bucket.openDownloadStream(new ObjectId(fileId))) {
    pieces.push(Buffer.isBuffer(piece) ? piece : Buffer.from(piece));
  }
  return Buffer.concat(pieces);
}

function splitText(text: string, locator: Locator, startOrd: number): ParsedChunk[] {
  const clean = text.split('\u0000').join('').replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
  if (!clean) return [];
  const chunks: ParsedChunk[] = [];
  let start = 0;
  let ord = startOrd;
  while (start < clean.length) {
    let end = Math.min(start + CHUNK_CHARS, clean.length);
    if (end < clean.length) {
      const naturalBreak = Math.max(
        clean.lastIndexOf('\n\n', end), clean.lastIndexOf('. ', end),
        clean.lastIndexOf('? ', end), clean.lastIndexOf('! ', end)
      );
      if (naturalBreak > start + Math.floor(CHUNK_CHARS * 0.55)) end = naturalBreak + 1;
    }
    const chunkText = clean.slice(start, end).trim();
    if (chunkText) chunks.push({ text: chunkText, locator, ord: ord++ });
    if (end >= clean.length) break;
    start = Math.max(end - CHUNK_OVERLAP, start + 1);
  }
  return chunks;
}

async function parsePdf(buffer: Buffer): Promise<{ chunks: ParsedChunk[]; pages: number }> {
  const pdf = await getDocument({ data: new Uint8Array(buffer) }).promise;
  const chunks: ParsedChunk[] = [];
  try {
    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber++) {
      const page = await pdf.getPage(pageNumber);
      const content = await page.getTextContent();
      const text = content.items
        .map((item) => ('str' in item ? item.str : ''))
        .join(' ');
      const pageChunks = splitText(text, { page: pageNumber }, chunks.length).map((chunk, index) => ({
        ...chunk,
        // A PDF page can produce several chunks. Keep their citation locators distinct
        // so the evaluator and UI can map each citation back to the exact passage.
        locator: { ...chunk.locator, line: index + 1 }
      }));
      chunks.push(...pageChunks);
      page.cleanup();
    }
    return { chunks, pages: pdf.numPages };
  } finally {
    await pdf.destroy();
  }
}

function parseText(buffer: Buffer, mimeType: string): { chunks: ParsedChunk[] } {
  const text = buffer.toString('utf8').replace(/^\uFEFF/, '');
  if (!text.trim()) return { chunks: [] };
  if (mimeType !== 'text/markdown') return { chunks: splitText(text, { line: 1 }, 0) };
  const lines = text.split(/\r?\n/);
  const sections: Array<{ heading?: string; line: number; text: string }> = [];
  let current = { line: 1, text: '' } as { heading?: string; line: number; text: string };
  for (let index = 0; index < lines.length; index++) {
    const heading = lines[index]!.match(/^#{1,6}\s+(.+?)\s*$/)?.[1]?.trim();
    if (heading) {
      if (current.text.trim()) sections.push(current);
      current = { heading, line: index + 1, text: lines[index]! };
    } else {
      current.text += `${current.text ? '\n' : ''}${lines[index]}`;
    }
  }
  if (current.text.trim()) sections.push(current);
  const chunks: ParsedChunk[] = [];
  for (const section of sections) {
    const locator = section.heading ? { heading: section.heading, line: section.line } : { line: section.line };
    chunks.push(...splitText(section.text, locator, chunks.length));
  }
  return { chunks };
}

async function embedBatch(inputs: string[]): Promise<{ embeddings: number[][]; tokens: number; costUsd: number }> {
  if (!secrets.openai) throw new Error('OPENAI_API_KEY is not set');
  const response = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${secrets.openai}` },
    body: JSON.stringify({ model: env.embeddingModel, input: inputs, dimensions: 1536 }),
    signal: AbortSignal.timeout(60000)
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({})) as { error?: { code?: string; type?: string } };
    const code = body.error?.code ?? body.error?.type ?? 'unknown_error';
    throw new Error(`OpenAI embedding request failed (${response.status}, ${code})`);
  }
  const body = await response.json() as {
    data?: Array<{ index?: number; embedding?: number[] }>;
    usage?: { prompt_tokens?: number; total_tokens?: number };
  };
  const ordered = [...(body.data ?? [])].sort((a, b) => (a.index ?? 0) - (b.index ?? 0));
  const embeddings = ordered.map((item) => item.embedding ?? []);
  if (embeddings.length !== inputs.length || embeddings.some((embedding) => embedding.length !== 1536)) {
    throw new Error('OpenAI returned an invalid embedding batch');
  }
  const tokens = body.usage?.prompt_tokens ?? body.usage?.total_tokens ?? 0;
  const usdPerMillion = env.embeddingModel === 'text-embedding-3-small' ? 0.02 : 0;
  return { embeddings, tokens, costUsd: tokens * usdPerMillion / 1_000_000 };
}

async function embedAndUpsert(
  chunks: ParsedChunk[], document: DocumentDoc, documents: Collection<DocumentDoc>,
  chunkCollection: Collection<ChunkDoc>
): Promise<{ firstChunk: ChunkDoc; tokens: number; costUsd: number }> {
  let processed = 0;
  let tokens = 0;
  let costUsd = 0;
  let firstChunk: ChunkDoc | undefined;
  for (let start = 0; start < chunks.length; start += 64) {
    const batch = chunks.slice(start, start + 64);
    const embedded = await embedBatch(batch.map((chunk) => chunk.text));
    tokens += embedded.tokens;
    costUsd += embedded.costUsd;
    const now = new Date();
    const docs = batch.map((chunk, index): ChunkDoc => ({
      _id: `${document._id}_${chunk.ord}`,
      docId: document._id,
      spaceId: document.spaceId,
      userId: document.userId,
      text: chunk.text,
      locator: chunk.locator,
      ord: chunk.ord,
      embedding: embedded.embeddings[index]!,
      createdAt: now
    }));
    if (!firstChunk) firstChunk = docs[0];
    await chunkCollection.bulkWrite(docs.map((chunk) => ({
      replaceOne: { filter: { _id: chunk._id }, replacement: chunk, upsert: true }
    })));
    processed += batch.length;
    await documents.updateOne(
      { _id: document._id, userId: document.userId },
      { $set: { pct: 20 + Math.round((processed / chunks.length) * 70) } }
    );
  }
  if (!firstChunk) throw new Error('document contained no readable text');
  return { firstChunk, tokens, costUsd };
}

async function probeIndexed(chunk: ChunkDoc, collection: Collection<ChunkDoc>): Promise<void> {
  if (env.vectorBackend === 'mongo-cosine-scan') {
    if (!(await collection.findOne({ _id: chunk._id, userId: chunk.userId, spaceId: chunk.spaceId }))) {
      throw new Error('read-your-write probe could not find the stored chunk');
    }
    return;
  }
  for (let attempt = 1; attempt <= 15; attempt++) {
    try {
      const rows = await collection.aggregate<{ _id: string }>([
        {
          $vectorSearch: {
            index: 'chunks_vector', path: 'embedding', queryVector: chunk.embedding,
            numCandidates: 100, limit: 20,
            filter: { spaceId: { $eq: chunk.spaceId }, userId: { $eq: chunk.userId } }
          }
        },
        { $project: { _id: 1 } }
      ]).toArray();
      if (rows.some((row) => row._id === chunk._id)) return;
    } catch (error) {
      if (attempt === 15) throw error;
    }
    await wait(2000);
  }
  throw new Error('Atlas vector index did not expose the new document within 30 seconds');
}

async function processJob(job: JobDoc): Promise<void> {
  const database = await db();
  const documents = database.collection<DocumentDoc>('documents');
  const chunks = database.collection<ChunkDoc>('chunks');
  const jobs = database.collection<JobDoc>('jobs');
  const document = await documents.findOne({ _id: job.payload.docId, userId: job.userId, spaceId: job.payload.spaceId });
  if (!document) throw new Error('target document no longer exists');

  let firstChunk: ChunkDoc | undefined;
  let totalTokens = 0;
  let totalCostUsd = 0;
  const existingCount = await chunks.countDocuments({ docId: document._id, userId: document.userId });
  if (document.status === 'embedding' && document.chunks && existingCount === document.chunks) {
    firstChunk = await chunks.findOne({ docId: document._id, userId: document.userId }, { sort: { ord: 1 } }) ?? undefined;
  } else {
    await documents.updateOne(
      { _id: document._id, userId: document.userId },
      { $set: { status: 'parsing', pct: 5 }, $unset: { error: '' } }
    );
    const file = await downloadGridFsFile(document.fileId);
    const parsed = document.mimeType === 'application/pdf' ? await parsePdf(file) : parseText(file, document.mimeType);
    if (!parsed.chunks.length) throw new Error('document contained no readable text');
    const pages = 'pages' in parsed && typeof parsed.pages === 'number' ? parsed.pages : undefined;
    await documents.updateOne(
      { _id: document._id, userId: document.userId },
      { $set: { status: 'embedding', pct: 20, chunks: parsed.chunks.length, ...(pages ? { pages } : {}) } }
    );
    await chunks.deleteMany({ docId: document._id, userId: document.userId });
    const embedded = await embedAndUpsert(parsed.chunks, document, documents, chunks);
    firstChunk = embedded.firstChunk;
    totalTokens = embedded.tokens;
    totalCostUsd = embedded.costUsd;
  }

  if (!firstChunk) throw new Error('document has no indexed chunks');
  await documents.updateOne({ _id: document._id, userId: document.userId }, { $set: { pct: 95 } });
  await probeIndexed(firstChunk, chunks);
  await documents.updateOne(
    { _id: document._id, userId: document.userId },
    { $set: { status: 'indexed', pct: 100 }, $unset: { error: '' } }
  );
  await jobs.updateOne(
    { _id: job._id, workerId },
    { $set: { status: 'done' }, $unset: { error: '', claimedAt: '', workerId: '' } }
  );
  log.info(
    { jobId: job._id, docId: document._id, chunks: document.chunks, embeddingTokens: totalTokens, embeddingCostUsd: totalCostUsd },
    'document indexed'
  );
}

async function sweepStaleJobs(jobs: Collection<JobDoc>): Promise<void> {
  const staleBefore = new Date(Date.now() - STALE_AFTER_MS);
  const retried = await jobs.updateMany(
    { status: 'running', claimedAt: { $lt: staleBefore }, attempts: { $lt: MAX_ATTEMPTS } },
    { $set: { status: 'pending' }, $unset: { claimedAt: '', workerId: '', error: '' } }
  );
  const exhausted = await jobs.updateMany(
    { status: 'running', claimedAt: { $lt: staleBefore }, attempts: { $gte: MAX_ATTEMPTS } },
    { $set: { status: 'failed', error: 'worker stopped repeatedly while processing this job' }, $unset: { claimedAt: '', workerId: '' } }
  );
  if (retried.modifiedCount || exhausted.modifiedCount) {
    log.warn({ retried: retried.modifiedCount, failed: exhausted.modifiedCount }, 'recovered stale document jobs');
  }
}

async function main(): Promise<void> {
  if (!secrets.openai) throw new Error('OPENAI_API_KEY is required by the document worker');
  const database = await db();
  const jobs = database.collection<JobDoc>('jobs');
  const documents = database.collection<DocumentDoc>('documents');
  log.info({ workerId, embeddingModel: env.embeddingModel, vectorBackend: env.vectorBackend }, 'document worker started');
  let lastSweep = 0;
  while (true) {
    if (Date.now() - lastSweep > 30000) {
      await sweepStaleJobs(jobs);
      lastSweep = Date.now();
    }
    const job = await jobs.findOneAndUpdate(
      { kind: 'index_document', status: 'pending', attempts: { $lt: MAX_ATTEMPTS } },
      { $set: { status: 'running', claimedAt: new Date(), workerId }, $inc: { attempts: 1 } },
      { sort: { createdAt: 1 }, returnDocument: 'after' }
    );
    if (!job) {
      await wait(1000);
      continue;
    }
    try {
      await processJob(job);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'unknown document indexing failure';
      log.error({ jobId: job._id, docId: job.payload.docId, error: message }, 'document indexing failed');
      await Promise.all([
        jobs.updateOne(
          { _id: job._id, workerId },
          { $set: { status: 'failed', error: message }, $unset: { claimedAt: '', workerId: '' } }
        ),
        documents.updateOne(
          { _id: job.payload.docId, userId: job.userId },
          { $set: { status: 'failed', error: message } }
        )
      ]);
    }
  }
}

void main().catch((error) => {
  log.fatal({ error: error instanceof Error ? error.message : String(error) }, 'document worker stopped');
  process.exit(1);
});

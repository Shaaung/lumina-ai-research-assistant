/**
 * LUMINA agent service — the AI backend. PROVIDED SKELETON: YOU BUILD THIS OUT.
 * This is where the real work is. Provider keys live only in this process.
 *
 * What is already here: the server, /health (Mongo ping + which model, provider and
 * vector backend are live), and a 501 for every other route.
 *
 * What you build (README Part 1, in this order — each step is testable with curl -N):
 *   1. the QUICK loop: plan → choose tool → observe → repeat → answer, with web_search
 *      and fetch_page, streaming trace → sources → token → done. sources BEFORE the
 *      first token. Disable compression on this route and flush after every event.
 *   2. the search cache: in-process LRU over the searchCache collection (TTL index),
 *      key = sha256(normalized query + provider). searchCached only when every hit.
 *   3. threads + messages, so a follow-up sees the thread.
 *   4. memory: save_memory / recall_memory over the memories vector index; GET /memory,
 *      DELETE /memory/:id.
 *   5. the run log: one runs/<requestId>.json per answer, in the RunLog shape from the
 *      contract. Ten lines. The gates read it, so it is not optional.
 *   6. spaces + the jobs worker: upload → GridFS → parse → chunk → embed → upsert →
 *      read-your-write probe → indexed.
 *   7. hybrid retrieval: $vectorSearch + $search fused with RRF, page locators.
 *   8. DEEP search (depth: "deep"): plan_research decomposes the question into 3–6
 *      sub-questions, you stream a `plan` event BEFORE retrieving anything, research each
 *      sub-question, then merge the results into ONE citation numbering and synthesise.
 *      Every trace step and every source carries the subQuestion it served. Deep runs
 *      under the wider caps (maxToolCallsDeep, maxWallClockSecDeep) and behind
 *      DEEP_DAILY_CAP → 429 {error, resetsAt}.
 *
 * Three rules to hold on to while you write it:
 *   - Fail loud. A provider exception ends the run with terminated:"error" and a 502.
 *     Never a try/catch that returns a plausible answer. (Live Translate served English
 *     for weeks because of exactly that catch.)
 *   - Grounded or nothing. A citation that does not resolve to something retrieved in
 *     THIS request is an automatic fail.
 *   - Depth is opted into, never drifted into. A quick search may not call plan_research,
 *     however much the model would like to. Deep costs several times more, and a product
 *     that escalates itself is a product with an unbounded bill.
 */
import express from 'express';
import pino from 'pino';
import { createHash } from 'node:crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import { mkdirSync, writeFileSync } from 'node:fs';
import { Readable } from 'node:stream';
import { finished } from 'node:stream/promises';
import { Readability } from '@mozilla/readability';
import { JSDOM } from 'jsdom';
import multer from 'multer';
import { GridFSBucket, ObjectId } from 'mongodb';
import {
  ACCEPTED_UPLOAD_TYPES,
  AnswerId,
  AskBody,
  CreateSpaceBody,
  CreateSpaceResponse,
  CreateThreadBody,
  CreateThreadResponse,
  GetThreadResponse,
  HealthResponse,
  ListDocumentsResponse,
  ListMemoryResponse,
  ListSpacesResponse,
  ListThreadsResponse,
  MAX_UPLOAD_BYTES,
  newId,
  PlanEvent,
  ROUTES,
  Source,
  StatsResponse,
  UploadDocumentResponse,
  unresolvedCitations
} from '@lumina/contract';
import { env, secrets } from './env.js';
import { db, pingDb } from './db.js';

const log = pino({ level: env.logLevel });
const app = express();

type ThreadDoc = { _id: string; userId: string; title: string; createdAt: Date };
type MessageDoc = {
  threadId: string;
  userId: string;
  role: 'user' | 'assistant';
  content: string;
  sources?: Source[];
  answerId?: string;
  done?: Record<string, unknown>;
  createdAt: Date;
};
type MemoryRowDoc = {
  _id: string;
  userId: string;
  text: string;
  embedding: number[];
  sourceThread?: string;
  createdAt: Date;
};
type ToolTrace = {
  step: number;
  tool: 'web_search' | 'fetch_page' | 'search_documents' | 'recall_memory' | 'save_memory' | 'plan_research';
  input: Record<string, unknown>;
  ok: boolean;
  ms: number;
  reason?: string;
  error?: string;
  subQuestion?: number;
};
type SpaceRowDoc = { _id: string; userId: string; name: string; createdAt: Date };
type DocumentRowDoc = {
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
type JobRowDoc = {
  _id: string;
  kind: 'index_document';
  status: 'pending' | 'running' | 'done' | 'failed';
  payload: { docId: string; spaceId: string; fileId: string; mimeType: string };
  userId: string;
  attempts: number;
  createdAt: Date;
};
type DeepUsageDoc = {
  _id: string;
  userId: string;
  day: string;
  count: number;
  createdAt: Date;
  updatedAt: Date;
};

// Request-scoped cancellation reaches provider calls without sharing signals
// between concurrent users. Closing a failed/timed-out proxy request stops work.
const askCancellation = new AsyncLocalStorage<AbortSignal>();
function providerSignal(timeoutMs: number): AbortSignal {
  const requestSignal = askCancellation.getStore();
  return requestSignal
    ? AbortSignal.any([requestSignal, AbortSignal.timeout(timeoutMs)])
    : AbortSignal.timeout(timeoutMs);
}

app.disable('x-powered-by');
app.use((req, res, next) => {
  if (req.method !== 'POST' || !req.path.endsWith('/ask')) return next();
  const controller = new AbortController();
  const onClose = () => {
    if (!res.writableFinished) controller.abort(new Error('Ask caller disconnected'));
  };
  res.once('close', onClose);
  res.once('finish', () => res.off('close', onClose));
  askCancellation.run(controller.signal, next);
});
app.use((req, res, next) =>
  req.path.endsWith('/documents') && req.method === 'POST'
    ? next()
    : express.json({ limit: '1mb' })(req, res, next)
);

mkdirSync(env.runsDir, { recursive: true });
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_UPLOAD_BYTES } });

// A bucket caches successful GridFS index checks. Reusing it avoids repeating
// those database round trips for every upload; streams are still per-file.
let uploadsBucket: GridFSBucket | undefined;
function getUploadsBucket(database: Awaited<ReturnType<typeof db>>): GridFSBucket {
  uploadsBucket ??= new GridFSBucket(database, { bucketName: 'uploads' });
  return uploadsBucket;
}

let uploadsReady: Promise<GridFSBucket> | undefined;

/** Prepare the driver's first-use index check without storing a placeholder file. */
function prepareUploadsBucket(database: Awaited<ReturnType<typeof db>>): Promise<GridFSBucket> {
  if (!uploadsReady) {
    const bucket = getUploadsBucket(database);
    uploadsReady = new Promise<GridFSBucket>((resolve, reject) => {
      // Register before construction: the public INDEX event signals readiness.
      // Never write or end this stream, so no chunks or files row is inserted.
      const onIndex = () => {
        clearTimeout(timer);
        stream.destroy();
        resolve(bucket);
      };
      const onFailure = (error: Error) => {
        clearTimeout(timer);
        bucket.off(GridFSBucket.INDEX, onIndex);
        stream.destroy();
        reject(error);
      };
      bucket.once(GridFSBucket.INDEX, onIndex);
      const stream = bucket.openUploadStream('__index_readiness__', { timeoutMS: 8000 });
      stream.once('error', onFailure);
      const timer = setTimeout(() => onFailure(new Error('GridFS index preparation timed out')), 9000);
    }).catch(error => {
      // The driver may mark a failed first attempt as started. Use a fresh
      // bucket on retry rather than trusting that instance's internal state.
      uploadsReady = undefined;
      uploadsBucket = undefined;
      throw error;
    });
  }
  return uploadsReady;
}


// ---------------------------------------------------------------- /health (implemented)

app.get('/health', async (_req, res) => {
  const dbStatus = await pingDb();
  const body: HealthResponse = {
    status: dbStatus === 'ok' ? 'ok' : 'degraded',
    model: `${env.quickLlmModel} (quick) / ${env.llmModel} (deep)`,
    searchProvider: env.searchProvider,
    vectorStore: env.vectorBackend,
    db: dbStatus,
    ai: { status: 'ok' }
  };
  res.status(dbStatus === 'ok' ? 200 : 503).json(body);
});

// ---------------------------------------------------------------- create thread (no provider calls)

app.post('/threads', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) {
    return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  }

  const parsedBody = CreateThreadBody.safeParse(req.body ?? {});
  if (!parsedBody.success) {
    return res.status(400).json({ error: 'invalid request body', status: 400 });
  }

  const threadId = newId('thr');
  const title = parsedBody.data.title ?? 'New conversation';

  try {
    await (await db()).collection<ThreadDoc>('threads').insertOne({
      _id: threadId,
      userId,
      title,
      createdAt: new Date()
    });
    return res.status(201).json(CreateThreadResponse.parse({ threadId }));
  } catch (error) {
    log.error(
      { userId, threadId, errorName: error instanceof Error ? error.name : 'unknown' },
      'failed to create thread'
    );
    return res.status(502).json({ error: 'failed to create thread', status: 502 });
  }
});

app.get('/threads', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  try {
    const rows = await (await db()).collection<ThreadDoc>('threads')
      .find({ userId })
      .sort({ createdAt: -1 })
      .limit(100)
      .toArray();
    return res.json(ListThreadsResponse.parse({
      threads: rows.map((row) => ({ threadId: row._id, title: row.title, createdAt: row.createdAt.toISOString() }))
    }));
  } catch (error) {
    log.error({ userId, errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to list threads');
    return res.status(502).json({ error: 'failed to list threads', status: 502 });
  }
});

app.get('/threads/:threadId', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  try {
    const database = await db();
    const thread = await database.collection<ThreadDoc>('threads').findOne({ _id: req.params.threadId, userId });
    if (!thread) return res.status(404).json({ error: 'thread not found', status: 404 });
    const messages = await database.collection<MessageDoc>('messages')
      .find({ threadId: thread._id, userId })
      .sort({ createdAt: 1 })
      .toArray();
    return res.json(GetThreadResponse.parse({
      threadId: thread._id,
      title: thread.title,
      messages: messages.map((message) => ({
        role: message.role,
        content: message.content,
        ...(message.sources ? { sources: message.sources } : {}),
        ...(message.answerId ? { answerId: message.answerId } : {}),
        ...(message.done ? { done: message.done } : {}),
        createdAt: message.createdAt.toISOString()
      }))
    }));
  } catch (error) {
    log.error({ userId, threadId: req.params.threadId, errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to read thread');
    return res.status(502).json({ error: 'failed to read thread', status: 502 });
  }
});

// ---------------------------------------------------------------- long-term memory

app.get('/memory', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  try {
    const rows = await (await db()).collection<MemoryRowDoc>('memories')
      .find({ userId })
      .sort({ createdAt: -1 })
      .limit(100)
      .toArray();
    return res.json(ListMemoryResponse.parse({
      memories: rows.map((row) => ({
        id: row._id,
        text: row.text,
        ...(row.sourceThread ? { sourceThread: row.sourceThread } : {}),
        createdAt: row.createdAt.toISOString()
      }))
    }));
  } catch (error) {
    log.error({ userId, errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to list memory');
    return res.status(502).json({ error: 'failed to list memory', status: 502 });
  }
});

app.delete('/memory/:memoryId', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  try {
    const result = await (await db()).collection<MemoryRowDoc>('memories')
      .deleteOne({ _id: req.params.memoryId, userId });
    if (!result.deletedCount) return res.status(404).json({ error: 'memory not found', status: 404 });
    return res.status(204).end();
  } catch (error) {
    log.error({ userId, memoryId: req.params.memoryId, errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to delete memory');
    return res.status(502).json({ error: 'failed to delete memory', status: 502 });
  }
});

// ---------------------------------------------------------------- spaces & document intake

app.post('/spaces', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  const parsedBody = CreateSpaceBody.safeParse(req.body ?? {});
  if (!parsedBody.success) return res.status(400).json({ error: 'invalid request body', status: 400 });

  const spaceId = newId('spc');
  try {
    await (await db()).collection<SpaceRowDoc>('spaces').insertOne({
      _id: spaceId,
      userId,
      name: parsedBody.data.name.trim(),
      createdAt: new Date()
    });
    return res.status(201).json(CreateSpaceResponse.parse({ spaceId, name: parsedBody.data.name.trim() }));
  } catch (error) {
    log.error({ userId, spaceId, errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to create space');
    return res.status(502).json({ error: 'failed to create space', status: 502 });
  }
});

app.get('/spaces', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  try {
    const rows = await (await db()).collection<SpaceRowDoc>('spaces')
      .find({ userId })
      .sort({ createdAt: -1 })
      .limit(100)
      .toArray();
    return res.json(ListSpacesResponse.parse({
      spaces: rows.map((row) => ({ spaceId: row._id, name: row.name, createdAt: row.createdAt.toISOString() }))
    }));
  } catch (error) {
    log.error({ userId, errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to list spaces');
    return res.status(502).json({ error: 'failed to list spaces', status: 502 });
  }
});

app.get('/spaces/:spaceId/documents', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  try {
    const database = await db();
    const space = await database.collection<SpaceRowDoc>('spaces').findOne({ _id: req.params.spaceId, userId });
    if (!space) return res.status(404).json({ error: 'space not found', status: 404 });
    const rows = await database.collection<DocumentRowDoc>('documents')
      .find({ spaceId: space._id, userId })
      .sort({ createdAt: -1 })
      .limit(100)
      .toArray();
    return res.json(ListDocumentsResponse.parse({
      documents: rows.map((row) => ({
        docId: row._id,
        title: row.title,
        status: row.status,
        pct: row.pct,
        ...(row.pages ? { pages: row.pages } : {}),
        ...(row.chunks !== undefined ? { chunks: row.chunks } : {}),
        ...(row.error ? { error: row.error } : {})
      }))
    }));
  } catch (error) {
    log.error({ userId, spaceId: req.params.spaceId, errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to list documents');
    return res.status(502).json({ error: 'failed to list documents', status: 502 });
  }
});

app.post('/spaces/:spaceId/documents', (req, res) => {
  const intakeStarted = Date.now();
  const intakeUserId = req.get('x-user-id')?.trim();
  if (!intakeUserId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  // Start ownership lookup while multipart bytes are being received. Capture
  // rejection immediately; malformed uploads must not leak an unhandled promise.
  const lookupStarted = Date.now();
  const spaceLookup = db().then(async (database) => ({
    database,
    space: await database.collection<SpaceRowDoc>('spaces').findOne({ _id: req.params.spaceId, userId: intakeUserId })
  })).then(value => ({ ok: true as const, value, ms: Date.now() - lookupStarted }),
    error => ({ ok: false as const, error, ms: Date.now() - lookupStarted }));
  upload.single('file')(req, res, async (uploadError) => {
    const userId = req.get('x-user-id')?.trim();
    if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
    if (uploadError instanceof multer.MulterError && uploadError.code === 'LIMIT_FILE_SIZE') {
      return res.status(413).json({ error: 'file exceeds the 25 MB upload limit', status: 413 });
    }
    if (uploadError) return res.status(400).json({ error: 'invalid multipart upload', status: 400 });
    if (!req.file) return res.status(400).json({ error: 'multipart field "file" is required', status: 400 });

    const lowerName = req.file.originalname.toLowerCase();
    const inferredMime = lowerName.endsWith('.pdf')
      ? 'application/pdf'
      : lowerName.endsWith('.md')
        ? 'text/markdown'
        : lowerName.endsWith('.txt')
          ? 'text/plain'
          : '';
    const mimeType = ACCEPTED_UPLOAD_TYPES.includes(req.file.mimetype as typeof ACCEPTED_UPLOAD_TYPES[number])
      ? req.file.mimetype
      : inferredMime;
    if (!ACCEPTED_UPLOAD_TYPES.includes(mimeType as typeof ACCEPTED_UPLOAD_TYPES[number])) {
      return res.status(400).json({ error: 'only PDF, Markdown, and plain-text files are accepted', status: 400 });
    }

    const docId = newId('doc');
    const jobId = newId('req');
    let fileId: ObjectId | undefined;
    const multipartMs = Date.now() - intakeStarted;
    const persistenceTiming: { gridFsMs?: number; documentInsertMs?: number } = {};
    try {
      const ownership = await spaceLookup;
      if (!ownership.ok) throw ownership.error;
      const { database, space } = ownership.value;
      if (!space) return res.status(404).json({ error: 'space not found', status: 404 });
      const storageStarted = Date.now();

      const preparationStarted = Date.now();
      const bucket = await prepareUploadsBucket(database);
      const preparationMs = Date.now() - preparationStarted;
      const stream = bucket.openUploadStream(req.file.originalname, {
        metadata: { userId, spaceId: space._id, docId, mimeType }
      });
      fileId = stream.id;
      const fileStarted = Date.now();
      const fileStored = finished(stream).finally(() => {
        persistenceTiming.gridFsMs = Date.now() - fileStarted;
      });
      Readable.from(req.file.buffer).pipe(stream);

      const createdAt = new Date();
      // File storage and pending metadata are independent. Wait for BOTH before
      // publishing the job, so the worker never sees an incomplete upload.
      const documentStarted = Date.now();
      const documentStored = database.collection<DocumentRowDoc>('documents').insertOne({
        _id: docId,
        spaceId: space._id,
        userId,
        title: req.file.originalname,
        mimeType,
        bytes: req.file.size,
        status: 'pending',
        pct: 0,
        fileId: fileId.toHexString(),
        createdAt
      }).finally(() => {
        persistenceTiming.documentInsertMs = Date.now() - documentStarted;
      });
      const stored = await Promise.allSettled([fileStored, documentStored]);
      for (const result of stored) {
        if (result.status === 'rejected') throw result.reason;
      }
      const storageMs = Date.now() - storageStarted;
      const jobStarted = Date.now();
      await database.collection<JobRowDoc>('jobs').insertOne({
        _id: jobId,
        kind: 'index_document',
        status: 'pending',
        payload: { docId, spaceId: space._id, fileId: fileId.toHexString(), mimeType },
        userId,
        attempts: 0,
        createdAt
      });
      log.info({ requestId: req.get('x-request-id'), userId, docId, ownershipMs: ownership.ms,
        multipartMs, preparationMs, ...persistenceTiming, storageMs, jobMs: Date.now() - jobStarted,
        acceptMs: Date.now() - intakeStarted }, 'document intake completed');
      return res.status(202).json(UploadDocumentResponse.parse({ docId, status: 'pending' }));
    } catch (error) {
      log.error({ requestId: req.get('x-request-id'), userId, spaceId: req.params.spaceId,
        docId, multipartMs, ...persistenceTiming, elapsedMs: Date.now() - intakeStarted,
        errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to accept document');
      try {
        const database = await db();
        await Promise.all([
          database.collection<DocumentRowDoc>('documents').deleteOne({ _id: docId, userId }),
          database.collection<JobRowDoc>('jobs').deleteOne({ _id: jobId, userId }),
          ...(fileId ? [getUploadsBucket(database).delete(fileId)] : [])
        ]);
      } catch {
        log.warn({ userId, docId }, 'failed to clean up partial document upload');
      }
      return res.status(502).json({ error: 'failed to store document', status: 502 });
    }
  });
});

type EmbeddingResult = { embedding: number[]; inputTokens: number; costUsd: number };

async function embedText(text: string): Promise<EmbeddingResult> {
  if (!secrets.openai) throw new Error('OPENAI_API_KEY is not set');
  const response = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      authorization: `Bearer ${secrets.openai}`
    },
    body: JSON.stringify({ model: env.embeddingModel, input: text, dimensions: 1536 }),
    signal: providerSignal(30000)
  });
  if (!response.ok) {
    const errorBody = await response.json().catch(() => ({})) as {
      error?: { code?: string; type?: string };
    };
    const code = errorBody.error?.code ?? errorBody.error?.type ?? 'unknown_error';
    const explanations: Record<string, string> = {
      credit_balance_exhausted: 'API credit balance is exhausted',
      insufficient_quota: 'API quota or billing limit is exhausted',
      organization_usage_limit_exceeded: 'organization usage limit is exhausted',
      organization_spend_limit_exceeded: 'organization spend limit is exhausted',
      project_spend_limit_exceeded: 'project spend limit is exhausted',
      rate_limit_exceeded: 'temporary request or token rate limit exceeded'
    };
    throw new Error(
      `OpenAI embedding request failed (${response.status}, ${code}): ${explanations[code] ?? 'check the OpenAI API billing and limits pages'}`
    );
  }
  const body = (await response.json()) as {
    data?: Array<{ embedding?: number[] }>;
    usage?: { prompt_tokens?: number; total_tokens?: number };
  };
  const embedding = body.data?.[0]?.embedding;
  if (!embedding || embedding.length !== 1536) {
    throw new Error(`OpenAI returned an invalid embedding (${embedding?.length ?? 0} dimensions)`);
  }
  const inputTokens = body.usage?.prompt_tokens ?? body.usage?.total_tokens ?? 0;
  const usdPerMillionTokens = env.embeddingModel === 'text-embedding-3-small' ? 0.02 : 0;
  return { embedding, inputTokens, costUsd: inputTokens * usdPerMillionTokens / 1_000_000 };
}

function cosineSimilarity(a: number[], b: number[]): number {
  let dot = 0;
  let aa = 0;
  let bb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i]! * b[i]!;
    aa += a[i]! * a[i]!;
    bb += b[i]! * b[i]!;
  }
  return aa && bb ? dot / Math.sqrt(aa * bb) : 0;
}

async function recallMemory(
  userId: string,
  query: string
): Promise<{ memories: MemoryRowDoc[]; inputTokens: number; costUsd: number }> {
  const collection = (await db()).collection<MemoryRowDoc>('memories');
  if (!(await collection.findOne({ userId }, { projection: { _id: 1 } }))) {
    return { memories: [], inputTokens: 0, costUsd: 0 };
  }
  const embedded = await embedText(query);
  if (env.vectorBackend === 'mongo-cosine-scan') {
    const rows = await collection.find({ userId }).limit(500).toArray();
    const memories = rows
      .map((row) => ({ row, score: cosineSimilarity(embedded.embedding, row.embedding) }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 10)
      .map(({ row }) => row);
    return { memories, inputTokens: embedded.inputTokens, costUsd: embedded.costUsd };
  }
  const memories = await collection.aggregate<MemoryRowDoc>([
    {
      $vectorSearch: {
        index: 'memories_vector',
        path: 'embedding',
        queryVector: embedded.embedding,
        numCandidates: 100,
        limit: 10,
        filter: { userId: { $eq: userId } }
      }
    },
    { $project: { _id: 1, userId: 1, text: 1, embedding: 1, sourceThread: 1, createdAt: 1 } }
  ]).toArray();
  return { memories, inputTokens: embedded.inputTokens, costUsd: embedded.costUsd };
}

function memoryRequestedBy(query: string): string | undefined {
  const english = query.match(/^\s*remember(?:\s+this)?(?:\s+preference)?(?:\s+for\s+all\s+future\s+answers)?\s*[:,-]?\s*(.+)$/is)?.[1];
  const chinese = query.match(/^\s*(?:請)?記住\s*[:：,-]?\s*(.+)$/is)?.[1];
  const candidate = (english ?? chinese)?.trim();
  if (!candidate || candidate.length > 2000) return undefined;
  const stable = /\b(?:always|never|prefer|preference|my name|i am|i use|i work|i live|future answers?)\b|偏好|總是|永遠|不要|我的名字|我使用|我住/is;
  return stable.test(candidate) ? candidate : undefined;
}

// ---------------------------------------------------------------- quick web answer

type SearchResult = { title?: string; url?: string; content?: string; snippet?: string };
type SearchCacheDoc = {
  _id: string;
  provider: string;
  normalizedQuery: string;
  results: SearchResult[];
  createdAt: Date;
  expiresAt: Date;
};
const searchCache = new Map<string, { expiresAt: number; results: SearchResult[] }>();
let searchCacheIndexReady = false;
let searchCacheIndexPromise: Promise<void> | undefined;

async function ensureSearchCacheIndex(): Promise<void> {
  if (searchCacheIndexReady) return;
  if (!searchCacheIndexPromise) {
    searchCacheIndexPromise = db()
      .then((database) => database.collection<SearchCacheDoc>('searchCache').createIndex(
        { expiresAt: 1 },
        { expireAfterSeconds: 0, name: 'searchCache_expiry_ttl' }
      ))
      .then(() => { searchCacheIndexReady = true; })
      .finally(() => { searchCacheIndexPromise = undefined; });
  }
  await searchCacheIndexPromise;
}

const pendingSearches = new Map<string, Promise<{ results: SearchResult[]; cached: boolean }>>();

async function quickSearch(query: string): Promise<{ results: SearchResult[]; cached: boolean }> {
  const key = query.trim().toLowerCase().replace(/\s+/g, ' ');
  const pending = pendingSearches.get(key);
  if (pending) {
    const result = await pending;
    return { results: result.results, cached: true };
  }
  const work = searchOnce(query);
  pendingSearches.set(key, work);
  try { return await work; } finally { pendingSearches.delete(key); }
}

async function searchOnce(query: string): Promise<{ results: SearchResult[]; cached: boolean }> {
  const normalizedQuery = query.trim().toLowerCase().replace(/\s+/g, ' ');
  const key = createHash('sha256').update(`${normalizedQuery}\n${env.searchProvider}`).digest('hex');
  const existing = searchCache.get(key);
  if (existing && existing.expiresAt > Date.now()) {
    searchCache.delete(key);
    searchCache.set(key, existing);
    return { results: existing.results, cached: true };
  }
  if (existing) searchCache.delete(key);

  const cacheLookupStarted = Date.now();
  await ensureSearchCacheIndex();
  const collection = (await db()).collection<SearchCacheDoc>('searchCache');
  const persisted = await collection.findOne({ _id: key, provider: env.searchProvider, normalizedQuery });
  log.info({ cacheKey: key, cacheLookupMs: Date.now() - cacheLookupStarted, cacheHit: Boolean(persisted && persisted.expiresAt.getTime() > Date.now()) }, 'persistent search cache lookup timing');
  if (persisted && persisted.expiresAt.getTime() > Date.now()) {
    const cached = { results: persisted.results, expiresAt: persisted.expiresAt.getTime() };
    searchCache.set(key, cached);
    if (searchCache.size > 200) searchCache.delete(searchCache.keys().next().value as string);
    return { results: cached.results, cached: true };
  }

  if (env.searchProvider !== 'tavily') {
    throw new Error('This Quick implementation currently supports SEARCH_PROVIDER=tavily');
  }
  if (!secrets.tavily) throw new Error('TAVILY_API_KEY is not set');
  const providerStarted = Date.now();
  const response = await fetch('https://api.tavily.com/search', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ api_key: secrets.tavily, query, search_depth: 'basic', max_results: 6 }),
    signal: providerSignal(15000)
  });
  if (!response.ok) throw new Error(`Tavily search failed (${response.status})`);
  const body = (await response.json()) as { results?: SearchResult[] };
  const providerMs = Date.now() - providerStarted;
  const persistenceStarted = Date.now();
  const results = (body.results ?? []).filter((item) => typeof item.url === 'string').slice(0, 6);
  const expiresAt = new Date(Date.now() + env.searchCacheTtlSeconds * 1000);
  await collection.updateOne(
    { _id: key },
    { $set: { provider: env.searchProvider, normalizedQuery, results, createdAt: new Date(), expiresAt } },
    { upsert: true }
  );
  log.info({ cacheKey: key, providerMs, cacheWriteMs: Date.now() - persistenceStarted, ttlSeconds: env.searchCacheTtlSeconds }, 'uncached web search timing');
  searchCache.set(key, { results, expiresAt: expiresAt.getTime() });
  if (searchCache.size > 200) searchCache.delete(searchCache.keys().next().value as string);
  return { results, cached: false };
}

const pageTextCache = new Map<string, { text: string; expiresAt: number }>();

// Work on the article subtree when the publisher declares one. Keep retrieved
// words intact; removing navigation reduces DOM work without inventing evidence.
function preparePageHtml(raw: string): string {
  const clean = raw
    .replace(/<(script|style|svg|noscript|template)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, '');
  const title = clean.match(/<title\b[^>]*>([\s\S]*?)<\/title\s*>/i)?.[1] ?? '';
  const main = clean.match(/<main\b[^>]*>([\s\S]*?)<\/main\s*>/i)?.[1];
  const article = clean.match(/<article\b[^>]*>([\s\S]*?)<\/article\s*>/i)?.[1];
  const body = (main || article || clean)
    .replace(/<(nav|header|footer|aside|form)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, ' ')
    .slice(0, 120_000);
  return `<html><head><title>${title.slice(0, 1000)}</title></head><body>${body}</body></html>`;
}

async function readPagePayload(response: Response): Promise<string> {
  if (!response.body) throw new Error('Page returned no body');
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let bytes = 0;
  let text = '';
  try {
    while (bytes < 1_000_000) {
      const { done, value } = await reader.read();
      if (done) break;
      const accepted = value.subarray(0, 1_000_000 - bytes);
      bytes += accepted.byteLength;
      text += decoder.decode(accepted, { stream: true });
    }
    return text + decoder.decode();
  } finally {
    try { await reader.cancel(); } catch { /* Preserve the read error if transport closed. */ }
    reader.releaseLock();
  }
}

function quickPageText(html: string): string | undefined {
  // Text-dense blocks from the selected article region avoid constructing a
  // browser document and running Readability over thousands of layout nodes.
  const blocks: string[] = [];
  let length = 0;
  for (const match of html.matchAll(/<(p|h[1-6]|li|pre|tr)\b[^>]*>([\s\S]*?)<\/\1\s*>/gi)) {
    const block = match[2]!.replace(/<[^>]+>/g, ' ').trim();
    if (!block) continue;
    blocks.push(block);
    length += block.length;
    if (length >= 18_000) break;
  }
  // Nonstandard HTML may have no paragraph/list tags. Extract its visible text
  // from the already cleaned region instead of falling back to a full DOM and
  // synchronous Readability pass on Quick's shared event loop.
  const plain = blocks.length ? blocks.join(' ') : html.replace(/<[^>]+>/g, ' ');
  // Bound entity decoding too; all words still come from the fetched page.
  const bounded = plain.slice(0, 18_000);
  // Most extracted text needs no entity decoding; avoid DOM setup in that case.
  const decoded = /&(?:#\d+|#x[\da-f]+|[a-z][a-z\d]+);/i.test(bounded)
    ? (JSDOM.fragment(bounded).textContent ?? '') : bounded;
  const text = decoded.replace(/\s+/g, ' ').trim();
  return text.length >= 120 ? text : undefined;
}

async function fetchReadablePage(url: string, timeoutMs = 12000, quick = false): Promise<string> {
  const cacheKey = `${quick ? 'quick' : 'deep'}:${url}`;
  const cached = pageTextCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    pageTextCache.delete(cacheKey);
    pageTextCache.set(cacheKey, cached);
    return cached.text;
  }
  pageTextCache.delete(cacheKey);
  const parsed = new URL(url);
  if (!['http:', 'https:'].includes(parsed.protocol)) throw new Error('unsupported source URL');
  const response = await fetch(parsed, {
    headers: { 'user-agent': 'LUMINA/1.0 (course project; readable-page fetch)' },
    signal: providerSignal(timeoutMs),
    redirect: 'follow'
  });
  if (!response.ok) throw new Error(`Page fetch failed (${response.status})`);
  const payload = await readPagePayload(response);
  const contentType = response.headers.get('content-type')?.toLowerCase() ?? '';
  const parseStarted = Date.now();
  let text: string;
  if (contentType.startsWith('text/plain') || contentType.startsWith('text/markdown')) {
    text = payload.replace(/\s+/g, ' ').trim();
  } else {
    const html = preparePageHtml(payload);
    if (quick) {
      const quickText = quickPageText(html);
      if (!quickText) throw new Error('Page contained no readable text in bounded Quick extraction');
      text = quickText;
    } else {
      // Deep retains the full extractor. Quick never takes this blocking path.
      const dom = new JSDOM(html, { url: response.url || url });
      try {
        const article = new Readability(dom.window.document).parse();
        text = (article?.textContent ?? dom.window.document.body?.textContent ?? '').replace(/\s+/g, ' ').trim();
      } finally {
        dom.window.close();
      }
    }
  }
  if (!text) throw new Error('Page contained no readable text');
  const readable = text.slice(0, 9000);
  log.info({ url, extractionMode: quick ? 'quick' : 'deep', payloadBytesLimit: 1_000_000, parseMs: Date.now() - parseStarted,
    characters: readable.length }, 'page text extracted');
  pageTextCache.set(cacheKey, { text: readable, expiresAt: Date.now() + 300_000 });
  if (pageTextCache.size > 100) pageTextCache.delete(pageTextCache.keys().next().value as string);
  return readable;
}

type ChunkRowDoc = {
  _id: string;
  docId: string;
  spaceId: string;
  userId: string;
  text: string;
  locator: { page?: number; heading?: string; line?: number };
  ord: number;
  embedding: number[];
  createdAt?: Date;
};

async function searchDocuments(
  userId: string,
  spaceId: string,
  query: string
): Promise<{ passages: Array<{ source: Source; text: string }>; inputTokens: number; costUsd: number }> {
  const database = await db();
  const space = await database.collection<SpaceRowDoc>('spaces').findOne({ _id: spaceId, userId });
  if (!space) throw new Error('space not found');
  const indexedDocs = await database.collection<DocumentRowDoc>('documents')
    .find({ spaceId, userId, status: 'indexed' })
    .project<{ _id: string; title: string }>({ _id: 1, title: 1 })
    .toArray();
  if (!indexedDocs.length) return { passages: [], inputTokens: 0, costUsd: 0 };

  const titles = new Map(indexedDocs.map((document) => [document._id, document.title]));
  const allowedDocIds = new Set(titles.keys());
  const chunks = database.collection<ChunkRowDoc>('chunks');
  const embedded = await embedText(query);
  let ranked: ChunkRowDoc[];

  if (env.vectorBackend === 'mongo-cosine-scan') {
    const rows = await chunks.find({ spaceId, userId, docId: { $in: [...allowedDocIds] } }).limit(2000).toArray();
    const terms = query.toLowerCase().split(/\W+/).filter((term) => term.length > 2);
    ranked = rows
      .map((row) => {
        const lower = row.text.toLowerCase();
        const lexical = terms.reduce((score, term) => score + (lower.includes(term) ? 1 : 0), 0) / Math.max(terms.length, 1);
        return { row, score: cosineSimilarity(embedded.embedding, row.embedding) * 0.8 + lexical * 0.2 };
      })
      .sort((a, b) => b.score - a.score)
      .slice(0, 5)
      .map(({ row }) => row);
  } else {
    const [vectorRows, textRows] = await Promise.all([
      chunks.aggregate<ChunkRowDoc>([
      {
        $vectorSearch: {
          index: 'chunks_vector',
          path: 'embedding',
          queryVector: embedded.embedding,
          numCandidates: 100,
          limit: 20,
          filter: { spaceId: { $eq: spaceId }, userId: { $eq: userId } }
        }
      },
      { $match: { docId: { $in: [...allowedDocIds] } } },
      { $project: { _id: 1, docId: 1, spaceId: 1, userId: 1, text: 1, locator: 1, ord: 1 } }
      ]).toArray(),
      chunks.aggregate<ChunkRowDoc>([
      {
        $search: {
          index: 'chunks_text',
          compound: {
            must: [{ text: { query, path: 'text' } }],
            filter: [
              { equals: { path: 'spaceId', value: spaceId } },
              { equals: { path: 'userId', value: userId } }
            ]
          }
        }
      },
      { $match: { docId: { $in: [...allowedDocIds] } } },
      { $limit: 20 },
      { $project: { _id: 1, docId: 1, spaceId: 1, userId: 1, text: 1, locator: 1, ord: 1 } }
      ]).toArray()
    ]);

    const fused = new Map<string, { row: ChunkRowDoc; score: number }>();
    const addRanks = (rows: ChunkRowDoc[]) => rows.forEach((row, index) => {
      const current = fused.get(row._id) ?? { row, score: 0 };
      current.score += 1 / (60 + index + 1);
      fused.set(row._id, current);
    });
    addRanks(vectorRows);
    addRanks(textRows);
    ranked = [...fused.values()].sort((a, b) => b.score - a.score).slice(0, 5).map(({ row }) => row);
  }

  return {
    passages: ranked.map((row, index) => ({
      source: {
        n: index + 1,
        kind: 'doc',
        title: titles.get(row.docId) ?? 'Uploaded document',
        snippet: row.text.slice(0, 500),
        docId: row.docId,
        locator: row.locator
      },
      text: row.text
    })),
    inputTokens: embedded.inputTokens,
    costUsd: embedded.costUsd
  };
}

async function openQuickStream(
  query: string,
  passages: Array<{ source: Source; text: string }>,
  history: Array<{ role: 'user' | 'assistant'; content: string }>,
  memories: MemoryRowDoc[],
  deepPlan?: PlanEvent,
  partial = false
): Promise<Response> {
  if (env.llmProvider !== 'anthropic') throw new Error('This Quick implementation currently supports LLM_PROVIDER=anthropic');
  if (!secrets.anthropic) throw new Error('ANTHROPIC_API_KEY is not set');
  const context = passages.map(({ source, text }) => {
    const location = source.kind === 'web'
      ? `URL: ${source.url}`
      : `Document: ${source.title}; locator: ${JSON.stringify(source.locator)}`;
    // Publisher footnotes have their own numbering. Keep originals for source
    // evidence, but prevent those markers from becoming app citation numbers.
    const contextText = text.slice(0, deepPlan ? 4000 : 2500).replace(/\[\d{1,3}\]/g, '');
    return `[${source.n}] ${source.title}\n${location}\n${contextText}`;
  }).join('\n\n');
  const rememberedPreferences = memories
    .map((memory) => `- ${memory.text}`)
    .join('\n')
    .slice(0, 4000);
  const explicitWordLimit = memories
    .map((memory) => memory.text.match(/\b(?:under|at most|no more than)\s+(\d{1,4})\s+words?\b/i)?.[1])
    .filter((value): value is string => Boolean(value))
    .map(Number)
    .filter((value) => Number.isFinite(value) && value >= 20 && value <= 2000)
    .sort((a, b) => a - b)[0];
  const languageInstruction = /[\u3400-\u9fff]/.test(query)
    ? 'Answer in Chinese, using the same script as the question.'
    : 'Answer entirely in English.';
  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': secrets.anthropic,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: deepPlan ? env.llmModel : env.quickLlmModel,
      max_tokens: deepPlan ? 3000 : 900,
      stream: true,
      system: [
        'Answer using only the supplied source passages. Do not invent facts or citations.',
        languageInstruction,
        'Use the prior messages only to understand references and follow-up context. Ground current factual claims in the fetched sources for this request.',
        rememberedPreferences
          ? `Follow these saved user facts and preferences when they apply:\n${rememberedPreferences}`
          : 'There are no relevant saved user facts or preferences.',
        explicitWordLimit
          ? `Mandatory saved preference: keep the entire answer, including headings, at or below ${explicitWordLimit} words. Prioritise the direct answer and strongest evidence.`
          : '',
        'Be concise and answer the exact question. Start with the direct answer. Use a short heading or compact table only when it improves clarity; avoid repeating the same point.',
        deepPlan ? 'Keep the full answer under 350 words. Use at most two short paragraphs per sub-question and a one-sentence unknowns section; avoid long tables and repeated calculations.' : 'Keep the full answer under 180 words unless the user explicitly requests more detail.',
        deepPlan
          ? 'This is a deep-research answer. Give a short direct answer, then one clearly labelled section for each planned sub-question, and finish with a section named "What remains unknown". Do not pad the answer.'
          : '',
        partial
          ? 'The research limit was reached. Clearly label the answer as partial and state which planned areas remain unresolved.'
          : '',
        'Attach each [n] citation to the factual sentence it supports. Cite only the supplied source numbers.',
        `The complete list of allowed citation markers for this answer is: ${passages.map(({ source }) => `[${source.n}]`).join(', ')}. Never use a publisher bibliography number or an earlier answer citation as a source for this answer.`,
        'Finish complete sentences; do not stop mid-sentence. If the sources are insufficient, state what is unknown.'
      ].join(' '),
      messages: [
        ...history.map((message) => ({ role: message.role, content: message.content })),
        {
          role: 'user',
          content: [
            `Question: ${query}`,
            deepPlan
              ? `Research plan:\n${deepPlan.subQuestions.map((item) => `${item.i}. ${item.question} — ${item.reason ?? ''}`).join('\n')}`
              : '',
            `Sources for this request:\n${context}`
          ].filter(Boolean).join('\n\n')
        }
      ]
    }),
    signal: providerSignal(45000)
  });
  if (!response.ok) throw new Error(`Anthropic request failed (${response.status})`);
  if (!response.body) throw new Error('Anthropic returned no response stream');
  return response;
}

type ModelTextResult = {
  text: string;
  inputTokens: number;
  outputTokens: number;
  costUsd: number;
};

function anthropicCostUsd(model: string, inputTokens: number, outputTokens: number): number {
  const prices = model.includes('haiku')
    ? { input: 1, output: 5 }
    : { input: 3, output: 15 };
  return (inputTokens * prices.input + outputTokens * prices.output) / 1_000_000;
}

class PlannerOutputError extends Error {
  constructor(message: string, readonly usage: ModelTextResult) {
    super(message);
    this.name = 'PlannerOutputError';
  }
}

async function callAnthropicText(system: string, prompt: string, maxTokens: number, model = env.plannerLlmModel): Promise<ModelTextResult> {
  if (env.llmProvider !== 'anthropic') throw new Error('This Deep implementation currently supports LLM_PROVIDER=anthropic');
  if (!secrets.anthropic) throw new Error('ANTHROPIC_API_KEY is not set');
  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': secrets.anthropic,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({ model, max_tokens: maxTokens, system, messages: [{ role: 'user', content: prompt }] }),
    signal: providerSignal(45000)
  });
  if (!response.ok) throw new Error(`Anthropic request failed (${response.status})`);
  const body = await response.json() as {
    content?: Array<{ type?: string; text?: string }>;
    usage?: { input_tokens?: number; output_tokens?: number };
  };
  const text = (body.content ?? []).filter((item) => item.type === 'text').map((item) => item.text ?? '').join('').trim();
  if (!text) throw new Error('Anthropic returned an empty response');
  const inputTokens = body.usage?.input_tokens ?? 0;
  const outputTokens = body.usage?.output_tokens ?? 0;
  return { text, inputTokens, outputTokens, costUsd: anthropicCostUsd(model, inputTokens, outputTokens) };
}

async function planDeepResearch(query: string): Promise<{ plan: PlanEvent; usage: ModelTextResult }> {
  // The plan is Deep's first paint. Asking the model for the minimum valid plan keeps
  // that first paint fast and leaves more of the 24-call budget for actual evidence.
  const count = Math.min(5, env.deepSubQuestionsMax, Math.max(3, env.deepSubQuestionsMin));
  const usage = await callAnthropicText(
    [
      `Return JSON only. Create exactly ${count} non-overlapping research sub-questions.`,
      'Together they must cover the whole user question; group related criteria when needed.',
      'Repeat the main named products, technologies, or entities from the user question in every sub-question so searches stay on topic.',
      'Keep every question under 18 words and every reason under 8 words. Use a short overall reason.',
      'Use: {"subQuestions":[{"i":1,"question":"...","reason":"..."}],"reason":"..."}'
    ].join(' '),
    query,
    600
  );
  const start = usage.text.indexOf('{');
  const end = usage.text.lastIndexOf('}');
  if (start < 0 || end <= start) throw new PlannerOutputError('Deep planner did not return complete JSON', usage);
  let parsed: ReturnType<typeof PlanEvent.parse>;
  try {
    parsed = PlanEvent.parse(JSON.parse(usage.text.slice(start, end + 1)));
    if (parsed.subQuestions.length !== count) {
      throw new Error(`Deep planner returned ${parsed.subQuestions.length} sub-questions; expected ${count}`);
    }
  } catch (error) {
    throw new PlannerOutputError(error instanceof Error ? error.message : 'Deep planner returned invalid JSON', usage);
  }
  const plan = PlanEvent.parse({
    ...parsed,
    subQuestions: parsed.subQuestions.map((item, index) => ({ ...item, i: index + 1 }))
  });
  return { plan, usage };
}

function deepUsageKey(userId: string, now = new Date()): { id: string; day: string; resetsAt: string } {
  const day = now.toISOString().slice(0, 10);
  const reset = new Date(`${day}T00:00:00.000Z`);
  reset.setUTCDate(reset.getUTCDate() + 1);
  return { id: `${userId}:${day}`, day, resetsAt: reset.toISOString() };
}

async function claimDeepRun(userId: string): Promise<{ allowed: boolean; count: number; resetsAt: string }> {
  const { id, day, resetsAt } = deepUsageKey(userId);
  const collection = (await db()).collection<DeepUsageDoc>('deepUsage');
  const row = await collection.findOneAndUpdate(
    { _id: id },
    {
      $setOnInsert: { userId, day, createdAt: new Date() },
      $set: { updatedAt: new Date() },
      $inc: { count: 1 }
    },
    { upsert: true, returnDocument: 'after' }
  );
  const count = row?.count ?? 1;
  if (count > env.deepDailyCap) {
    await collection.updateOne({ _id: id }, { $inc: { count: -1 }, $set: { updatedAt: new Date() } });
    return { allowed: false, count: env.deepDailyCap, resetsAt };
  }
  return { allowed: true, count, resetsAt };
}

async function releaseDeepRun(userId: string): Promise<void> {
  const { id } = deepUsageKey(userId);
  await (await db()).collection<DeepUsageDoc>('deepUsage').updateOne(
    { _id: id, count: { $gt: 0 } },
    { $inc: { count: -1 }, $set: { updatedAt: new Date() } }
  );
}

function sourceKey(source: Source): string {
  if (source.kind === 'web') {
    try {
      const url = new URL(source.url!);
      url.hash = '';
      return `web:${url.toString().replace(/\/$/, '')}`;
    } catch {
      return `web:${source.url}`;
    }
  }
  return `doc:${source.docId}:${JSON.stringify(source.locator ?? {})}`;
}

async function consumeQuickStream(
  response: Response,
  onText: (text: string) => void,
  model = env.quickLlmModel
): Promise<{ text: string; inputTokens: number; outputTokens: number; costUsd: number; stopReason?: string }> {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let text = '';
  let inputTokens = 0;
  let outputTokens = 0;
  let stopReason: string | undefined;
  let messageStopped = false;

  const handleFrame = (frame: string) => {
    const data = frame.split('\n')
      .filter((line) => line.startsWith('data:'))
      .map((line) => line.slice(5).trimStart())
      .join('\n');
    if (!data) return;
    const event = JSON.parse(data) as {
      type?: string;
      message?: { usage?: { input_tokens?: number } };
      delta?: { type?: string; text?: string; stop_reason?: string };
      usage?: { output_tokens?: number };
      error?: { message?: string };
    };
    if (event.type === 'error') throw new Error(event.error?.message || 'Anthropic stream failed');
    if (event.type === 'message_stop') messageStopped = true;
    if (event.type === 'message_start') inputTokens = event.message?.usage?.input_tokens ?? inputTokens;
    if (event.type === 'message_delta') {
      outputTokens = event.usage?.output_tokens ?? outputTokens;
      stopReason = event.delta?.stop_reason ?? stopReason;
    }
    if (event.type === 'content_block_delta' && event.delta?.type === 'text_delta' && event.delta.text) {
      text += event.delta.text;
      onText(event.delta.text);
    }
  };

  try {
    // message_stop is the provider's terminal event. Do not keep waiting for a
    // transport EOF after the answer and usage have already been delivered.
    while (!messageStopped) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer = (buffer + decoder.decode(value, { stream: true })).replace(/\r\n/g, '\n');
      let split: number;
      while ((split = buffer.indexOf('\n\n')) !== -1) {
        handleFrame(buffer.slice(0, split));
        buffer = buffer.slice(split + 2);
        if (messageStopped) break;
      }
    }
    if (!messageStopped) {
      buffer += decoder.decode();
      if (buffer.trim()) handleFrame(buffer.replace(/\r\n/g, '\n'));
    }
    if (!messageStopped || !stopReason) throw new Error('Anthropic stream ended before its terminal event and usage');
  } finally {
    // Release connections on terminal events, malformed frames and timeouts alike.
    try { await reader.cancel(); } catch { /* The transport may already be closed. */ }
    reader.releaseLock();
  }
  text = text.trim();
  if (!text) throw new Error('Anthropic returned an empty answer');
  const costUsd = anthropicCostUsd(model, inputTokens, outputTokens);
  return { text, inputTokens, outputTokens, costUsd, ...(stopReason ? { stopReason } : {}) };
}

type DeepAskInput = {
  startedAt: number;
  requestId: string;
  userId: string;
  threadId: string;
  query: string;
  retrievalMode: 'web' | 'docs';
  spaceId?: string;
  history: Array<{ role: 'user' | 'assistant'; content: string }>;
  trace: ToolTrace[];
  res: express.Response;
};

async function handleDeepAsk(input: DeepAskInput): Promise<void> {
  const { startedAt, requestId, userId, threadId, query, retrievalMode, spaceId, history, trace, res } = input;
  const allowanceStarted = Date.now();
  const allowance = await claimDeepRun(userId);
  const allowanceMs = Date.now() - allowanceStarted;
  if (!allowance.allowed) {
    res.status(429).json({
      error: `daily Deep search cap of ${env.deepDailyCap} reached`,
      status: 429,
      resetsAt: allowance.resetsAt,
      requestId
    });
    return;
  }

  const planStarted = Date.now();
  let planned: Awaited<ReturnType<typeof planDeepResearch>>;
  try {
    planned = await planDeepResearch(query);
  } catch (error) {
    trace.push({
      step: 1,
      tool: 'plan_research',
      input: { query },
      ok: false,
      ms: Date.now() - planStarted,
      error: error instanceof Error ? error.message : 'unknown planning error'
    });
    const plannerUsage = error instanceof PlannerOutputError ? error.usage : undefined;
    const failingDir = `${env.runsDir}/failing`;
    mkdirSync(failingDir, { recursive: true });
    const safeRequestId = requestId.replace(/[^A-Za-z0-9_-]/g, '_');
    writeFileSync(`${failingDir}/${safeRequestId}.json`, JSON.stringify({
      query,
      tokens: (plannerUsage?.inputTokens ?? 0) + (plannerUsage?.outputTokens ?? 0),
      wallClockSec: (Date.now() - startedAt) / 1000,
      costUsd: plannerUsage?.costUsd ?? 0,
      terminated: 'error',
      depth: 'deep',
      toolCalls: trace.map((item) => ({
        name: item.tool,
        ok: item.ok,
        ...(item.error ? { error: item.error } : {}),
        ms: item.ms
      }))
    }, null, 2));
    await releaseDeepRun(userId).catch((releaseError) => {
      log.warn({ userId, errorName: releaseError instanceof Error ? releaseError.name : 'unknown' }, 'failed to refund Deep allowance after planner failure');
    });
    throw error;
  }

  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
  const send = (event: string, data: unknown) => { res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); };

  // The plan is deliberately the first frame. No memory, web, or document retrieval has
  // happened yet, so the user sees the actual plan rather than a post-hoc explanation.
  send('plan', planned.plan);
  log.info({ requestId, beforeAllowanceMs: allowanceStarted - startedAt, allowanceMs,
    plannerMs: Date.now() - planStarted, planSentMs: Date.now() - startedAt,
    plannerInputTokens: planned.usage.inputTokens, plannerOutputTokens: planned.usage.outputTokens
  }, 'deep plan stage timing');
  trace.push({
    step: 1,
    tool: 'plan_research',
    input: { query },
    ok: true,
    ms: Date.now() - planStarted,
    reason: `Decomposed the question into ${planned.plan.subQuestions.length} research threads`
  });
  send('trace', trace[0]);

  const recallStarted = Date.now();
  let recalled: Awaited<ReturnType<typeof recallMemory>>;
  try {
    recalled = await recallMemory(userId, query);
    trace.push({
      step: trace.length + 1,
      tool: 'recall_memory',
      input: { query },
      ok: true,
      ms: Date.now() - recallStarted,
      reason: `Recalled ${recalled.memories.length} relevant saved memories for this user`
    });
    send('trace', trace.at(-1));
  } catch (error) {
    const item: ToolTrace = {
      step: trace.length + 1,
      tool: 'recall_memory',
      input: { query },
      ok: false,
      ms: Date.now() - recallStarted,
      error: error instanceof Error ? error.message : 'unknown memory retrieval error'
    };
    trace.push(item);
    send('trace', item);
    throw error;
  }

  const merged = new Map<string, { source: Source; text: string }>();
  let retrievalInputTokens = 0;
  let retrievalCostUsd = 0;
  let searchCalls = 0;
  let allSearchesCached = true;
  let hitCap = false;
  const overLimit = () =>
    trace.length >= env.maxToolCallsDeep ||
    Date.now() - startedAt >= env.maxWallClockSecDeep * 1000;
  const addPassage = (passage: { source: Source; text: string }, subQuestion: number) => {
    const source = { ...passage.source, n: Math.max(1, merged.size + 1), subQuestion } satisfies Source;
    const key = sourceKey(source);
    if (!merged.has(key)) merged.set(key, { source, text: passage.text });
  };

  for (const sub of planned.plan.subQuestions) {
    if (overLimit()) {
      hitCap = true;
      break;
    }
    if (retrievalMode === 'docs') {
      const searchStarted = Date.now();
      try {
        const result = await searchDocuments(userId, spaceId!, sub.question);
        retrievalInputTokens += result.inputTokens;
        retrievalCostUsd += result.costUsd;
        result.passages.forEach((passage) => addPassage(passage, sub.i));
        const item: ToolTrace = {
          step: trace.length + 1,
          tool: 'search_documents',
          input: { query: sub.question, spaceId },
          ok: true,
          ms: Date.now() - searchStarted,
          reason: `Retrieved ${result.passages.length} document passages for research thread ${sub.i}`,
          subQuestion: sub.i
        };
        trace.push(item);
        send('trace', item);
      } catch (error) {
        const item: ToolTrace = {
          step: trace.length + 1,
          tool: 'search_documents',
          input: { query: sub.question, spaceId },
          ok: false,
          ms: Date.now() - searchStarted,
          error: error instanceof Error ? error.message : 'unknown document search error',
          subQuestion: sub.i
        };
        trace.push(item);
        send('trace', item);
        throw error;
      }
      continue;
    }

    const searchStarted = Date.now();
    let search: Awaited<ReturnType<typeof quickSearch>>;
    try {
      search = await quickSearch(sub.question);
      searchCalls += 1;
      allSearchesCached = allSearchesCached && search.cached;
      const item: ToolTrace = {
        step: trace.length + 1,
        tool: 'web_search',
        input: { query: sub.question },
        ok: true,
        ms: Date.now() - searchStarted,
        reason: search.cached ? 'Reused cached results for this research thread' : 'Searched for distinct evidence for this research thread',
        subQuestion: sub.i
      };
      trace.push(item);
      send('trace', item);
    } catch (error) {
      const item: ToolTrace = {
        step: trace.length + 1,
        tool: 'web_search',
        input: { query: sub.question },
        ok: false,
        ms: Date.now() - searchStarted,
        error: error instanceof Error ? error.message : 'unknown web search error',
        subQuestion: sub.i
      };
      trace.push(item);
      send('trace', item);
      throw error;
    }

    // Read three distinct pages per sub-question; avoid spending calls on URLs
    // already retrieved for another sub-question. Three threads can yield nine sources.
    const candidates = search.results.filter((result) => !merged.has(sourceKey({ n: 1, snippet: String(), kind: 'web', title: result.title || result.url!, url: result.url! }))).slice(0, 3);
    await Promise.all(candidates.map(async (result) => {
      if (overLimit()) {
        hitCap = true;
        return;
      }
      const fetchStarted = Date.now();
      try {
        const pageCached = (pageTextCache.get(`deep:${result.url!}`)?.expiresAt ?? 0) > Date.now();
        const text = await fetchReadablePage(result.url!, 2500);
        addPassage({
          source: {
            n: Math.max(1, merged.size + 1),
            kind: 'web',
            title: (result.title || result.url!).slice(0, 300),
            url: result.url!,
            snippet: text.slice(0, 500),
            subQuestion: sub.i
          },
          text
        }, sub.i);
        const item: ToolTrace = {
          step: trace.length + 1,
          tool: 'fetch_page',
          input: { url: result.url },
          ok: true,
          ms: Date.now() - fetchStarted,
          reason: pageCached ? 'Retrieved page text from the five-minute page cache for grounded deep synthesis' : 'Read page text for grounded deep synthesis',
          subQuestion: sub.i
        };
        trace.push(item);
        send('trace', item);
      } catch (error) {
        const item: ToolTrace = {
          step: trace.length + 1,
          tool: 'fetch_page',
          input: { url: result.url },
          ok: false,
          ms: Date.now() - fetchStarted,
          error: `${result.url}: ${error instanceof Error ? error.message : 'unknown page fetch error'}`,
          subQuestion: sub.i
        };
        trace.push(item);
        send('trace', item);
      }
    }));
    if (hitCap) break;
  }

  const passages = [...merged.values()].map((passage, index) => ({
    ...passage,
    source: { ...passage.source, n: index + 1 }
  }));
  send('sources', passages.map((passage) => passage.source));

  let firstTokenAt: number | undefined;
  let answer: { text: string; inputTokens: number; outputTokens: number; costUsd: number; stopReason?: string };
  if (passages.length) {
    const stream = await openQuickStream(query, passages, history, recalled.memories, planned.plan, hitCap);
    answer = await consumeQuickStream(stream, (text) => {
      firstTokenAt ??= Date.now();
      send('token', { text });
    }, env.llmModel);
    const unresolved = unresolvedCitations(answer.text, passages.map((passage) => passage.source));
    if (unresolved.length) throw new Error(`answer contained unresolved citations: ${unresolved.join(', ')}`);
  } else {
    const text = retrievalMode === 'docs'
      ? 'I could not retrieve indexed document passages for any planned research thread, so I do not have grounded sources to cite.'
      : 'I could not retrieve readable web pages for any planned research thread, so I do not have grounded sources to cite.';
    firstTokenAt = Date.now();
    send('token', { text });
    answer = { text, inputTokens: 0, outputTokens: 0, costUsd: 0 };
  }

  const terminated = hitCap || answer.stopReason === 'max_tokens' ? 'cap' as const : 'done' as const;
  const answerId = newId('ans') as AnswerId;
  const tokensIn = planned.usage.inputTokens + recalled.inputTokens + retrievalInputTokens + answer.inputTokens;
  const tokensOut = planned.usage.outputTokens + answer.outputTokens;
  const costUsd = planned.usage.costUsd + recalled.costUsd + retrievalCostUsd + answer.costUsd;
  const done = {
    answerId,
    latencyMs: Date.now() - startedAt,
    ttftMs: (firstTokenAt ?? Date.now()) - startedAt,
    model: env.llmModel,
    tokens: { in: tokensIn, out: tokensOut },
    costUsd,
    searchCached: searchCalls > 0 && allSearchesCached,
    terminated,
    depth: 'deep' as const,
    subQuestions: planned.plan.subQuestions.length
  };

  const database = await db();
  await database.collection<MessageDoc>('messages').insertMany([
    { threadId, userId, role: 'user', content: query, createdAt: new Date() },
    { threadId, userId, role: 'assistant', content: answer.text, sources: passages.map((passage) => passage.source), answerId, done, createdAt: new Date() }
  ]);
  const run = {
    query,
    tokens: tokensIn + tokensOut,
    wallClockSec: (Date.now() - startedAt) / 1000,
    costUsd,
    terminated,
    depth: 'deep',
    toolCalls: trace.map((item) => ({
      name: item.tool,
      ok: item.ok,
      ...(item.error ? { error: item.error } : {}),
      ms: item.ms,
      ...(item.subQuestion ? { subQuestion: item.subQuestion } : {})
    }))
  };
  const safeRequestId = requestId.replace(/[^A-Za-z0-9_-]/g, '_');
  writeFileSync(`${env.runsDir}/${safeRequestId}.json`, JSON.stringify(run, null, 2));
  await database.collection('runs').updateOne(
    { requestId },
    { $set: { ...run, requestId, userId, threadId, createdAt: new Date() } },
    { upsert: true }
  );
  log.info({ requestId, userId, threadId, toolCalls: trace.length, terminated, tokens: done.tokens, costUsd, searchCached: done.searchCached, ttftMs: done.ttftMs, latencyMs: done.latencyMs }, 'deep answer completed');
  send('done', done);
  res.end();
}

app.get('/stats', async (req, res) => {
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401 });
  try {
    const database = await db();
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);
    const messages = await database.collection<MessageDoc>('messages')
      .find({ userId, role: 'assistant', createdAt: { $gte: today } })
      .project<{ done?: Record<string, unknown> }>({ done: 1 })
      .toArray();
    const ttfts = messages.map((row) => Number(row.done?.ttftMs ?? 0)).filter((value) => value >= 0).sort((a, b) => a - b);
    const p95 = ttfts.length ? ttfts[Math.min(ttfts.length - 1, Math.ceil(ttfts.length * 0.95) - 1)]! : 0;
    const cached = messages.filter((row) => row.done?.searchCached === true).length;
    const usageKey = deepUsageKey(userId);
    const deepUsage = await database.collection<DeepUsageDoc>('deepUsage').findOne({ _id: usageKey.id });
    const requests = await database.collection<MessageDoc>('messages').countDocuments({ userId, role: 'user' });
    return res.json(StatsResponse.parse({
      requests,
      answers: messages.length,
      searchCacheHitRatePct: messages.length ? cached * 100 / messages.length : 0,
      ttftP95Ms: p95,
      costUsdToday: messages.reduce((sum, row) => sum + Number(row.done?.costUsd ?? 0), 0),
      deepToday: deepUsage?.count ?? 0,
      deepDailyCap: env.deepDailyCap
    }));
  } catch (error) {
    log.error({ userId, errorName: error instanceof Error ? error.name : 'unknown' }, 'failed to read stats');
    return res.status(502).json({ error: 'failed to read stats', status: 502 });
  }
});

app.post('/threads/:threadId/ask', async (req, res) => {
  const startedAt = Date.now();
  const requestId = req.get('x-request-id') ?? newId('req');
  const userId = req.get('x-user-id')?.trim();
  if (!userId) return res.status(401).json({ error: 'X-User-Id is required', status: 401, requestId });
  const parsedBody = AskBody.safeParse(req.body ?? {});
  if (!parsedBody.success) return res.status(400).json({ error: 'invalid request body', status: 400, requestId });
  const { query, mode, depth, spaceId } = parsedBody.data;
  const retrievalMode = mode === 'auto' ? (spaceId ? 'docs' : 'web') : mode;
  if (retrievalMode === 'docs' && !spaceId) {
    return res.status(400).json({ error: 'spaceId is required in docs mode', status: 400, requestId });
  }

  log.info({ requestId, userId, depth, retrievalMode }, 'agent ask received');
  let thread;
  let priorMessages: MessageDoc[] = [];
  try {
    const database = await db();
    // Both reads are user-scoped and independent. No provider work starts
    // until ownership is confirmed; an unknown/foreign thread still returns 404.
    [thread, priorMessages] = await Promise.all([
      database.collection<ThreadDoc>('threads').findOne({ _id: req.params.threadId, userId }),
      database.collection<MessageDoc>('messages')
        .find({ threadId: req.params.threadId, userId })
        .sort({ createdAt: -1 }).limit(6).toArray()
    ]);
  } catch {
    return res.status(502).json({ error: 'database lookup failed', status: 502, requestId });
  }
  if (!thread) return res.status(404).json({ error: 'thread not found', status: 404, requestId });

  const trace: ToolTrace[] = [];
  log.info({ requestId, userId, depth, retrievalMode, lookupMs: Date.now() - startedAt }, 'agent ask lookup completed');
  try {
    const messageCollection = (await db()).collection<MessageDoc>('messages');
    const history = priorMessages
      .reverse()
      .map((message) => ({ role: message.role, content: message.content }));

    if (depth === 'deep') {
      log.info({ requestId, historyReadWithOwnership: true, beforePlanningMs: Date.now() - startedAt }, 'deep history ready');
      await handleDeepAsk({
        startedAt,
        requestId,
        userId,
        threadId: req.params.threadId,
        query,
        retrievalMode,
        ...(spaceId ? { spaceId } : {}),
        history,
        trace,
        res
      });
      return;
    }
    const requestedMemory = memoryRequestedBy(query);
    if (requestedMemory) {
      const saveStarted = Date.now();
      const embedded = await embedText(requestedMemory);
      const memoryId = newId('mem');
      await (await db()).collection<MemoryRowDoc>('memories').insertOne({
        _id: memoryId,
        userId,
        text: requestedMemory,
        embedding: embedded.embedding,
        sourceThread: req.params.threadId,
        createdAt: new Date()
      });
      trace.push({
        step: 1,
        tool: 'save_memory',
        input: { text: requestedMemory },
        ok: true,
        ms: Date.now() - saveStarted,
        reason: 'Saved an explicit stable fact or preference for future threads'
      });

      res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
      res.setHeader('Cache-Control', 'no-cache, no-transform');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('X-Accel-Buffering', 'no');
      res.flushHeaders();
      const send = (event: string, data: unknown) => { res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); };
      send('trace', trace[0]);
      send('sources', []);
      const responseText = /[\u3400-\u9fff]/.test(query)
        ? `已儲存這項長期記憶：${requestedMemory}`
        : `Saved this long-term memory: ${requestedMemory}`;
      const firstTokenAt = Date.now();
      send('token', { text: responseText });
      const answerId = newId('ans') as AnswerId;
      const done = {
        answerId,
        latencyMs: Date.now() - startedAt,
        ttftMs: firstTokenAt - startedAt,
        model: env.embeddingModel,
        tokens: { in: embedded.inputTokens, out: 0 },
        costUsd: embedded.costUsd,
        searchCached: false,
        terminated: 'done' as const,
        depth: 'quick' as const,
        subQuestions: 0
      };
      await messageCollection.insertMany([
        { threadId: req.params.threadId, userId, role: 'user', content: query, createdAt: new Date() },
        { threadId: req.params.threadId, userId, role: 'assistant', content: responseText, sources: [], answerId, done, createdAt: new Date() }
      ]);
      const safeRequestId = requestId.replace(/[^A-Za-z0-9_-]/g, '_');
      const run = {
        query,
        tokens: embedded.inputTokens,
        wallClockSec: (Date.now() - startedAt) / 1000,
        costUsd: embedded.costUsd,
        terminated: 'done',
        depth: 'quick',
        toolCalls: trace.map((item) => ({ name: item.tool, ok: item.ok, ...(item.error ? { error: item.error } : {}), ms: item.ms }))
      };
      writeFileSync(`${env.runsDir}/${safeRequestId}.json`, JSON.stringify(run, null, 2));
      await (await db()).collection('runs').updateOne({ requestId }, { $set: { ...run, requestId, userId, createdAt: new Date() } }, { upsert: true });
      log.info({ requestId, userId, threadId: req.params.threadId, toolCalls: trace.length, terminated: done.terminated, tokens: done.tokens, costUsd: done.costUsd, searchCached: false, ttftMs: done.ttftMs, latencyMs: done.latencyMs }, 'memory saved');
      send('done', done);
      return res.end();
    }

    const recallStarted = Date.now();
    const webStarted = Date.now();
    const docsStarted = Date.now();
    let docsMs = 0;
    let recallMs = 0;
    let searchMs = 0;
    const measureRetrieval = async <T>(tool: string, operation: () => Promise<T>): Promise<T> => {
      const start = Date.now();
      log.info({ requestId, userId, tool }, 'retrieval started');
      try {
        const result = await operation();
        log.info({ requestId, userId, tool, ms: Date.now() - start }, 'retrieval completed');
        return result;
      } catch (error) {
        log.error({ requestId, userId, tool, ms: Date.now() - start,
          error: error instanceof Error ? error.message : String(error) }, 'retrieval failed');
        throw error;
      }
    };
    // Fetch pages immediately after search, overlapping independent memory recall.
    const webRetrieval = retrievalMode === 'web'
      ? measureRetrieval('web_search', () => quickSearch(query))
        .finally(() => { searchMs = Date.now() - webStarted; })
        .then(async (search) => {
      const fetchedPages = await Promise.all(search.results.slice(0, 3).map(async (result) => {
        const fetchStarted = Date.now();
        try {
          const pageCached = (pageTextCache.get(`quick:${result.url!}`)?.expiresAt ?? 0) > Date.now();
          const text = await fetchReadablePage(result.url!, env.quickPageTimeoutMs, true);
          return { result, text, pageCached, ms: Date.now() - fetchStarted };
        } catch (error) {
          return {
            result,
            error: error instanceof Error ? error.message : 'unknown page fetch error',
            ms: Date.now() - fetchStarted
          };
        }
      }));
          return { search, fetchedPages };
        })
      : Promise.resolve(undefined);
    const initial = await Promise.allSettled([
      measureRetrieval('recall_memory', () => recallMemory(userId, query)).finally(() => { recallMs = Date.now() - recallStarted; }),
      webRetrieval,
      retrievalMode === 'docs' ? measureRetrieval('search_documents', () => searchDocuments(userId, spaceId!, query)).finally(() => { docsMs = Date.now() - docsStarted; }) : Promise.resolve(undefined)
    ]);
    const recallResult = initial[0];
    const webResult = initial[1];
    if (recallResult.status === 'rejected') {
      trace.push({ step: 1, tool: 'recall_memory', input: { query }, ok: false, ms: recallMs, error: recallResult.reason instanceof Error ? recallResult.reason.message : String(recallResult.reason) });
      throw recallResult.reason;
    }
    const recalled = recallResult.value;
    trace.push({
      step: 1,
      tool: 'recall_memory',
      input: { query },
      ok: true,
      ms: recallMs,
      reason: `Recalled ${recalled.memories.length} relevant saved memories for this user`
    });
    let passages: Array<{ source: Source; text: string }> = [];
    let searchCached = false;
    let retrievalInputTokens = 0;
    let retrievalCostUsd = 0;
    if (retrievalMode === 'docs') {
      const docsResult = initial[2];
      if (docsResult.status === 'rejected') {
        trace.push({ step: trace.length + 1, tool: 'search_documents', input: { query, spaceId }, ok: false,
          ms: docsMs, error: docsResult.reason instanceof Error ? docsResult.reason.message : String(docsResult.reason) });
        throw docsResult.reason;
      }
      const result = docsResult.value!;
      passages = result.passages;
      retrievalInputTokens = result.inputTokens;
      retrievalCostUsd = result.costUsd;
      trace.push({
        step: trace.length + 1,
        tool: 'search_documents',
        input: { query, spaceId },
        ok: true,
        ms: docsMs,
        reason: `Retrieved ${passages.length} passages from indexed documents in the selected Space`
      });
    } else {
      if (webResult.status === 'rejected') {
        trace.push({ step: trace.length + 1, tool: 'web_search', input: { query }, ok: false, ms: searchMs, error: webResult.reason instanceof Error ? webResult.reason.message : String(webResult.reason) });
        throw webResult.reason;
      }
      const { search, fetchedPages } = webResult.value!;
      searchCached = search.cached;
      trace.push({ step: trace.length + 1, tool: 'web_search', input: { query }, ok: true, ms: searchMs, reason: search.cached ? 'Reused cached search results' : 'Find current web sources' });

      for (const fetched of fetchedPages) {
        const { result } = fetched;
        if ('text' in fetched && fetched.text) {
          const source: Source = {
            n: passages.length + 1,
            kind: 'web',
            title: (result.title || result.url!).slice(0, 300),
            url: result.url!,
            snippet: fetched.text.slice(0, 500)
          };
          passages.push({ source, text: fetched.text.slice(0, 4000) });
          trace.push({ step: trace.length + 1, tool: 'fetch_page', input: { url: result.url }, ok: true, ms: fetched.ms, reason: fetched.pageCached ? 'Retrieved page text from the five-minute page cache for grounded synthesis' : 'Read page text for grounded synthesis' });
        } else {
          const fallbackText = (result.content ?? result.snippet ?? '').trim();
          trace.push({
            step: trace.length + 1,
            tool: 'fetch_page',
            input: { url: result.url },
            ok: false,
            ms: fetched.ms,
            error: `${result.url}: ${fetched.error}`,
            ...(fallbackText ? { reason: 'Page fetch failed; explicitly falling back to the search provider snippet for this source' } : {})
          });
          if (fallbackText) {
            const source: Source = {
              n: passages.length + 1,
              kind: 'web',
              title: (result.title || result.url!).slice(0, 300),
              url: result.url!,
              snippet: fallbackText.slice(0, 500)
            };
            passages.push({ source, text: fallbackText.slice(0, 4000) });
          }
        }
      }
    }

    // Establish the Anthropic stream before committing the browser response so an
    // immediate upstream rejection can still be returned as HTTP 502.
    const retrievalReadyAt = Date.now();
    const modelStartedAt = Date.now();
    const anthropicStream = passages.length
      ? await openQuickStream(query, passages, history, recalled.memories)
      : undefined;
    const modelHeadersAt = Date.now();
    log.info({ requestId, retrievalReadyMs: retrievalReadyAt - startedAt, modelHeadersMs: modelHeadersAt - modelStartedAt }, 'quick pre-token stages completed');

    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    const send = (event: string, data: unknown) => { res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); };
    for (const item of trace) send('trace', item);
    send('sources', passages.map((item) => item.source));

    let firstTokenAt: number | undefined;
    let answer: { text: string; inputTokens: number; outputTokens: number; costUsd: number; stopReason?: string };
    if (anthropicStream) {
      answer = await consumeQuickStream(anthropicStream, (text) => {
        if (firstTokenAt === undefined) {
          firstTokenAt = Date.now();
          log.info({ requestId, retrievalReadyMs: retrievalReadyAt - startedAt,
            modelHeadersMs: modelHeadersAt - modelStartedAt, streamFirstTokenMs: firstTokenAt - modelHeadersAt,
            modelFirstTokenMs: firstTokenAt - modelStartedAt, ttftMs: firstTokenAt - startedAt }, 'quick first token timing');
        }
        send('token', { text });
      });
      const unresolved = unresolvedCitations(answer.text, passages.map((item) => item.source));
      if (unresolved.length) throw new Error(`answer contained unresolved citations: ${unresolved.join(', ')}`);
    } else {
      const text = retrievalMode === 'docs'
        ? 'I could not retrieve any indexed document passages from this Space, so I do not have grounded sources to cite.'
        : 'I could not retrieve readable pages for this question, so I do not have grounded sources to cite.';
      firstTokenAt = Date.now();
      send('token', { text });
      answer = { text, inputTokens: 0, outputTokens: 0, costUsd: 0 };
    }

    // Persist the exchange before returning a successful stream.
    const answerId = newId('ans') as AnswerId;
    const done = {
      answerId,
      latencyMs: Date.now() - startedAt,
      ttftMs: (firstTokenAt ?? Date.now()) - startedAt,
      model: env.quickLlmModel,
      tokens: { in: answer.inputTokens + recalled.inputTokens + retrievalInputTokens, out: answer.outputTokens },
      costUsd: answer.costUsd + recalled.costUsd + retrievalCostUsd,
      searchCached,
      terminated: answer.stopReason === 'max_tokens' ? 'cap' as const : 'done' as const,
      depth: 'quick' as const,
      subQuestions: 0
    };
    await messageCollection.insertMany([
      { threadId: req.params.threadId, userId, role: 'user', content: query, createdAt: new Date() },
      { threadId: req.params.threadId, userId, role: 'assistant', content: answer.text, sources: passages.map((item) => item.source), answerId, done, createdAt: new Date() }
    ]);
    const safeRequestId = requestId.replace(/[^A-Za-z0-9_-]/g, '_');
    const run = {
      query,
      tokens: answer.inputTokens + recalled.inputTokens + retrievalInputTokens + answer.outputTokens,
      wallClockSec: (Date.now() - startedAt) / 1000,
      costUsd: answer.costUsd + recalled.costUsd + retrievalCostUsd,
      terminated: done.terminated,
      depth: 'quick',
      toolCalls: trace.map((item) => ({ name: item.tool, ok: item.ok, ...(item.error ? { error: item.error } : {}), ms: item.ms }))
    };
    writeFileSync(`${env.runsDir}/${safeRequestId}.json`, JSON.stringify(run, null, 2));
    await (await db()).collection('runs').updateOne({ requestId }, { $set: { ...run, requestId, userId, createdAt: new Date() } }, { upsert: true });
    log.info({ requestId, userId, threadId: req.params.threadId, toolCalls: trace.length, terminated: done.terminated, tokens: done.tokens, costUsd: done.costUsd, searchCached: done.searchCached, ttftMs: done.ttftMs, latencyMs: done.latencyMs }, 'quick answer completed');

    send('done', done);
    return res.end();
  } catch (error) {
    const message = error instanceof Error ? error.message : 'unknown provider failure';
    const failedRun = { query, depth, tokens: 0, costUsd: 0, usageIncomplete: true, wallClockSec: (Date.now() - startedAt) / 1000, terminated: 'error', error: message, toolCalls: trace.map((item) => ({ name: item.tool, ok: item.ok, ...(item.error ? { error: item.error } : {}), ms: item.ms })) };
    try {
      const safeId = requestId.replace(/[^A-Za-z0-9_-]/g, '_');
      writeFileSync(`${env.runsDir}/${safeId}.json`, JSON.stringify(failedRun, null, 2));
      await (await db()).collection('runs').updateOne({ requestId }, { $setOnInsert: { ...failedRun, requestId, userId, createdAt: new Date() } }, { upsert: true });
    } catch { log.error({ requestId }, 'failed to persist error trajectory'); }
    log.error({ error: message, requestId, userId, threadId: req.params.threadId, depth, errorName: error instanceof Error ? error.name : 'unknown' }, 'answer failed');
    if (res.destroyed) return;
    if (res.headersSent) {
      res.write(`event: error\ndata: ${JSON.stringify({ status: 502, error: message })}\n\n`);
      return res.end();
    }
    return res.status(502).json({ error: message, status: 502, requestId });
  }
});

// ---------------------------------------------------------------- everything else: 501

const notImplemented = (route: string) => (_req: express.Request, res: express.Response) => {
  res.status(501).json({ error: `not implemented yet: ${route}. Build it in backend/agent/src/.`, status: 501 });
};

for (const declaredRoute of ROUTES) {
  // Widen the generated route union so this defensive fallback remains valid
  // if the contract adds a route in a future revision.
  const route: { method: string; path: string } = declaredRoute;
  if (
    route.path === '/health' ||
    route.path === '/stats' ||
    route.path === '/evals/report.json' ||
    (route.method === 'POST' && route.path === '/threads') ||
    (route.method === 'GET' && route.path === '/threads') ||
    (route.method === 'GET' && route.path === '/threads/:threadId') ||
    (route.method === 'POST' && route.path === '/threads/:threadId/ask') ||
    (route.method === 'GET' && route.path === '/memory') ||
    (route.method === 'DELETE' && route.path === '/memory/:memoryId') ||
    (route.method === 'POST' && route.path === '/spaces') ||
    (route.method === 'GET' && route.path === '/spaces') ||
    (route.method === 'POST' && route.path === '/spaces/:spaceId/documents') ||
    (route.method === 'GET' && route.path === '/spaces/:spaceId/documents')
  ) continue;
  const method = route.method.toLowerCase() as 'get' | 'post' | 'delete';
  app[method](route.path, notImplemented(`${route.method} ${route.path}`));
}

app.use((req, res) => res.status(404).json({ error: `no route ${req.method} ${req.path}`, status: 404 }));

app.use((err: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  log.error({ err }, 'agent error');
  res.status(502).json({ error: err.message, status: 502 });
});

app.listen(env.port, () => {
  const preparationStarted = Date.now();
  void ensureSearchCacheIndex().then(() => {
    log.info({ preparationMs: Date.now() - preparationStarted, ttlSeconds: env.searchCacheTtlSeconds }, 'search cache index ready');
  }).catch(error => {
    log.warn({ errorName: error instanceof Error ? error.name : 'unknown' }, 'search cache preparation failed; searches will retry explicitly');
  });
  void db().then(prepareUploadsBucket).then(() => {
    log.info({ preparationMs: Date.now() - preparationStarted }, 'document storage ready');
  }).catch(error => {
    log.warn({ errorName: error instanceof Error ? error.name : 'unknown' },
      'document storage preparation failed; uploads will retry and fail explicitly if unavailable');
  });
  log.info(
    {
      port: env.port,
      model: env.llmModel,
      searchProvider: env.searchProvider,
      vectorStore: env.vectorBackend,
      caps: {
        quick: { toolCalls: env.maxToolCalls, wallClockSec: env.maxWallClockSec },
        deep: { toolCalls: env.maxToolCallsDeep, wallClockSec: env.maxWallClockSecDeep, dailyCap: env.deepDailyCap }
      }
    },
    'agent up — Quick, Deep, memory, and document research are enabled'
  );
});

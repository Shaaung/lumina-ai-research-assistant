import { MongoClient, type Db } from 'mongodb';
import { env } from './env.js';

let connectedClient: Promise<MongoClient> | undefined;

/** Share initial connection as well as the pool; retry after a failed connection. */
export async function db(): Promise<Db> {
  if (!env.mongoUri) throw new Error('MONGODB_URI is not set — copy .env.example to .env');
  if (!connectedClient) {
    const client = new MongoClient(env.mongoUri, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 10000,
      socketTimeoutMS: 15000,
      waitQueueTimeoutMS: 5000,
      // Keep headroom rather than relying only on startup pings to open sockets.
      minPoolSize: 4,
      maxConnecting: 4
    });
    connectedClient = client.connect().then(async () => {
      // Upload persistence runs metadata and GridFS writes concurrently. Warm
      // primary connections with overlapping read-only commands so the first
      // file write need not wait for another socket's TLS/authentication.
      // Await readiness rather than assuming minPoolSize has already filled.
      await Promise.all(Array.from({ length: 4 }, () =>
        client.db(env.mongoDb).command({ ping: 1 })
      ));
      return client;
    }).catch(async (error) => {
      connectedClient = undefined;
      try { await client.close(); } catch { /* Preserve the original connection error. */ }
      throw error;
    });
  }
  return (await connectedClient).db(env.mongoDb);
}

export async function pingDb(): Promise<'ok' | 'down'> {
  try {
    await (await db()).command({ ping: 1 });
    return 'ok';
  } catch {
    return 'down';
  }
}

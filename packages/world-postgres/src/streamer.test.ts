import type { Pool } from 'pg';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Drizzle } from './drizzle/index.js';
import { createStreamer } from './streamer.js';

type NotificationMessage = { payload?: string | undefined };
type NotificationHandler = (message: NotificationMessage) => void;

const { client, notificationHandlers } = vi.hoisted(() => {
  const notificationHandlers = new Set<NotificationHandler>();
  const client = {
    connect: vi.fn(async () => {}),
    query: vi.fn(async () => ({ rows: [] })),
    on: vi.fn((event: string, handler: NotificationHandler) => {
      if (event === 'notification') {
        notificationHandlers.add(handler);
      }
    }),
    removeListener: vi.fn((event: string, handler: NotificationHandler) => {
      if (event === 'notification') {
        notificationHandlers.delete(handler);
      }
    }),
    end: vi.fn(async () => {}),
  };

  return { client, notificationHandlers };
});

vi.mock('pg', () => ({
  Client: vi.fn(function Client() {
    return client;
  }),
}));

type SelectResult = unknown[] | Promise<unknown[]> | Error;

function createDrizzle(selectResults: SelectResult[]) {
  const select = vi.fn(() => {
    const result = selectResults.shift() ?? [];
    const chain: Record<string, unknown> = {};

    chain.from = vi.fn(() => chain);
    chain.where = vi.fn(() => chain);
    chain.orderBy = vi.fn(() => chain);
    chain.limit = vi.fn(() => chain);
    chain.then = (
      onFulfilled: (value: unknown[]) => unknown,
      onRejected: (error: unknown) => unknown
    ) => {
      const promise =
        result instanceof Error ? Promise.reject(result) : Promise.resolve(result);
      return promise.then(onFulfilled, onRejected);
    };

    return chain;
  });

  return {
    drizzle: { select } as unknown as Drizzle,
    select,
  };
}

async function getNotificationHandler(): Promise<NotificationHandler> {
  await vi.waitFor(() => {
    expect(notificationHandlers.size).toBe(1);
  });

  const [handler] = notificationHandlers;
  if (!handler) {
    throw new Error('notification handler was not registered');
  }
  return handler;
}

async function flushNotificationWork(): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
}

async function expectNotificationIgnored(
  handler: NotificationHandler,
  select: ReturnType<typeof vi.fn>,
  streamId: string
): Promise<void> {
  const callsBeforeNotification = select.mock.calls.length;

  handler({
    payload: JSON.stringify({
      streamId,
      chunkId: 'chnk_01KTESTSTREAMREADERCLEANUP',
    }),
  });
  await flushNotificationWork();

  expect(select).toHaveBeenCalledTimes(callsBeforeNotification);
}

describe('Postgres stream reader cleanup', () => {
  const pool = {
    options: {},
    query: vi.fn(async () => ({ rows: [] })),
  } as unknown as Pool;

  beforeEach(() => {
    vi.clearAllMocks();
    notificationHandlers.clear();
  });

  it('removes the reader listener when persisted EOF closes the stream', async () => {
    const streamId = 'stream-eof';
    const { drizzle, select } = createDrizzle([
      [
        {
          id: 'chnk_01KTESTEOF',
          data: Buffer.from([]),
          eof: true,
        },
      ],
    ]);
    const streamer = createStreamer(pool, drizzle);
    const notificationHandler = await getNotificationHandler();
    const stream = await streamer.streams.get('run-1', streamId);
    const reader = stream.getReader();

    await expect(reader.read()).resolves.toEqual({
      done: true,
      value: undefined,
    });
    expect(select).toHaveBeenCalledTimes(1);

    await expectNotificationIgnored(notificationHandler, select, streamId);
    await streamer.close();
  });

  it('removes the reader listener when live EOF arrives via NOTIFY', async () => {
    const streamId = 'stream-live-eof';
    const { drizzle, select } = createDrizzle([
      [],
      [{ data: Buffer.from([]), eof: true }],
    ]);
    const streamer = createStreamer(pool, drizzle);
    const notificationHandler = await getNotificationHandler();
    const stream = await streamer.streams.get('run-1', streamId);
    const reader = stream.getReader();

    // Let the initial empty query finish so this exercises the live listener
    // rather than the startup buffer used to bridge query/NOTIFY races.
    await flushNotificationWork();
    const pendingRead = reader.read();

    notificationHandler({
      payload: JSON.stringify({
        streamId,
        chunkId: 'chnk_01KLIVEEOF',
      }),
    });

    await expect(pendingRead).resolves.toEqual({
      done: true,
      value: undefined,
    });
    expect(select).toHaveBeenCalledTimes(2);

    await expectNotificationIgnored(notificationHandler, select, streamId);
    await streamer.close();
  });

  it('removes the reader listener when the initial stream query fails', async () => {
    const streamId = 'stream-query-error';
    const queryError = new Error('stream query failed');
    const { drizzle, select } = createDrizzle([queryError]);
    const streamer = createStreamer(pool, drizzle);
    const notificationHandler = await getNotificationHandler();
    const stream = await streamer.streams.get('run-1', streamId);
    const reader = stream.getReader();

    await expect(reader.read()).rejects.toThrow(queryError);
    expect(select).toHaveBeenCalledTimes(1);

    await expectNotificationIgnored(notificationHandler, select, streamId);
    await streamer.close();
  });

  it('closes pending readers and removes their listeners on shutdown', async () => {
    const streamId = 'stream-world-close';
    const { drizzle, select } = createDrizzle([
      [
        {
          id: 'chnk_01KDATA',
          data: Buffer.from('hello'),
          eof: false,
        },
      ],
    ]);
    const streamer = createStreamer(pool, drizzle);
    const notificationHandler = await getNotificationHandler();
    const stream = await streamer.streams.get('run-1', streamId);
    const reader = stream.getReader();

    await expect(reader.read()).resolves.toEqual({
      done: false,
      value: new Uint8Array(Buffer.from('hello')),
    });
    const pendingRead = reader.read();

    await streamer.close();
    await expect(pendingRead).resolves.toEqual({
      done: true,
      value: undefined,
    });
    await expectNotificationIgnored(notificationHandler, select, streamId);
  });

  it('does not register new readers after shutdown', async () => {
    const streamId = 'stream-after-close';
    const { drizzle, select } = createDrizzle([]);
    const streamer = createStreamer(pool, drizzle);
    const notificationHandler = await getNotificationHandler();

    await streamer.close();

    const stream = await streamer.streams.get('run-1', streamId);
    const reader = stream.getReader();
    await expect(reader.read()).resolves.toEqual({
      done: true,
      value: undefined,
    });
    expect(select).not.toHaveBeenCalled();
    await expectNotificationIgnored(notificationHandler, select, streamId);
  });

  it('retains explicit consumer cancellation cleanup', async () => {
    const streamId = 'stream-cancel';
    const { drizzle, select } = createDrizzle([[]]);
    const streamer = createStreamer(pool, drizzle);
    const notificationHandler = await getNotificationHandler();
    const stream = await streamer.streams.get('run-1', streamId);
    const reader = stream.getReader();

    await flushNotificationWork();
    await reader.cancel();
    await expectNotificationIgnored(notificationHandler, select, streamId);
    await streamer.close();
  });
});

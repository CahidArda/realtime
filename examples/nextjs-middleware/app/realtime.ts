import { Realtime, InferRealtimeEvents } from "@upstash/realtime";
import { redis } from "./redis";
import { z } from "zod";

const schema = {
  sseStream: z.object({
    event: z.string(),
  }),
  waitForEvent: z.object({
    event: z.string(),
  }),
};

export const realtime = new Realtime({ schema, redis });
export type RealtimeEvents = InferRealtimeEvents<typeof realtime>;
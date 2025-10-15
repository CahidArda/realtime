import { Realtime, InferRealtimeEvents } from "@/app/src/server";
import { redis } from "./redis";
import { z } from "zod";

const schema = {
  sseStream: z.object({
    event: z.string(),
  }),
};

export const realtime = new Realtime({ schema, redis });
export type RealtimeEvents = InferRealtimeEvents<typeof realtime>;
import { handle, toStreamResponse } from "@/app/src/server";
import { realtime, RealtimeEvents } from "@/app/realtime";
import { NextRequest } from "next/server";
import { AI_SDK_HEADER } from "@/app/constants";

export const POST = handle({
  realtime,
  middleware: async ({ request, channel }) => {
    if (request.headers.get(AI_SDK_HEADER)) {
      const nextUrl = (request as NextRequest).nextUrl;
      await fetch(`${nextUrl.origin}/api/trigger`, {
        method: "POST",
        body: await request.text(),
        headers: request.headers,
      });

      return toStreamResponse<RealtimeEvents>(realtime, {
        includeSystemEvents: false,
        abortOn: "[DONE]",
        events: {
          sseStream: {
            event: {
              transform: (event) => {
                return event.data;
              },
            },
          },
        },
      });
    }

    return;
  },
});

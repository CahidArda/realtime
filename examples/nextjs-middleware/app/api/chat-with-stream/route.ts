import { NextRequest } from "next/server";
import { realtime, RealtimeEvents } from "@/app/realtime";
import { toStreamResponse } from "@upstash/realtime"

export const dynamic = "force-dynamic"
export const maxDuration = 60
export const runtime = "nodejs"

export const POST = async (request: NextRequest) => {
  const nextUrl = request.nextUrl

  await fetch(`${nextUrl.origin}/api/trigger`, {
    method: "POST",
    body: await request.text(),
    headers: request.headers
  })

  return toStreamResponse<RealtimeEvents>(
  realtime,
  {
    includeSystemEvents: false,
    abortOn: "[DONE]",
    events: {
      sseStream: {
        event: {
          transform: (event) => {
            return typeof event.data === "string" ? event.data : JSON.stringify(event.data)
          }
        }
      }
    }
  })
}

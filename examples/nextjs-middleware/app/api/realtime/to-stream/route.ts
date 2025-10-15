import { realtime, RealtimeEvents } from "@/app/realtime"
import { toStreamResponse } from "@/app/src/server"

export const GET = () => toStreamResponse<RealtimeEvents>(
  realtime,
  {
    events: {
      sseStream: {
        event: {
          resume: true,
          transform: (event) => {
            return typeof event === "string" ? event : JSON.stringify(event)
          }
        }
      }
    }
  }
)
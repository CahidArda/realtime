import { realtime, RealtimeEvents } from "@/app/realtime"
import { toStreamResponse } from "@upstash/realtime"

export const GET = () => toStreamResponse<RealtimeEvents>(
  realtime,
  {
    events: {
      sseStream: {
        event: {
          resume: true,
          transform: (event) => {
            console.log(event);
            return event
          }
        }
      }
    }
  }
)
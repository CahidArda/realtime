
import type { Redis } from "@upstash/redis"
import type { Realtime } from "./realtime"
import type { SystemEvent, UserEvent, RealtimeMessage } from "../types"

interface BackendClientOpts<T> {
  channel?: string
  events?: Partial<{
    [N in keyof T]: Partial<{
      [K in keyof T[N]]: (data: T[N][K], streamId: string) => void
    }>
  }>
}


interface StreamResponseOpts<T> {
  channel?: string
  includeSystemEvents?: boolean
  abortOn?: string | string[]
  events?: Partial<{
    [N in keyof T]: Partial<{
      [K in keyof T[N]]: {
        resume?: boolean
        transform?: (event: { data: T[N][K]; __event_path: string[]; __stream_id: string }) => string
      }
    }>
  }>
}

export class BackendClient<T extends Record<string, Record<string, unknown>>> {
  private realtime: Realtime<any>
  private redis: Redis
  private logger: any
  private channel: string
  public events?: BackendClientOpts<T>["events"]
  private subscriber?: ReturnType<Redis["subscribe"]>
  private isSubscribed = false

  constructor(
    realtime: Realtime<any>,
    opts: BackendClientOpts<T> = {}
  ) {
    this.realtime = realtime
    this.redis = realtime._redis!
    this.logger = realtime._logger
    this.channel = opts.channel || "default"
    this.events = opts.events

    if (!this.redis) {
      throw new Error("Redis instance is required for BackendClient")
    }

    // Auto-start subscription if events are provided
    if (this.events) {
      this._startSubscription().catch(error => {
        this.logger.error("Failed to start subscription:", error)
      })
    }
  }

  /**
   * Get history of events from the stream
   */
  async getHistory(count?: number): Promise<UserEvent[]> {
    try {
      const streamKey = `channel:${this.channel}`
      const messages = count
        ? await this.redis.xrange(streamKey, "-", "+", count)
        : await this.redis.xrange(streamKey, "-", "+")

      const events: UserEvent[] = []

      Object.entries(messages).forEach(([streamId, value]) => {
        if (typeof value === "object" && value !== null) {
          const { __event_path, data } = value as Record<string, unknown>

          if (__event_path && Array.isArray(__event_path)) {
            events.push({
              data,
              __event_path: __event_path as string[],
              __stream_id: streamId,
            })
          }
        }
      })

      // Sort events by stream ID (which contains timestamp) from past to future
      events.sort((a, b) => a.__stream_id.localeCompare(b.__stream_id))

      return events
    } catch (error) {
      this.logger.error("Error getting history:", error)
      return []
    }
  }

  /**
   * Start subscription to events
   */
  async startSubscription(): Promise<void> {
    await this._startSubscription()
  }

  /**
   * Internal method to start subscription
   */
  async _startSubscription(): Promise<void> {
    if (this.isSubscribed || !this.events) {
      return
    }

    try {
      this.subscriber = this.redis.subscribe(`channel:${this.channel}`)
      this.isSubscribed = true

      this.subscriber.on("message", async ({ message }: { message: unknown }) => {
        try {
          let payload: Record<string, unknown>

          if (typeof message === "string") {
            try {
              payload = JSON.parse(message)
            } catch {
              return
            }
          } else if (typeof message === "object" && message !== null) {
            payload = message as Record<string, unknown>
          } else {
            return
          }

          // Skip system events like ping
          if (payload.type) {
            return
          }

          const { __stream_id, __event_path, data } = payload

          if (!__event_path || !Array.isArray(__event_path)) {
            return
          }

          this.logger.log("⬇️ Received event:", {
            channel: this.channel,
            __event_path,
            data
          })

          // Navigate to the handler function
          const handler = __event_path.reduce(
            (acc: any, key: any) => acc?.[key],
            this.events
          )

          if (typeof handler === "function") {
            handler(data, __stream_id as string)
          }
        } catch (error) {
          this.logger.error("Error processing message:", error)
        }
      })

      this.subscriber.on("error", (error) => {
        this.logger.error("Subscription error:", error)
      })

      this.logger.log(`✅ Backend client subscribed to channel: ${this.channel}`)
    } catch (error) {
      this.logger.error("Error starting subscription:", error)
      throw error
    }
  }

  /**
   * Unsubscribe from events
   */
  async unsubscribe(): Promise<void> {
    if (this.subscriber) {
      await this.subscriber.unsubscribe()
      this.isSubscribed = false
      this.logger.log(`⬅️ Backend client unsubscribed from channel: ${this.channel}`)
    }
  }


}

/**
 * Create a Server-Sent Events response for realtime events
 */
export function toStreamResponse<T extends Record<string, Record<string, unknown>>>(
  realtime: Realtime<T>,
  options: StreamResponseOpts<T> = {}
): Response {
  const { channel = "default", includeSystemEvents = true, abortOn, events } = options
  const redis = realtime._redis!
  const logger = realtime._logger

  // Normalize abortOn to an array
  const abortSignals = abortOn ? (Array.isArray(abortOn) ? abortOn : [abortOn]) : []

  if (!redis) {
    throw new Error("Redis instance is required for toStreamResponse")
  }

  const stream = new ReadableStream({
    async start(controller) {
      const safeEnqueue = (data: Uint8Array) => {
        try {
          controller.enqueue(data)
        } catch (error) {
          // Stream might be closed
        }
      }

      // Helper function to format SSE data
      const formatSSE = (data: RealtimeMessage) => {
        return new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`)
      }

      // Send connected event
      if (includeSystemEvents) {
        const connectedEvent: SystemEvent = {
          type: "connected",
          channel,
        }
        safeEnqueue(formatSSE(connectedEvent))
      }

      try {
        // Helper to get history
        const getHistory = async (count?: number): Promise<UserEvent[]> => {
          try {
            const streamKey = `channel:${channel}`
            const messages = count
              ? await redis.xrange(streamKey, "-", "+", count)
              : await redis.xrange(streamKey, "-", "+")

            const historyEvents: UserEvent[] = []

            Object.entries(messages).forEach(([streamId, value]) => {
              if (typeof value === "object" && value !== null) {
                const { __event_path, data } = value as Record<string, unknown>

                if (__event_path && Array.isArray(__event_path)) {
                  historyEvents.push({
                    data,
                    __event_path: __event_path as string[],
                    __stream_id: streamId,
                  })
                }
              }
            })

            // Sort events by stream ID from past to future
            historyEvents.sort((a, b) => a.__stream_id.localeCompare(b.__stream_id))
            return historyEvents
          } catch (error) {
            logger.error("Error getting history:", error)
            return []
          }
        }

        // Process historical events if resume is enabled for any event
        if (events) {
          let historyFetched = false

          for (const [eventCategory, eventHandlers] of Object.entries(events)) {
            for (const [eventName, config] of Object.entries(eventHandlers || {})) {
              const eventConfig = config as { resume?: boolean; transform?: (event: { data: any; __event_path: string[]; __stream_id: string }) => string }

              if (eventConfig?.resume && !historyFetched) {
                const history = await getHistory()
                historyFetched = true

                for (const event of history) {
                  // Check if this event matches any configured event with resume
                  if (event.__event_path.length === 2) {
                    const [evtCategory, evtName] = event.__event_path
                    const categoryConfig = (events as any)?.[evtCategory as string]
                    const evtConfig = categoryConfig?.[evtName as string] as { resume?: boolean; transform?: (event: { data: any; __event_path: string[]; __stream_id: string }) => string } | undefined

                    if (evtConfig?.resume) {
                      if (evtConfig.transform) {
                        const transformed = evtConfig.transform({
                          data: event.data,
                          __event_path: event.__event_path,
                          __stream_id: event.__stream_id
                        })
                        safeEnqueue(new TextEncoder().encode(`data: ${transformed}\n\n`))
                      } else {
                        // Default: JSON.stringify
                        const defaultTransform = JSON.stringify(event)
                        safeEnqueue(new TextEncoder().encode(`data: ${defaultTransform}\n\n`))
                      }
                    }
                  }
                }
                break
              }
            }
            if (historyFetched) break
          }
        }

        // Set up real-time subscription
        const subscriber = await redis.subscribe(`channel:${channel}`)

        const cleanup = () => {
          subscriber.unsubscribe().catch(() => { })
          controller.close()
        }

        subscriber.on("message", ({ message }: { message: unknown }) => {
          try {
            let payload: Record<string, unknown>

            if (typeof message === "string") {
              // Check for raw abort signals
              if (abortSignals.length > 0 && abortSignals.includes(message)) {
                logger.log(`Received abort signal "${message}", closing stream`)
                cleanup()
                return
              }
              try {
                payload = JSON.parse(message)
              } catch {
                return
              }
            } else if (typeof message === "object" && message !== null) {
              payload = message as Record<string, unknown>
            } else {
              return
            }

            // Check for abort signals in payload data
            if (abortSignals.length > 0 && abortSignals.includes(payload.data as string)) {
              logger.log(`Received abort signal "${payload.data}" in payload, closing stream`)
              cleanup()
              return
            }

            // Handle system events (like ping)
            if (payload.type) {
              if (includeSystemEvents) {
                safeEnqueue(formatSSE(payload as SystemEvent))
              }
              // Handle disconnected event
              if (payload.type === "disconnected") {
                logger.log("Received disconnected event, closing stream")
                cleanup()
              }
              return
            }

            // Handle user events
            const { __stream_id, __event_path, data } = payload

            if (__event_path && Array.isArray(__event_path) && __event_path.length === 2) {
              const [eventCategory, eventName] = __event_path

              // If events filter is specified, check if this event matches
              if (events) {
                const categoryConfig = (events as any)?.[eventCategory as string]
                const eventConfig = categoryConfig?.[eventName as string] as { resume?: boolean; transform?: (event: { data: any; __event_path: string[]; __stream_id: string }) => string } | undefined

                // Skip events that don't match the filter
                if (!eventConfig) {
                  return
                }

                // Apply transform if present
                if (eventConfig.transform) {
                  const transformed = eventConfig.transform({
                    data,
                    __event_path: __event_path as string[],
                    __stream_id: __stream_id as string
                  })
                  safeEnqueue(new TextEncoder().encode(`data: ${transformed}\n\n`))
                } else {
                  // Default: JSON.stringify
                  const userEvent: UserEvent = {
                    data,
                    __event_path: __event_path as string[],
                    __stream_id: __stream_id as string,
                  }
                  safeEnqueue(new TextEncoder().encode(`data: ${JSON.stringify(userEvent)}\n\n`))
                }
              } else {
                // No filter, send all events
                const userEvent: UserEvent = {
                  data,
                  __event_path: __event_path as string[],
                  __stream_id: __stream_id as string,
                }

                safeEnqueue(formatSSE(userEvent))
              }
            }
          } catch (error) {
            const errorEvent: SystemEvent = {
              type: "error",
              error: error instanceof Error ? error.message : "Unknown error",
            }
            safeEnqueue(formatSSE(errorEvent))
          }
        })

        subscriber.on("error", (error: Error) => {
          const errorEvent: SystemEvent = {
            type: "error",
            error: error instanceof Error ? error.message : "Unknown error",
          }
          safeEnqueue(formatSSE(errorEvent))
        })

        // Set up keepalive
        const keepaliveInterval = setInterval(() => {
          redis.publish(`channel:${channel}`, {
            type: "ping",
            timestamp: Date.now(),
          }).catch(() => { })
        }, 10_000)

        // Cleanup on stream cancel
        return () => {
          clearInterval(keepaliveInterval)
          cleanup()
        }
      } catch (error) {
        const errorEvent: SystemEvent = {
          type: "error",
          error: error instanceof Error ? error.message : "Unknown error",
        }
        safeEnqueue(formatSSE(errorEvent))
        controller.close()
      }
    },
  })

  return new Response(stream, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Cache-Control",
    },
  })
}

/**
 * Create a backend client for realtime operations
 * 
 * @param realtime - The Realtime instance
 * @param opts - Configuration options including channel and event handlers
 * @returns BackendClient instance with getHistory and unsubscribe methods
 */
export function createBackendClient<T extends Record<string, Record<string, unknown>>>(
  realtime: Realtime<any>,
  opts: BackendClientOpts<T> = {}
): BackendClient<T> {
  return new BackendClient(realtime, opts)
}


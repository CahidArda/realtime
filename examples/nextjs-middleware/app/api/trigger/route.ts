import { openai } from "@ai-sdk/openai";
import {
  convertToModelMessages,
  streamText,
  UIMessage,
  type ModelMessage,
} from "ai";
import { serve } from "@upstash/workflow/nextjs";
import { realtime } from "@/app/realtime";
import { greeting } from "../chat/route";

export const { POST } = serve<{ messages: UIMessage[] }>(async (context) => {
  const conversationMessages = [...context.requestPayload.messages];
  
  const { finalResponse } = await context.run(
    "planning-stage",
    async () => {
      const stream = streamText({
        model: openai("gpt-4o-mini"),
        messages: convertToModelMessages(conversationMessages),
        tools: {
          greeting
        },
      });

       const uiMessageStream = stream.toUIMessageStream({
        generateMessageId: () => Math.random().toString(36).substring(2, 10),
      })

      for await (const message of uiMessageStream) {
        await realtime.sseStream.event.emit(JSON.stringify(message));
      }

      await realtime.sseStream.event.emit("[DONE]");

      return {
        finalResponse: await stream.text,
      }
    }
  );

  return new Response(
    JSON.stringify({ success: true, text: finalResponse }),
    {
      headers: { "Content-Type": "application/json" },
    }
  );
}, {
  retries: 0
});

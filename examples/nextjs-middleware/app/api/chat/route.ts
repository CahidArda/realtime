import { openai } from '@ai-sdk/openai';
import { streamText, UIMessage, convertToModelMessages, tool, InferAgentUIMessage, Agent, validateUIMessages } from 'ai';
import z, { transform } from 'zod';

// Allow streaming responses up to 30 seconds
export const maxDuration = 30;

export const greeting = tool({
  description: "A simple greeting tool that returns a friendly message.",
  execute: async ({ name }: { name: string }) => {
    return `Hello, ${name}! How can I assist you today?`;
  },
  inputSchema: z.object({ name: z.string().min(1).max(100) }),
  needsApproval: true
});

export const greetingWithApprovalAgent = new Agent({
  
  model: openai('gpt-4o'),
  // context engineering required to make sure the model does not retry
  // the tool execution if it is not approved:
  system:
    'When a tool execution is not approved by the user, do not retry it.' +
    'Just say that the tool execution was not approved.',
  tools: {
    greeting,
  },
  onStepFinish: ({ request }) => {
    console.log(JSON.stringify(request.body, null, 2));
  },
});

export type GreetingWithApprovalAgentUIMessage = InferAgentUIMessage<
  typeof greetingWithApprovalAgent
>;

// export async function POST(req: Request) {
//   const { messages }: { messages: UIMessage[] } = await req.json();

//   return greetingWithApprovalAgent.respond({
//     messages: await validateUIMessages({ messages }),
//   });
// }

export async function POST(req: Request) {
  const { messages }: { messages: UIMessage[] } = await req.json();

  const result = streamText({
    model: openai('gpt-4o'),
    tools: {
      greeting,
    },
    messages: convertToModelMessages(messages),
  });

  return result.toUIMessageStreamResponse();
}
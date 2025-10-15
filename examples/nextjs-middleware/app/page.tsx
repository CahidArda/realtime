'use client';

import { useChat } from '@ai-sdk/react';
import { DefaultChatTransport, lastAssistantMessageIsCompleteWithApprovalResponses } from 'ai';
import { useState } from 'react';
import { GreetingWithApprovalAgentUIMessage } from './api/chat/route';

export default function Chat() {
  const [input, setInput] = useState('');

  const { messages, sendMessage, addToolApprovalResponse, status } = useChat<GreetingWithApprovalAgentUIMessage>(
    {
      sendAutomaticallyWhen: lastAssistantMessageIsCompleteWithApprovalResponses,
      transport: new DefaultChatTransport({
        api: "/api/chat-with-stream",
        fetch: async (url, options) => {
          return fetch(url, options);
        },
      })
    }
  );
  console.log(status);
  

  return (
    <div className="flex flex-col w-full max-w-md py-24 mx-auto stretch">
      {messages.map(message => (
        <div key={message.id} className="whitespace-pre-wrap">
          {message.role === 'user' ? 'User: ' : 'AI: '}
          {message.parts.map((part, i) => {
            switch (part.type) {
              case 'text':
                return <div key={`${message.id}-${i}`}>{part.text}</div>;
              case 'tool-greeting':
                return (
                  <div key={`${message.id}-${i}`}>
                    [Tool: {
                      part.state === "input-streaming" ? "Input streaming" :
                      part.state === "input-available" ? "Input available" :
                      part.state === "approval-requested" ? "Approval requested" :
                      part.state === "approval-responded" ? "Approval responded" :
                      part.state === "output-available" ? "Output available" :
                      part.state === "output-error" ? "Output error" :
                      part.state === "output-denied" ? "Output denied" :
                      "Unknown state"
                    }]
                    {part.state === "approval-requested" && part.approval?.id && (
                      <div className="mt-2 space-x-2">
                        <button
                          onClick={() => addToolApprovalResponse({
                            id: part.approval.id,
                            approved: true
                          })}
                          className="px-3 py-1 bg-green-500 text-white rounded hover:bg-green-600"
                        >
                          Approve
                        </button>
                        <button
                          onClick={() => addToolApprovalResponse({
                            id: part.approval.id,
                            approved: false
                          })}
                          className="px-3 py-1 bg-red-500 text-white rounded hover:bg-red-600"
                        >
                          Reject
                        </button>
                      </div>
                    )}
                  </div>
                );
            }
          })}
        </div>
      ))}

      <form
        onSubmit={e => {
          e.preventDefault();
          sendMessage({ text: input });
          setInput('');
        }}
      >
        <input
          className="fixed dark:bg-zinc-900 bottom-0 w-full max-w-md p-2 mb-8 border border-zinc-300 dark:border-zinc-800 rounded shadow-xl"
          value={input}
          placeholder="Say something..."
          onChange={e => setInput(e.currentTarget.value)}
        />
      </form>
    </div>
  );
}
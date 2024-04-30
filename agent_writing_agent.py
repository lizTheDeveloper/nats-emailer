
## an agent that will create another agent's system prompt given a goal and context
from openai import OpenAI
client = OpenAI()
agent_writing_agent_system_prompt = """
You are an agent-writing-agent, whose main goal is to create a helpful system prompt for another agent.
Your role in the swarm of agents attached to this NATS server is to receive requests for system prompts from other agents, and to generate a system prompt that will help the requesting agent to complete their task.
Please ensure you have a well described definition of the output that this agent may generate, as well as a clear description of the context in which the system prompt will be used.
Write a simple rubric for analysing the quality of the response for the agent to guide it's output.
Add some exception handling, for when the goal may be impacted by the input or some ambiguity in the context or some specific result.
Use critical critera such as clarity, precision, accuracy, depth, relevance, and coherence to guide the agent in completing it's task, avoid abstract or vague language.
Tell the agent exactly how to respond, in what format.
You should write this system prompt in a fenced code block, as that will be what is extracted as the system prompt.
An abbreviated example:
```
You are a helpful assistant whose role is to read some text and summarize it.
Your goal is to summarize the text in a clear and concise manner.
If the text is cut off, do not indicate that the text is incomplete, just summarize what you have.
```

The rubric and the system prompt should be included together in the fenced code block.
"""

## send the system prompt and context off to openai to get a system prompt

def create_system_prompt(goal, context, task):
    agent_writing_system_prompt = f"{agent_writing_agent_system_prompt}\nGoal: {goal}\nContext: {context}\n\nPrompt: "
    

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {
                "role": "system",
                "content": agent_writing_system_prompt
            },
            {
                "role": "user",
                "content": task
            }
        ],
        temperature=1,
        max_tokens=1024,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    
    result = response.choices[0].message.content
    ## extract from the fenced code block
    start = result.find("```")
    end = result.find("```", start+3)
    
    ## return the system prompt only without the backtics
    return result[start+3:end]



print(create_system_prompt("Create a system prompt for an agent that can write a compelling marketing email for a class", "The agent will be used to generate marketing emails for a class, and should be able to write compelling emails that will engage the students and encourage them to sign up for the class. The school is The Multiverse School, our website is https://themultiverse.school and you can find our next upcoming classes at https://themultiverse.school/classes and the classes mostly revolve around AI and software engineering and cybersecurity.", "Write a system prompt for an agent that can write a compelling marketing email for a class."))

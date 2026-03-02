"""
Base definitions and registry for Agent Tools.
Tools are strictly segregated into "READ_ONLY" (safe for autonomous execution)
and "ACTION" (requires explicit user confirmation and costs $$).
"""
import inspect
import json
from enum import Enum
from typing import Any, Callable, Dict, List

class ToolCategory(Enum):
    READ_ONLY = "read_only"
    ACTION = "action"

class AgentTool:
    def __init__(self, name: str, description: str, category: ToolCategory, func: Callable, schema: dict):
        self.name = name
        self.description = description
        self.category = category
        self.func = func
        self.schema = schema
        
    def to_openai_schema(self) -> dict:
        """Returns the schema formatted for OpenAI/OpenRouter tools array."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.schema
            }
        }

class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, AgentTool] = {}
        
    def register(self, name: str, description: str, category: ToolCategory, schema: dict):
        """Decorator to register a function as an Agent Tool."""
        def decorator(func: Callable):
            self._tools[name] = AgentTool(
                name=name,
                description=description,
                category=category,
                func=func,
                schema=schema
            )
            return func
        return decorator
        
    def get_tool(self, name: str) -> AgentTool | None:
        return self._tools.get(name)
        
    def get_all_schemas(self, categories: list[ToolCategory] | None = None) -> list[dict]:
        """Get schemas for the prompt, optionally filtering by category."""
        return [
            tool.to_openai_schema()
            for tool in self._tools.values()
            if not categories or tool.category in categories
        ]

# Global registry instance
agent_tools = ToolRegistry()

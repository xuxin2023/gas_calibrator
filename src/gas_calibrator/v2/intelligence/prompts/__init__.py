from pathlib import Path


PROMPTS_DIR = Path(__file__).parent


def load_prompt(name: str) -> str:
    """Load a prompt template by filename."""

    return (PROMPTS_DIR / name).read_text(encoding="utf-8")


__all__ = ["PROMPTS_DIR", "load_prompt"]

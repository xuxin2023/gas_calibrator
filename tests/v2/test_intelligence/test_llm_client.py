from gas_calibrator.v2.intelligence.llm_client import LLMConfig, MockLLMClient, create_llm_client


def test_mock_llm_complete_and_chat() -> None:
    client = MockLLMClient(LLMConfig(provider="mock", model="mock"))

    complete_text = client.complete("hello world")
    chat_text = client.chat([{"role": "user", "content": "hello"}])

    assert complete_text.startswith("[Mock Response]")
    assert "1 messages" in chat_text


def test_create_llm_client_returns_mock() -> None:
    client = create_llm_client(LLMConfig(provider="mock", model="mock"))
    assert isinstance(client, MockLLMClient)

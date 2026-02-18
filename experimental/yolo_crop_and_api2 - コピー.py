def call_api2_openai(
    system_text: str,
    image_paths: List[Path],
    model: str,
    max_output_tokens: int = 220
) -> str:
    """
    OpenAI Responses API で vision 入力（画像）＋テキスト生成。
    429(TPM) は数秒待って自動リトライする。
    """
    try:
        from openai import OpenAI
        from openai import RateLimitError
    except Exception as e:
        raise SystemExit(f"[ERR] openai python package not found. install with: pip3 install --user openai\n{e}")

    import time
    import re
    import base64

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("[ERR] OPENAI_API_KEY is not set")

    client = OpenAI(api_key=api_key)

    # 画像を data URL として渡す（jpeg）
    content = []
    content.append({
        "type": "input_text",
        "text": "以下はクロップ後（9:16）の連続静止画です。この世界だけを根拠に文章を書いてください。"
    })
    for p in image_paths:
        b64 = base64.b64encode(p.read_bytes()).decode("ascii")
        content.append({
            "type": "input_image",
            "image_url": f"data:image/jpeg;base64,{b64}"
        })

    last_err = None
    resp = None

    for attempt in range(1, 6):  # 最大5回
        try:
            resp = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": system_text},
                    {"role": "user", "content": content},
                ],
                max_output_tokens=max_output_tokens,
            )
            last_err = None
            break

        except RateLimitError as e:
            last_err = e
            msg = str(e)
            m = re.search(r"try again in ([0-9.]+)s", msg, re.IGNORECASE)
            wait_s = float(m.group(1)) + 0.5 if m else (2.0 + attempt)
            print(f"[RATE_LIMIT] attempt={attempt}/5 sleep={wait_s:.3f}s")
            time.sleep(wait_s)

    if last_err is not None:
        raise last_err

    # textを結合
    out_text = ""
    for item in resp.output:
        if item.type == "message":
            for c in item.content:
                if c.type == "output_text":
                    out_text += c.text

    return out_text.strip()

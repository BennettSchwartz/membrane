import json
import sys

def main() -> int:
    payload = json.load(sys.stdin)
    model_name = payload.get("model") or "all-MiniLM-L6-v2"
    texts = payload.get("texts") or []
    if not isinstance(texts, list):
        raise ValueError("texts must be a list")

    try:
        from sentence_transformers import SentenceTransformer
    except Exception as exc:  # pragma: no cover
        raise SystemExit(
            "sentence-transformers not installed. Run: python3 -m pip install -r tools/eval/requirements.txt"
        ) from exc

    model = SentenceTransformer(model_name)
    embeddings = model.encode(texts, normalize_embeddings=True)
    out = {"embeddings": embeddings.tolist(), "model": model_name}
    json.dump(out, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())

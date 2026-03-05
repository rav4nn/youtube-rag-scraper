import os
import json

def semantic_search(question, index_file, rag_file, top_k=5):
    """Query the FAISS knowledge base with a natural language question.
    
    Args:
        question (str): The user's question.
        index_file (str): Path to the .faiss index file.
        rag_file (str): Path to the _rag.jsonl file.
        top_k (int): Number of top results to return.
        
    Returns:
        list: Top matching chunks with metadata.
    """
    try:
        from sentence_transformers import SentenceTransformer
        import faiss
        import numpy as np
    except ImportError:
        print("\n❌ ERROR: Required ML libraries for semantic search are missing.")
        print("   Install them with: pip install sentence-transformers faiss-cpu numpy")
        return []

    if not os.path.exists(index_file):
        print(f"\n❌ ERROR: FAISS index not found at {index_file}")
        print("   Run with --knowledge-base first to build the index.")
        return []
        
    if not os.path.exists(rag_file):
        print(f"\n❌ ERROR: RAG dataset not found at {rag_file}")
        return []

    # Load RAG chunks
    chunks = []
    with open(rag_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                chunks.append(json.loads(line))
    
    if not chunks:
        print("   No chunks found in RAG dataset.")
        return []

    # Load FAISS index
    index = faiss.read_index(index_file)

    # Embed the question
    model = SentenceTransformer("all-MiniLM-L6-v2")
    query_embedding = model.encode([question])

    # Search
    distances, indices = index.search(query_embedding, min(top_k, len(chunks)))

    results = []
    for i, idx in enumerate(indices[0]):
        if idx < len(chunks):
            chunk = chunks[idx]
            chunk["score"] = float(distances[0][i])
            results.append(chunk)

    return results


def print_search_results(question, results):
    """Pretty-print semantic search results to stdout."""
    print(f"\n{'=' * 60}")
    print(f"🔍 Question:")
    print(f"   {question}")
    print(f"{'=' * 60}")
    
    if not results:
        print("\n   No results found. Build a knowledge base first with --knowledge-base.")
        return
        
    print(f"\n📚 Top {len(results)} Results:\n")
    
    for i, result in enumerate(results):
        title = result.get("title", "Unknown")
        channel = result.get("channel", "Unknown")
        text = result.get("text", "")
        
        print(f"  [{i+1}] {channel} – {title}")
        # Show a preview of the chunk (first 200 chars)
        preview = text[:200] + "..." if len(text) > 200 else text
        print(f"      \"{preview}\"")
        print()

import sys
import fitz  # PyMuPDF

def main():
    if len(sys.argv) < 2:
        print("Usage: pdf_to_text.py <pdf_path>", file=sys.stderr)
        sys.exit(2)

    pdf_path = sys.argv[1]
    doc = fitz.open(pdf_path)

    chunks = []
    for i, page in enumerate(doc, start=1):
        text = page.get_text("text") or ""
        text = text.replace("\x00", "")
        chunks.append(f"\n\n[PAGE {i}]\n{text}")

    out = "\n".join(chunks).strip()
    if not out:
        out = "[NO_TEXT_EXTRACTED]"
    print(out)

if __name__ == "__main__":
    main()

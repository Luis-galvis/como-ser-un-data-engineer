import sys, json, re
import pymupdf

doc = pymupdf.open(sys.argv[1])
out = {}
for i, page in enumerate(doc, start=1):
    for line in page.get_text().splitlines():
        s = re.sub(r"\s+", "", line)
        m_cap = re.fullmatch(r"(?:CAP[ÍI\u00cd\ufffd]TULO|CAPITULO)([0-9A-Z.]+)", s, re.IGNORECASE)
        if m_cap:
            out.setdefault(f"CAP_{m_cap.group(1).upper()}", i)
        m_part = re.fullmatch(r"PARTE([IVX]+)", s, re.IGNORECASE)
        if m_part:
            out.setdefault(f"PARTE_{m_part.group(1).upper()}", i)
print(json.dumps({"pages": len(doc), "labels": out}))

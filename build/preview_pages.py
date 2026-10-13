import sys, re, pymupdf
d = pymupdf.open('libro/libro-digital.pdf')
labels = {}
for i, p in enumerate(d, 1):
    for l in p.get_text().splitlines():
        s = l.strip()
        if re.fullmatch(r'(CAPÍTULO|PARTE) [0-9A-Z.]+', s) and s not in labels:
            labels[s] = i
print(len(d), labels)
pages = [int(x) for x in sys.argv[1:]]
for n in pages:
    d[n - 1].get_pixmap(dpi=75).save(f'scratch/p{n}.png')

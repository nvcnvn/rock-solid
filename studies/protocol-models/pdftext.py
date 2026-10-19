# Prints the text of a PDF whose content streams are Flate-compressed, using only the standard library.
# Good enough to grep; words may be split where the PDF positions glyphs separately.
import re, sys, zlib
data = open(sys.argv[1], 'rb').read()
out = []
for m in re.finditer(rb'stream\r?\n(.*?)\r?\nendstream', data, re.S):
    try:
        s = zlib.decompress(m.group(1))
    except Exception:
        continue
    if b'TJ' not in s and b'Tj' not in s:
        continue
    for t in re.finditer(rb'\[(.*?)\]\s*TJ|\((.*?)\)\s*Tj|(T\*|ET)', s, re.S):
        if t.group(3):
            out.append('\n'); continue
        seg = t.group(1) if t.group(1) is not None else b'(' + t.group(2) + b')'
        parts = re.findall(rb'\(((?:\\.|[^\\)])*)\)|(-?\d+\.?\d*)', seg)
        line = ''
        for p, n in parts:
            if p:
                p = re.sub(rb'\\([()\\])', rb'\1', p)
                line += p.decode('latin-1')
            elif n and float(n) < -200:
                line += ' '
        out.append(line)
print(''.join(out))

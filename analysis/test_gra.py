# Copyright (c) Microsoft Corporation.
from trafilatura import extract

text = open('data/obelics_omni/analysis/test.html').read()

OUTPUT_FORMAT = 'html'

html = extract(
    text,
    favor_precision=True,
    include_comments=False,
    deduplicate=True,
    include_tables=True,
    include_images=True,
    include_links=True,
    output_format=OUTPUT_FORMAT
)

with open(f'gra_{OUTPUT_FORMAT}.txt', 'w') as file:
    print(type(html))
    file.write(html)
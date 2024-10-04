


# download to gzip

# /data/mengcliu/DATASET/cc/crawl-data/CC-MAIN-2024-30/segments/1720763514387.30/warc/CC-MAIN-20240712094214-20240712124214-00000.warc.gz

90k * 0.7G = 63T

first warc

62974 records
20991 responses


record -> page, encoding -> 46ms 

page -> fast

encoding needs parallel


Omni processing:

 - READER: 🕷 Warc                                                                                                                                                                                                                                              
    Runtime: (3.06%) 1 minute and 40 seconds±2 seconds/task, min=1 minute and 38 seconds, max=1 minute and 42 seconds [0.40 milliseconds±4.17 milliseconds/doc]                                                                                                  
    Stats: {input_files: 8, doc_len: 23138691922 [min=1, max=1048576, 155744.79±198242/doc], documents: 148560 [min=18453, max=18740, 18570.00±112/input_file]}                                                                                                  
🔻 - FILTER: 😈 Url-filter                                                                                                                                                                                                                                       
    Runtime: (0.47%) 15 seconds±0 seconds/task, min=15 seconds, max=15 seconds [0.21 milliseconds±10.41 milliseconds/doc]                                                                                                                                        
    Stats: {total: 148568, forwarded: 147293, doc_len: 22925947573 [min=1, max=1048576, 155648.59±197707/doc], dropped: 1275, dropped_domain: 724, dropped_hard_blacklisted: 435, dropped_blacklisted_subword: 81, dropped_soft_blacklisted: 23, dropped_subdomai
n: 12}                                                                                                                                                                                                                                                           
🛢 - EXTRAC: ⛏ Trafilatura                                                                                                                                                                                                                                        
    Runtime: (89.36%) 48 minutes and 51 seconds±3 minutes and 36 seconds/task, min=46 minutes and 18 seconds, max=51 minutes and 24 seconds [39.81 milliseconds±30.47 milliseconds/doc] 
    Stats: {total: 147293, forwarded: 118874, doc_len: 252541641 [min=1, max=488728, 2124.45±4841/doc], dropped: 16703}


 - FILTER: 🌍 Language ID
    Runtime: (1.44%) 47 seconds±0 seconds/task, min=46 seconds, max=47 seconds [0.79 milliseconds±1.64 milliseconds/doc]
    Stats: {total: 118874, dropped: 75385, forwarded: 43489, doc_len: 100146900 [min=1, max=288270, 2302.81±4447/doc]}
🔻 - FILTER: 👯 Gopher Repetition
    Runtime: (3.05%) 1 minute and 40 seconds±0 seconds/task, min=1 minute and 39 seconds, max=1 minute and 40 seconds [4.61 milliseconds±8.79 milliseconds/doc]
    Stats: {total: 43489, forwarded: 32802, doc_len: 83933713 [min=1, max=151271, 2558.80±4146/doc], dropped: 10687, dropped_top_3_gram: 1719, dropped_top_2_gram: 2223, dropped_top_4_gram: 1386, dropped_dup_line_frac: 2895, dropped_duplicated_5_n_grams: 135
1, dropped_duplicated_8_n_grams: 75, dropped_dup_line_char_frac: 650, dropped_duplicated_7_n_grams: 81, dropped_duplicated_9_n_grams: 94, dropped_duplicated_6_n_grams: 125, dropped_duplicated_10_n_grams: 88}
🔻 - FILTER: 🥇 Gopher Quality
    Runtime: (1.49%) 48 seconds±0 seconds/task, min=48 seconds, max=49 seconds [2.97 milliseconds±3.84 milliseconds/doc]
    Stats: {total: 32802, dropped: 11682, dropped_gopher_below_alpha_threshold: 6632, forwarded: 21120, doc_len: 65818538 [min=244, max=151271, 3116.41±4540/doc], dropped_gopher_short_doc: 4094, dropped_gopher_too_many_end_ellipsis: 773, dropped_gopher_too_
many_bullets: 38, dropped_gopher_above_avg_threshold: 116, dropped_gopher_too_many_hashes: 5, dropped_gopher_too_many_ellipsis: 10, dropped_gopher_enough_stop_words: 12, dropped_gopher_below_avg_threshold: 2}


🔻 - FILTER: ⛰ C4 Quality
    Runtime: (0.27%) 8 seconds±0 seconds/task, min=8 seconds, max=8 seconds [0.83 milliseconds±0.98 milliseconds/doc]
    Stats: {total: 21120, line-total: 371241, line-filter-too_few_words: 45651, line-kept: 323348, dropped: 1937, dropped_too_few_sentences: 1848, forwarded: 19183, doc_len: 63029861 [min=218, max=150980, 3285.71±4550/doc], line-filter-policy: 1926, dropped
_curly_bracket: 61, line-filter-javascript: 224, dropped_lorem_ipsum: 28, line-filter-too_long_word: 3}
🔻 - FILTER: 🍷 FineWeb Quality
    Runtime: (0.76%) 24 seconds±0 seconds/task, min=24 seconds, max=25 seconds [2.61 milliseconds±3.34 milliseconds/doc]
    Stats: {total: 19183, forwarded: 17258, doc_len: 57009533 [min=221, max=150980, 3303.37±4472/doc], dropped: 1925, dropped_line_punct_ratio: 866, dropped_char_dup_ratio: 948, dropped_short_line_ratio: 111}
💽 - WRITER: 🐿 Jsonl
    Runtime: (0.10%) 3 seconds±0 seconds/task, min=3 seconds, max=3 seconds [0.38 milliseconds±0.54 milliseconds/doc]
    Stats: {XXXXX.jsonl.gz: 17258, total: 17258, doc_len: 57009533 [min=221, max=150980, 3303.37±4472/doc]}

-> 1 snapshot: 123.75G

===== BASE =====

2024-08-17 23:48:27.237 | INFO     | datatrove.executor.local:_launch_run_for_rank:79 - 2/2 tasks completed.                                                                                                                                                                               2024-08-17 23:48:27.451 | SUCCESS  | datatrove.executor.local:run:146 -                                                                                                                                                                                                                    
                                                                      
📉📉📉 Stats: All 2 tasks 📉📉📉                                                                                                             
                                                                                                                                                                                                                                                                                           Total Runtime: 56 minutes and 58 seconds ± 4 seconds/task                                                                                                                                                                                                                                  
                                                                      
📖 - READER: 🕷 Warc                                                                                                                          
    Runtime: (3.01%) 1 minute and 43 seconds±1 second/task, min=1 minute and 41 seconds, max=1 minute and 44 seconds [0.41 milliseconds±4.50 milliseconds/doc]                                                                                                                                 Stats: {input_files: 8, doc_len: 23138691922 [min=1, max=1048576, 155744.79±198242/doc], documents: 148560 [min=18453, max=18740, 18570.00±112/input_file]}                                                                                                                            
🔻 - FILTER: 😈 Url-filter                                            
    Runtime: (0.45%) 15 seconds±0 seconds/task, min=15 seconds, max=15 seconds [0.21 milliseconds±10.16 milliseconds/doc]                    
    Stats: {total: 148568, forwarded: 147293, doc_len: 22925947573 [min=1, max=1048576, 155648.59±197707/doc], dropped: 1275, dropped_domain: 724, dropped_hard_blacklisted: 435, dropped_blacklisted_subword: 81, dropped_soft_blacklisted: 23, dropped_subdomain: 12}                    
🛢 - EXTRAC: ⛏ Trafilatura                                             
    Runtime: (89.81%) 51 minutes and 9 seconds±4 seconds/task, min=51 minutes and 6 seconds, max=51 minutes and 12 seconds [41.68 milliseconds±30.68 milliseconds/doc]                                                                                                                     
    Stats: {total: 147293, forwarded: 118121, doc_len: 230961409 [min=1, max=341410, 1955.30±4313/doc], dropped: 16608} -> 80.1% valid

🔻 - FILTER: 🌍 Language ID                                                                                                                                                                                                                                                                
    Runtime: (1.29%) 44 seconds±0 seconds/task, min=43 seconds, max=44 seconds [0.75 milliseconds±1.53 milliseconds/doc]                     
    Stats: {total: 118121, dropped: 73764, forwarded: 44357, doc_len: 96388050 [min=1, max=288270, 2173.01±4248/doc]}                        
🔻 - FILTER: 👯 Gopher Repetition                                     
    Runtime: (2.85%) 1 minute and 37 seconds±0 seconds/task, min=1 minute and 36 seconds, max=1 minute and 38 seconds [4.40 milliseconds±8.50 milliseconds/doc]                                                                                                                            
    Stats: {total: 44357, forwarded: 33061, doc_len: 81393264 [min=1, max=149799, 2461.91±4037/doc], dropped: 11296, dropped_top_3_gram: 1713, dropped_top_2_gram: 2694, dropped_duplicated_5_n_grams: 1194, dropped_dup_line_frac: 3059, dropped_top_4_gram: 1557, dropped_dup_line_char_f
rac: 688, dropped_duplicated_7_n_grams: 73, dropped_duplicated_10_n_grams: 88, dropped_duplicated_9_n_grams: 83, dropped_duplicated_6_n_grams: 85, dropped_duplicated_8_n_grams: 62}                                                                                                       
🔻 - FILTER: 🥇 Gopher Quality                                        
    Runtime: (1.41%) 48 seconds±0 seconds/task, min=47 seconds, max=48 seconds [2.91 milliseconds±3.82 milliseconds/doc]                     
    Stats: {total: 33061, dropped: 9718, dropped_gopher_below_alpha_threshold: 4748, forwarded: 23343, doc_len: 69555368 [min=244, max=149799, 2979.71±4380/doc], dropped_gopher_too_many_bullets: 160, dropped_gopher_short_doc: 3916, dropped_gopher_too_many_end_ellipsis: 849, dropped_
gopher_too_many_hashes: 6, dropped_gopher_too_many_ellipsis: 11, dropped_gopher_enough_stop_words: 23, dropped_gopher_below_avg_threshold: 4, dropped_gopher_above_avg_threshold: 1}                                                                                                       
🔻 - FILTER: ⛰ C4 Quality                                             
    Runtime: (0.29%) 9 seconds±0 seconds/task, min=9 seconds, max=10 seconds [0.84 milliseconds±2.59 milliseconds/doc]                       
    Stats: {total: 23343, line-total: 410628, line-filter-too_few_words: 50587, line-kept: 357714, dropped: 2209, dropped_too_few_sentences: 2105, forwarded: 21134, doc_len: 66529564 [min=160, max=149508, 3147.99±4348/doc], line-filter-policy: 2032, dropped_curly_bracket: 75, line-f
ilter-javascript: 190, dropped_lorem_ipsum: 29, line-filter-too_long_word: 1}                                                                
🔻 - FILTER: 🍷 FineWeb Quality                                       
    Runtime: (0.78%) 26 seconds±0 seconds/task, min=26 seconds, max=26 seconds [2.52 milliseconds±3.23 milliseconds/doc]                     
    Stats: {total: 21134, forwarded: 18879, doc_len: 59675237 [min=213, max=149508, 3160.93±4259/doc], dropped: 2255, dropped_line_punct_ratio: 1061, dropped_char_dup_ratio: 1057, dropped_short_line_ratio: 137}                                                                         
💽 - WRITER: 🐿 Jsonl                                                  
    Runtime: (0.10%) 3 seconds±0 seconds/task, min=3 seconds, max=3 seconds [0.38 milliseconds±0.49 milliseconds/doc]                        
    Stats: {XXXXX.jsonl.gz: 18879, total: 18879, doc_len: 59675237 [min=213, max=149508, 3160.93±4259/doc]}


Examine

>>> lines_base = open('/data/mengcliu/DATASET/cc/base_processing-output-CC-MAIN-2024-30-00000.jsonl').read().splitlines()
>>> len(lines_base)
9502
>>> lines_omni = open('/data/mengcliu/DATASET/cc/omni_processing-output-CC-MAIN-2024-30-00000.jsonl').read().splitlines()
>>> len(lines_omni)
8593



Total Runtime: 10 minutes and 38 seconds                                                                                                                                                                                                          [3/1895]
                                                              
📖 - READER: 🕷 Warc                                                                                                          
    Runtime: (6.13%) 39 seconds [0.62 milliseconds±9.02 milliseconds/doc]                                                                                                                                                                                 
    Stats: {input_files: 1, doc_len: 2937782740 [min=1, max=1048576, 158507.76±202478/doc], documents: 18533 [18533.00/input_file]}                                                                                                                       
🔻 - FILTER: 😈 Url-filter                                    
    Runtime: (0.90%) 5 seconds [0.31 milliseconds±17.97 milliseconds/doc]                                                    
    Stats: {total: 18534, forwarded: 18388, doc_len: 2910224826 [min=1, max=1048576, 158267.61±201849/doc], dropped: 146, dropped_domain: 72, dropped_subdomain: 1, dropped_blacklisted_subword: 14, dropped_hard_blacklisted: 57, dropped_soft_blackliste
d: 2}                                                         
🛢 - EXTRAC: ⛏ Trafilatura                                     
    Runtime: (91.86%) 9 minutes and 46 seconds [31.88 milliseconds±37.51 milliseconds/doc]                                                                                                                                                                
    Stats: {total: 18388, forwarded: 17305, doc_len: 64689172 [min=11, max=162537, 3738.18±7385/doc]}

    17305 / 18388 = 



================

for chunk-1:

n: 17405
n_image: 28227
n_audio: 26
n_video: 72
n_media: 42050

media: {'a': 40403}

image_suffix = [
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".bmp",
    ".ico",
    ".tiff",
    ".tif"
]


[('jpg', 1045), ('gif', 34), ('png', 131), ('svg', 15), ('tif', 2), ('bmp', 17), ('jpeg', 69), ('webp', 12)]

'figure' is removed due to img is in, ignore the relationship
'picture' is removed: only use the fall back image
'canvas' removed due to not accessible in static

parse successful:

print(len(documents))
print(len(text_documents))
print(len(image_documents))
print(len(video_documents))
print(len(audio_documents))


12991
6880
6094
34
13

======== apply magic and all filters =========

Total Runtime: 11 minutes and 45 seconds                                                                                                                                                                                                     [24/1902]
                                                                                                                           
📖 - READER: 🕷 Warc                                                                                                                                                                                                                                   
    Runtime: (5.12%) 36 seconds [0.57 milliseconds±7.19 milliseconds/doc]                                                                                                                                                                             
    Stats: {input_files: 1, doc_len: 2937782740 [min=1, max=1048576, 158507.76±202478/doc], documents: 18533 [18533.00/input_file]}
🔻 - FILTER: 😈 Url-filter
    Runtime: (0.81%) 5 seconds [0.31 milliseconds±19.45 milliseconds/doc]
    Stats: {total: 18534, forwarded: 18388, doc_len: 2910224826 [min=1, max=1048576, 158267.61±201849/doc], dropped: 146, dropped_domain: 72, dropped_subdomain: 1, dropped_blacklisted_subword: 14, dropped_hard_blacklisted: 57, dropped_soft_blackl
isted: 2}
🛢 - EXTRAC: ⛏ MagicExtractor
    Runtime: (82.90%) 9 minutes and 44 seconds [31.80 milliseconds±36.33 milliseconds/doc]
    Stats: {total: 18388, forwarded: 17299, doc_len: 64609011 [min=11, max=162537, 3734.84±7357/doc]}
 - FILTER: MediaFilter                                                                                                                                                                                       
    Runtime: (3.92%) 27 seconds [1.60 milliseconds±9.17 milliseconds/doc]
    Stats: {total: 17299, dropped: 4501, dropped_empty: 4501, forwarded: 12798, doc_len: 26573645 [min=1, max=147783, 2076.39±4143/doc]}
🔻 - FILTER: 🌍 Language ID
    Runtime: (1.32%) 9 seconds [0.73 milliseconds±1.87 milliseconds/doc]
    Stats: {total: 12798, dropped: 8085, forwarded: 4713, doc_len: 10940549 [min=3, max=70669, 2321.36±3940/doc]}
🔻 - FILTER: 👯 Gopher Repetition
    Runtime: (2.99%) 21 seconds [4.47 milliseconds±12.79 milliseconds/doc]
    Stats: {total: 4713, forwarded: 3991, doc_len: 9472830 [min=7, max=51696, 2373.55±3494/doc], dropped: 722, dropped_top_3_gram: 94, dropped_dup_line_frac: 207, dropped_top_2_gram: 100, dropped_duplicated_5_n_grams: 75, dropped_top_4_gram: 160,
 dropped_dup_line_char_frac: 53, dropped_duplicated_8_n_grams: 4, dropped_duplicated_9_n_grams: 5, dropped_duplicated_7_n_grams: 9, dropped_duplicated_10_n_grams: 6, dropped_duplicated_6_n_grams: 9}
 🔻 - FILTER: 🥇 Gopher Quality
    Runtime: (1.59%) 11 seconds [2.81 milliseconds±6.61 milliseconds/doc]
    Stats: {total: 3991, forwarded: 2769, doc_len: 8323119 [min=255, max=51696, 3005.82±3815/doc], dropped: 1222, dropped_gopher_below_alpha_threshold: 580, dropped_gopher_short_doc: 578, dropped_gopher_too_many_end_ellipsis: 63, dropped_gopher_e
nough_stop_words: 1}
🔻 - FILTER: ⛰ C4 Quality
    Runtime: (0.34%) 2 seconds [0.87 milliseconds±0.90 milliseconds/doc]
    Stats: {total: 2769, line-total: 73682, line-kept: 52916, forwarded: 2570, doc_len: 7931328 [min=192, max=51560, 3086.12±3773/doc], line-filter-too_few_words: 20577, dropped: 199, dropped_too_few_sentences: 189, dropped_lorem_ipsum: 4, line-f
ilter-policy: 160, line-filter-javascript: 19, dropped_curly_bracket: 6}
🔻 - FILTER: 🍷 FineWeb Quality
    Runtime: (0.87%) 6 seconds [2.40 milliseconds±2.90 milliseconds/doc]
    Stats: {total: 2570, forwarded: 2237, doc_len: 7004125 [min=209, max=51560, 3131.03±3798/doc], dropped: 333, dropped_line_punct_ratio: 115, dropped_char_dup_ratio: 193, dropped_short_line_ratio: 25}


============ Fixing all bugs ==========

📉📉📉 Stats: All 1 tasks 📉📉📉                                                                                                 │
                                                                                                                                 │
Total Runtime: 12 minutes and 3 seconds                                                                                          │
                                                                                                                                 │
📖 - READER: 🕷 Warc                                                                                                              │
    Runtime: (5.15%) 37 seconds [0.59 milliseconds±7.69 milliseconds/doc]                                                        │
    Stats: {input_files: 1, doc_len: 2937782740 [min=1, max=1048576, 158507.76±202478/doc], documents: 18533 [18533.00/input_file│
]}

🔻 - FILTER: 😈 Url-filter                                                                                                       │
    Runtime: (0.77%) 5 seconds [0.30 milliseconds±18.66 milliseconds/doc]                                                        │
    Stats: {total: 18534, forwarded: 18388, doc_len: 2910224826 [min=1, max=1048576, 158267.61±201849/doc], dropped: 146, dropped│
_domain: 72, dropped_subdomain: 1, dropped_blacklisted_subword: 14, dropped_hard_blacklisted: 57, dropped_soft_blacklisted: 2}   │
🛢 - EXTRAC: ⛏ MagicExtractor                                                                                                     │
    Runtime: (80.10%) 9 minutes and 39 seconds [31.53 milliseconds±37.10 milliseconds/doc]                                       │
    Stats: {total: 18388, forwarded: 17333, doc_len: 64806721 [min=11, max=162537, 3738.92±7407/doc]}                            │
🔻 - FILTER: MediaFilter                                                                                                         │
    Runtime: (3.88%) 28 seconds [1.62 milliseconds±8.13 milliseconds/doc]                                                        │
    Stats: {total: 17333, dropped: 4412, dropped_empty: 4412, forwarded: 12921, doc_len: 27219881 [min=1, max=147950, 2106.64±421│
9/doc]}

🔻 - FILTER: 🌍 Language ID                                                                                             [15/1850]│
    Runtime: (1.85%) 13 seconds [1.04 milliseconds±2.24 milliseconds/doc]                                                        │
    Stats: {total: 12921, dropped: 8109, forwarded: 4812, doc_len: 11222115 [min=2, max=81384, 2332.11±4031/doc]}                │
🔻 - FILTER: 👯 Gopher Repetition                                                                                                │
    Runtime: (3.10%) 22 seconds [4.67 milliseconds±12.81 milliseconds/doc]                                                       │
    Stats: {total: 4812, forwarded: 3989, doc_len: 9718391 [min=2, max=51800, 2436.30±3647/doc], dropped: 823, dropped_top_4_gram│
: 165, dropped_dup_line_frac: 214, dropped_top_2_gram: 194, dropped_duplicated_5_n_grams: 71, dropped_top_3_gram: 94, dropped_dup│
_line_char_frac: 52, dropped_duplicated_6_n_grams: 9, dropped_duplicated_8_n_grams: 5, dropped_duplicated_7_n_grams: 6, dropped_d│
uplicated_10_n_grams: 7, dropped_duplicated_9_n_grams: 6}

🔻 - FILTER: 🥇 Gopher Quality                                                                                                   │
    Runtime: (1.64%) 11 seconds [2.98 milliseconds±6.59 milliseconds/doc]                                                        │
    Stats: {total: 3989, forwarded: 2885, doc_len: 8628918 [min=255, max=51800, 2990.96±3853/doc], dropped: 1104, dropped_gopher_│
short_doc: 552, dropped_gopher_below_alpha_threshold: 489, dropped_gopher_too_many_end_ellipsis: 61, dropped_gopher_enough_stop_w│
ords: 2}                                                                                                                         │
🔻 - FILTER: ⛰ C4 Quality                                                                                                        │
    Runtime: (2.40%) 17 seconds [6.03 milliseconds±264.82 milliseconds/doc]                                                      │
    Stats: {total: 2885, line-total: 80415, line-kept: 57346, forwarded: 2696, doc_len: 8195828 [min=209, max=51635, 3040.00±3800│
/doc], line-filter-too_few_words: 22867, dropped: 189, dropped_lorem_ipsum: 4, line-filter-policy: 171, line-filter-javascript: 2│
0, dropped_too_few_sentences: 178, dropped_curly_bracket: 7}

🔻 - FILTER: 🍷 FineWeb Quality                                                                                                  │
    Runtime: (0.90%) 6 seconds [2.42 milliseconds±2.92 milliseconds/doc]                                                         │
    Stats: {total: 2696, forwarded: 2302, doc_len: 7029646 [min=209, max=51635, 3053.71±3773/doc], dropped: 394, dropped_char_dup│
_ratio: 230, dropped_short_line_ratio: 29, dropped_line_punct_ratio: 135}                                                        │
💽 - WRITER: 🐿 Jsonl                                                                                                             │
    Runtime: (0.20%) 1 second [0.63 milliseconds±0.69 milliseconds/doc]                                                          │
    Stats: {XXXXX.jsonl.gz: 2302, total: 2302, doc_len: 7029646 [min=209, max=51635, 3053.71±3773/doc]}

============= Stat for crwal 30 =========

82449 in 90000 step 2.2 valid download

157B tokens step 3.1

325608 documents * 700 -> 227925600 documents

print(n_text_doc)
print(n_image_doc)
print(n_video_doc)
print(n_audio_doc)

157722
167293
925
831


deduped output chunk 0:

325000
n_image 524031
n_video 1273
n_audio 1448


download image: 

{'failed_to_download': 455, 'failed_to_resize': 429, 'success': 2711}
3595

508M

change to:

image_size=self.config.get('image_size', 1344),
resize_only_if_bigger=True,
resize_mode='keep_ratio_largest',

364M -> 35T

{'failed_to_download': 454, 'failed_to_resize': 429, 'success': 2712}
3595
width: 1344 1
heights: 1344 1
sizes: 1806336 1

change to

min_image_size=150,

362M

{'failed_to_download': 459, 'failed_to_resize': 769, 'success': 2367}
3595
width: 1344 72
heights: 1344 48
sizes: 1806336 22500

max_aspect_ratio=10.0

360M


AUDIO:

wav:

7 local -> 6239180


Failed to download {'url': 'https://www.buzzsprout.com/1567345/9153577-episode-44-john-eresman-founder-gradient-beverages-corp.mp3?_=1', 'caption': '775_0'}: 403 Client Error: Forbidden for url: https://www.buzzsprout.com/1567345/9153577-episode-44-john-eresman-founder-gradie
nt-beverages-corp.mp3?_=1

mp3

7 local -> 579780

43 valid in 54 audios

change to img2dataset download_url

46 valid in 54 audios








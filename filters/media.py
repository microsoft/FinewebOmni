# Copyright (c) Microsoft Corporation.
import os

from datatrove.data import Media
from datatrove.data import Document
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter
from urllib.parse import urlsplit, urljoin
# from urllib.parse import urlparse, urlsplit

from bs4 import BeautifulSoup
from bs4 import NavigableString, CData, Tag

MEDIA_RESOURCE = [
    {
        'name': 'image',
        'type': 0,
        'text': '<image_{id}>',
        'suffix': [
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
    },
    {
        'name': 'video',
        'type': 1,
        'text': '<video_{id}>',
        'suffix': [
            ".mp4",
            ".webm",
            ".ogg",
            ".mov",
            ".avi",
            ".mkv",
            ".flv",
            ".wmv"
        ]
    },
    {
        'name': 'audio',
        'type': 2,
        'text': '<audio_{id}>',
        'suffix': [
            ".mp3",
            ".wav",
            ".ogg",
            ".aac",
            ".flac",
            ".m4a",
            ".webm"
        ]
    },
    # {
    #     'name': 'text',
    #     'type': 3,
    #     'text': '<audio_{id}>',
    #     'suffix': [
    #         ".mp3",
    #         ".wav",
    #         ".ogg",
    #         ".aac",
    #         ".flac",
    #         ".m4a",
    #         ".webm"
    #     ]
    # }
]


# def complete_url(url):
#     return urljoin(base_url, url)


def get_image_media(tag):
    
    name = tag.name
    
    url = ""
    alt = ""
    
    if name == 'img':
        url = tag.attrs.get('src', '')
        alt = tag.attrs.get('alt', '')
    
    if url:
        
        # url = complete_url(url, base_url)
        
        return Media(
            type=0, 
            url=url,
            alt=alt
        ), 0

    return None, -1


def get_audio_media(audio_tag):
    
    name = audio_tag.name
    
    # url = ""
    # alt = ""
    
    if name == 'audio':
        
        # Extract sources from <source> tags within the <audio> tag
        audio_sources = []
        for source in audio_tag.find_all('source'):
            src = source.get('src')
            if src:
                return Media(type=2, url=src), 2

        # If the audio source is directly in the <audio> tag
        if not audio_sources:
            src = audio_tag.get('src')
            if src:
                return Media(type=2, url=src), 2
                # audio_sources.append(src)
            
    return None, -1

def get_video_media(video_tag):
    
    name = video_tag.name
    
    # url = ""
    # alt = ""
    
    if name == 'video':
        
        # Extract sources from <source> tags within the <video> tag
        video_sources = []
        for source in video_tag.find_all('source'):
            src = source.get('src')
            if src:
                return Media(type=1, url=src), 1

        # If the video source is directly in the <video> tag
        if not video_sources:
            src = video_tag.get('src')
            if src:
                return Media(type=1, url=src), 1  
            
    return None, -1


def get_media_content(tag):
    if tag.name == 'a':
        # print(tag.attrs); exit(0)
        # return tag.attrs.get('href', '')
        href = tag.attrs.get('href', '')
        if href:
            parsed_url = urlsplit(href)
            href_path = parsed_url.path
            _, suffix = os.path.splitext(href_path)
            suffix = suffix.lower()
            for resource in MEDIA_RESOURCE:
                if suffix in resource['suffix']:
                    return Media(type=resource['type'], url=href), resource['type']
    
    return None, -1


TAG_RESOURCE = {
    'img': get_image_media,
    'video': get_video_media,
    'audio': get_audio_media,
    'a': get_media_content
}


def html2mm(document, text_types = (NavigableString, CData)):
    
    html = document.text
    url = document.metadata['url']
    
    # parsed_url = urlsplit(url)
    # base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    # base_url = url.split('/')[0]
    soup = BeautifulSoup(html, "lxml")
    
    all_text = []
    medias = []
    # media_ids = [0] * len(MEDIA_RESOURCE)
    urls_dedup = set()
    for descendant in soup.descendants:
        if type(descendant) in text_types:
            text = descendant.strip()
            if len(text) > 0:
                all_text.append(text)
                medias.append(Media(type=-1, url="", alt=text))
        elif isinstance(descendant, Tag):
            name = descendant.name

            media = None
            if name in TAG_RESOURCE:
                media, media_type = TAG_RESOURCE[name](descendant)

            if media and (media.url not in urls_dedup):
                # resource = MEDIA_RESOURCE[media_type]
                
                # text = resource['text'].format(id=media_ids[media_type])
                # all_text.append(text)
                # media.local_path = text
                # media_ids[media_type] += 1

                urls_dedup.add(media.url)

                medias.append(media)
                descendant.clear()
            
    document.media = medias
    document.text = '\n'.join(all_text)

    # print(document)
    
    if document.text:
        return True
    
    
class MediaFilter(BaseFilter):
    name = "MediaFilter"
    
    def __init__(
        self,
        exclude_text=False,
        exclusion_writer: DiskWriter = None,
        **kwargs,
    ):
        super().__init__(exclusion_writer)
        self.exclude_text = exclude_text

    def filter(self, doc):
        
        try:
            is_good_document = html2mm(doc)
        except Exception as e:
            return False, str(e)
        
        if not is_good_document:
            return False, "empty"
        else:
            if self.exclude_text and len(doc.media) == 0:
                return False, "pure-text"
            else:
                return True
            
        
        
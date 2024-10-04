# Copyright (c) Microsoft Corporation.
import os

from datatrove.data import Media
from datatrove.data import Document

from bs4 import BeautifulSoup
from bs4 import NavigableString, CData, Tag

from datatrove.pipeline.extractors.base import BaseExtractor


IMAGE_TYPE = 'img'
AUDIO_TYPE = 'audio'
VIDEO_TYPE = 'video'
MEDIA_TYPE = 'a'

# media_types = set(['a']) # set(['object', 'embed', 'a', 'iframe'])

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

audio_suffix = [
    ".mp3",
    ".wav",
    ".ogg",
    ".aac",
    ".flac",
    ".m4a",
    ".webm"
]


video_suffix = [
    ".mp4",
    ".webm",
    ".ogg",
    ".mov",
    ".avi",
    ".mkv",
    ".flv",
    ".wmv"
]

def complete_url(url, base_url):
    if not url.startswith('http'):
        return base_url + url
    return url

def get_image_media(tag, base_url):
    
    name = tag.name
    
    url = ""
    alt = ""
    
    if name == 'img':
        url = tag.attrs.get('src', '')
        alt = tag.attrs.get('alt', '')
    
    if url:
        
        url = complete_url(url, base_url)
        
        return Media(
            type=0, 
            url=url,
            alt=alt
        )

    return None



def get_audio_media(audio_tag, base_url):
    
    name = audio_tag.name
    
    # url = ""
    # alt = ""
    
    if name == 'audio':
        
        # Extract sources from <source> tags within the <audio> tag
        audio_sources = []
        for source in audio_tag.find_all('source'):
            src = source.get('src')
            if src:
                return Media(type=2, 
                             url=complete_url(src, base_url))  

        # If the audio source is directly in the <audio> tag
        if not audio_sources:
            src = audio_tag.get('src')
            if src:
                return Media(type=2, 
                             url=complete_url(src, base_url))  
                # audio_sources.append(src)
            
    return None

def get_video_media(video_tag, base_url):
    
    name = video_tag.name
    
    # url = ""
    # alt = ""
    
    if name == 'video':
        
        # Extract sources from <source> tags within the <video> tag
        video_sources = []
        for source in video_tag.find_all('source'):
            src = source.get('src')
            if src:
                return Media(type=1, 
                             url=complete_url(src, base_url))  

        # If the video source is directly in the <video> tag
        if not video_sources:
            src = video_tag.get('src')
            if src:
                return Media(type=1, 
                             url=complete_url(src, base_url))  
            
    return None


def get_media_content(tag, base_url):
    if tag.name == 'a':
        # print(tag.attrs); exit(0)
        # return tag.attrs.get('href', '')
        href = tag.attrs.get('href', '')
        if href:
            _, suffix = os.path.splitext(href)
            if suffix.lower() in image_suffix:
                return Media(type=0, url=complete_url(href, base_url))
            elif suffix.lower() in video_suffix:
                return Media(type=1, url=complete_url(href, base_url))
            elif suffix.lower() in audio_suffix:
                return Media(type=2, url=complete_url(href, base_url))
    
    return None

def html2mm(html, text_types = (NavigableString, CData)):
    n_image = 0
    n_audio = 0
    n_video = 0
    
    # html = document['text']
    # url = document['metadata']['url']
    
    soup = BeautifulSoup(html, "lxml")
    # base_url = url.split('/')[0]
    all_text = []
    medias = []
    local_urls = set()
    for descendant in soup.descendants:
        if type(descendant) in text_types:
            text = descendant.strip()
            if len(text) > 0:
                all_text.append(text)
        elif isinstance(descendant, Tag):
            name = descendant.name
            if name == IMAGE_TYPE:
                media = get_image_media(descendant)
                if media and (media.url not in local_urls):
                    all_text.append(f"<image_{n_image}>")
                    media.local_url = n_image
                    n_image += 1
                    medias.append(media)
                    local_urls.add(media.url)
                descendant.clear()
            elif name in AUDIO_TYPE:
                media = get_audio_media(descendant)
                if media and (media.url not in local_urls):
                    all_text.append(f"<audio_{n_audio}>")
                    media.local_url = n_audio
                    n_audio += 1
                    medias.append(media)
                    local_urls.add(media.url)
                descendant.clear()
            elif name in VIDEO_TYPE:
                media = get_video_media(descendant)
                if media and (media.url not in local_urls):
                    all_text.append(f"<video_{n_video}>")
                    media.local_url = n_video
                    n_video += 1
                    medias.append(media)
                    local_urls.add(media.url)
                descendant.clear()
            elif name == MEDIA_TYPE:
                media = get_media_content(descendant)
                if media and (media.url not in local_urls):
                    media_type = media.type
                    medias.append(media)
                    local_urls.add(media.url)
                    if media_type == 0:
                        media.local_url = n_image
                        n_image += 1
                    elif media_type == 1:
                        media.local_url = n_video
                        n_video += 1
                    elif media_type == 2:
                        media.local_url = n_audio
                        n_audio += 1

    document.media = medias

    return '\n'.join(all_text)
    
    # if all_text:
    #     # result = Document(
    #     #     text = '\n'.join(all_text),
    #     #     id = document['id'],
    #     #     metadata = document['metadata'],
    #     #     media = medias
    #     # )
    #     return result
    
class MMExtractor(BaseExtractor):
    name = "MMExtractor"
    
    def __init__(
        self,
        timeout: float = 0.1,
        **kwargs,
    ):
        super().__init__(timeout)
        self.kwargs = kwargs

    def extract(self, text: str) -> str:
        return extract(
            text,
            favor_precision=self.favour_precision,
            include_comments=False,
            deduplicate=self.deduplicate,
            **self.kwargs,
        )
# Copyright (c) Microsoft Corporation.
from datatrove.pipeline.readers.parquet import ParquetReader
from PIL import Image
import io
import os
from pydub import AudioSegment
from io import BytesIO

FILE = 'DATASET/cc/media/CC-MAIN-2024-30/media_type_1/00007/media/00000.parquet'
# OUTPUT = 'DATASET/cc/media/CC-MAIN-2024-30/media_type_0/00001/images-aspect-raio-gt-2/'

# def save_image(image_bytes, image_filename):

#     # Convert bytes to a BytesIO object
#     image_stream = io.BytesIO(image_bytes)

#     # Open the image using PIL
#     image = Image.open(image_stream)

#     # Display the image (optional)
#     image.save(image_filename)

if __name__ == "__main__":
    
    batch_size = 8
    n = 0
    status = {}
    is_printed = False
    
    widths = []
    heights = []
    sizes = []
    
    
    import pyarrow.parquet as pq
    
    # os.makedirs(OUTPUT, exist_ok=True)

    with open(FILE, "rb") as f:
        with pq.ParquetFile(f) as pqf:
            li = 0
            # columns = [self.text_key, self.id_key] if not self.read_metadata else None
            for batch in pqf.iter_batches(batch_size=batch_size): # , columns=columns
                documents = []
                # with self.track_time("batch"):
                for line in batch.to_pylist():
                    # print(line.keys());exit(0)
                    # dict_keys([
                        # 'caption', 'url', 
                        # 'key', 
                        # 'status', 'error_message', 
                        # 'width', 'height', 'original_width', 'original_height', 
                        # 'exif', 'sha256', 
                        # 'jpg'])
                    # if line['error_message'] == 'failed to resize':
                    #     print(line['url']); exit(0)
                    # print(line.keys())
                    
                    video_bytes = line['content']
                    if video_bytes:
                        print(line['url']); exit(0)
                        # audio = AudioSegment.from_file(BytesIO(video_bytes), 'mp4')  # Automatically detect format
                        # buffer = BytesIO()
                        # audio.export(buffer, format="wav") # Export as WAV
                        # audio.export('test_audio', format="mp3") # Export as WAV
                        with open('test.mp4', 'wb') as file:
                            file.write(video_bytes)
                        exit(0)
                        # return buffer.read() # return actual bytes
                    
    
    # print(status) 
    # print(n)
    
    # print(f"width: {max(widths)} {min(widths)}")   
    # print(f"heights: {max(heights)} {min(heights)}")   
    # print(f"sizes: {max(sizes)} {min(sizes)}")   
                    # for k, v in line.items():
                    #     if k == 'jpg':
                    #         continue
                    #     print(f"{k}\t{v}")
                    # exit(0)
                    # document = self.get_document_from_dict(line, filepath, li)
                #     if not document:
                #         continue
                #     documents.append(document)
                #     li += 1
                # yield from documents

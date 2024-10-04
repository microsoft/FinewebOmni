# Copyright (c) Microsoft Corporation.
from datatrove.pipeline.extractors.base import BaseExtractor
from magic_html import GeneralExtractor

class MagicExtractor(BaseExtractor):

    name = "⛏ MagicExtractor"
    _requires_dependencies = ["trafilatura"]

    def __init__(
        self,
        # favour_precision: bool = True,
        # include_images: bool = False,
        # extractor = None,
        timeout: float = 0.1,
        # deduplicate: bool = True,
        **kwargs,
    ):
        super().__init__(timeout)
        self.kwargs = kwargs
        self.extractor = GeneralExtractor()

    def extract(self, text: str) -> str:
        """

        Args:
          text: str: html content

        Returns: plain text extracted text

        """
        
        return self.extractor.extract(text, base_url=None)['html']

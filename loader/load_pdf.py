# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo

from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfpage import PDFPage, PDFTextExtractionNotAllowed
from pdfminer.converter import PDFPageAggregator


def load(filepath, maxPages=0):
    with open(filepath, 'rb') as fin:
        parser = PDFParser(fin)
        document = PDFDocument(parser)

        if not document.is_extractable:
            raise PDFTextExtractionNotAllowed

        resmag = PDFResourceManager()
        laparams = LAParams()
        device = PDFPageAggregator(resmag, laparams=laparams)
        interpreter = PDFPageInterpreter(resmag, device)

        texts = []
        pageNum = 0
        for page in PDFPage.create_pages(document):
            pageNum += 1
            if maxPages > 0 and pageNum > maxPages:
                break
            interpreter.process_page(page)
            layout = device.get_result()
            for y in layout:
                if isinstance(y, LTTextBoxHorizontal):
                    texts.append(y.get_text())

        ret = {
            'content': ''.join(texts).replace('\n', '')
        }
        return ret

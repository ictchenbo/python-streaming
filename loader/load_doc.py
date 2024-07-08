# -*- coding: utf-8 -*-
# @Time    : 2021/8/8 15:28
# @Author  : chenbo


import os
from docx import Document


# 将doc文件转换为docx(适用于Linux环境)
def doc2docx_linux(docPath, outdir):
    os.system("libreoffice6.4 --headless --convert-to docx %s --outdir %s" % (docPath, outdir))


def load(filepath: str):
    if filepath.endswith(".doc"):
        out_path = os.path.dirname(filepath)
        doc2docx_linux(filepath, out_path)
        filepath = filepath + "x"

    with open(filepath, 'rb') as f:
        document = Document(f)
        # TODO meta
        props = document.core_properties

        texts = []
        for paragraph in document.paragraphs:
            texts.append(paragraph.text)

        return {
            "content": ''.join(texts).replace('\n', '')
        }

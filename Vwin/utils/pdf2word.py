#只是提取文本
import PyPDF2
from docx import Document
from pdf2docx import Converter


def pdf_to_docx(pdf_file, docx_file):
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    doc = Document()
    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        text = page.extract_text()
        doc.add_paragraph(text)
    doc.save(docx_file)


def pdf_to_docx(pdf_file, docx_file):
    cv = Converter(pdf_file)
    cv.convert(docx_file, start=0, end=None)
    cv.close()

if __name__ == "__main__":

# 使用示例

pdf_to_docx("input.pdf", "output.docx")

# # 使用示例
# with open('input.pdf', 'rb') as pdf_file:
#     pdf_to_docx(pdf_file, 'output.docx')
